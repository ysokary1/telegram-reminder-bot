import os
import json
import logging
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import Application, MessageHandler, MessageReactionHandler, filters, ContextTypes
from telegram.constants import ReactionEmoji
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import httpx

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class Database:
    """Simplified database for conversation-first bot"""
    
    def __init__(self):
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        self.init_db()
    
    def get_connection(self):
        return psycopg2.connect(self.database_url)
    
    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Simplified tasks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                due_date TIMESTAMP,
                priority TEXT DEFAULT 'medium',
                completed INTEGER DEFAULT 0,
                completed_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                commitment INTEGER DEFAULT 0,
                times_pushed INTEGER DEFAULT 0,
                job_id TEXT
            )
        ''')
        
        # Conversation history for context
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversation_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                role TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        ''')
        
        # User stats for pattern detection
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id BIGINT PRIMARY KEY,
                tasks_completed_today INTEGER DEFAULT 0,
                tasks_completed_week INTEGER DEFAULT 0,
                current_streak INTEGER DEFAULT 0,
                consecutive_misses INTEGER DEFAULT 0,
                last_completion TIMESTAMP,
                last_reset DATE
            )
        ''')
        
        # Active users table - CRITICAL FIX for reminders
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_users (
                user_id BIGINT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                last_interaction TIMESTAMP NOT NULL,
                first_seen TIMESTAMP NOT NULL DEFAULT NOW()
            )
        ''')
        
        # Message-to-task mapping for reaction-based completion
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS message_task_map (
                message_id BIGINT PRIMARY KEY,
                task_id INTEGER NOT NULL,
                chat_id BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def register_user(self, user_id: int, chat_id: int):
        """Register or update user activity - CRITICAL for reminders to work"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now(ZoneInfo('Europe/London'))
        
        cursor.execute('''
            INSERT INTO active_users (user_id, chat_id, last_interaction, first_seen)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                chat_id = EXCLUDED.chat_id, 
                last_interaction = EXCLUDED.last_interaction
        ''', (user_id, chat_id, now, now))
        
        conn.commit()
        conn.close()
    
    def get_active_users(self) -> List[Dict]:
        """Get all active users for scheduled check-ins"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get users who've interacted in the last 7 days
        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(days=7)
        cursor.execute('''
            SELECT user_id, chat_id, last_interaction 
            FROM active_users 
            WHERE last_interaction > %s
            ORDER BY last_interaction DESC
        ''', (cutoff,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def add_task(self, user_id: int, chat_id: int, title: str, 
                 due_date: str = None, priority: str = 'medium', 
                 commitment: bool = False, job_id: str = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO tasks (user_id, chat_id, title, due_date, priority, commitment, created_at, job_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (user_id, chat_id, title, due_date, priority, 1 if commitment else 0, 
              datetime.now(ZoneInfo('Europe/London')), job_id))
        
        task_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return task_id
    
    def get_tasks(self, user_id: int, completed: bool = False, 
                  due_today: bool = False, overdue: bool = False) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = 'SELECT * FROM tasks WHERE user_id = %s AND completed = %s'
        params = [user_id, 1 if completed else 0]
        
        if due_today:
            today = datetime.now(ZoneInfo('Europe/London')).date()
            query += ' AND DATE(due_date) = %s'
            params.append(today)
        
        if overdue:
            query += ' AND due_date < %s'
            params.append(datetime.now(ZoneInfo('Europe/London')))
        
        query += ' ORDER BY due_date ASC NULLS LAST, priority DESC, created_at DESC'
        
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_task(self, task_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM tasks WHERE id = %s', (task_id,))
        result = cursor.fetchone()
        conn.close()
        return dict(result) if result else None
    
    def complete_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE tasks SET completed = 1, completed_at = %s WHERE id = %s
        ''', (datetime.now(ZoneInfo('Europe/London')), task_id))
        conn.commit()
        conn.close()
    
    def update_task(self, task_id: int, **kwargs):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        updates = []
        values = []
        for key, value in kwargs.items():
            updates.append(f"{key} = %s")
            values.append(value)
        
        values.append(task_id)
        query = f"UPDATE tasks SET {', '.join(updates)} WHERE id = %s"
        cursor.execute(query, values)
        conn.commit()
        conn.close()
    
    def delete_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks WHERE id = %s', (task_id,))
        conn.commit()
        conn.close()
    
    def increment_push_count(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE tasks SET times_pushed = times_pushed + 1 WHERE id = %s', (task_id,))
        conn.commit()
        conn.close()
    
    def store_message_task_map(self, message_id: int, task_id: int, chat_id: int):
        """Store mapping between message and task for reaction-based completion"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO message_task_map (message_id, task_id, chat_id, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (message_id) DO UPDATE SET task_id = EXCLUDED.task_id
        ''', (message_id, task_id, chat_id, datetime.now(ZoneInfo('Europe/London'))))
        conn.commit()
        conn.close()
    
    def get_task_from_message(self, message_id: int) -> Optional[int]:
        """Get task_id from message_id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT task_id FROM message_task_map WHERE message_id = %s', (message_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    # Conversation history
    def add_message(self, user_id: int, role: str, message: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO conversation_history (user_id, role, message, timestamp)
            VALUES (%s, %s, %s, %s)
        ''', (user_id, role, message, datetime.now(ZoneInfo('Europe/London'))))
        conn.commit()
        
        # Keep only last 20 messages per user
        cursor.execute('''
            DELETE FROM conversation_history 
            WHERE id IN (
                SELECT id FROM conversation_history 
                WHERE user_id = %s 
                ORDER BY timestamp DESC 
                OFFSET 20
            )
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    def get_recent_messages(self, user_id: int, limit: int = 10) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT role, message FROM conversation_history 
            WHERE user_id = %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        ''', (user_id, limit))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return list(reversed(results))
    
    # Stats
    def get_stats(self, user_id: int) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Ensure user stats exist
        cursor.execute('SELECT * FROM user_stats WHERE user_id = %s', (user_id,))
        stats = cursor.fetchone()
        
        if not stats:
            cursor.execute('''
                INSERT INTO user_stats (user_id, last_reset) 
                VALUES (%s, %s) 
                RETURNING *
            ''', (user_id, datetime.now(ZoneInfo('Europe/London')).date()))
            stats = cursor.fetchone()
            conn.commit()
        
        # Check if we need to reset daily/weekly stats
        today = datetime.now(ZoneInfo('Europe/London')).date()
        last_reset = stats['last_reset']
        
        if last_reset != today:
            cursor.execute('''
                UPDATE user_stats 
                SET tasks_completed_today = 0, last_reset = %s 
                WHERE user_id = %s
            ''', (today, user_id))
            conn.commit()
            stats['tasks_completed_today'] = 0
        
        # Get active and overdue counts
        cursor.execute('SELECT COUNT(*) as count FROM tasks WHERE user_id = %s AND completed = 0', (user_id,))
        active_count = cursor.fetchone()['count']
        
        cursor.execute('''
            SELECT COUNT(*) as count FROM tasks 
            WHERE user_id = %s AND completed = 0 AND due_date < %s
        ''', (user_id, datetime.now(ZoneInfo('Europe/London'))))
        overdue_count = cursor.fetchone()['count']
        
        conn.close()
        
        return {
            'completed_today': stats['tasks_completed_today'],
            'completed_week': stats['tasks_completed_week'],
            'current_streak': stats['current_streak'],
            'consecutive_misses': stats['consecutive_misses'],
            'active_tasks': active_count,
            'overdue_tasks': overdue_count
        }
    
    def record_completion(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        now = datetime.now(ZoneInfo('Europe/London'))
        today = now.date()
        
        cursor.execute('SELECT * FROM user_stats WHERE user_id = %s', (user_id,))
        stats = cursor.fetchone()
        
        if not stats:
            cursor.execute('''
                INSERT INTO user_stats 
                (user_id, tasks_completed_today, tasks_completed_week, current_streak, consecutive_misses, last_completion, last_reset) 
                VALUES (%s, 1, 1, 1, 0, %s, %s)
            ''', (user_id, now, today))
        else:
            cursor.execute('''
                UPDATE user_stats 
                SET tasks_completed_today = tasks_completed_today + 1,
                    tasks_completed_week = tasks_completed_week + 1,
                    consecutive_misses = 0,
                    last_completion = %s
                WHERE user_id = %s
            ''', (now, user_id))
        
        conn.commit()
        conn.close()
    
    def record_miss(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE user_stats 
            SET consecutive_misses = consecutive_misses + 1 
            WHERE user_id = %s
        ''', (user_id,))
        conn.commit()
        conn.close()


class ConversationAI:
    """AI that handles all conversation and task management"""
    
    def __init__(self, api_key: str, db: Database):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
        self.db = db
    
    async def process_message(self, user_id: int, chat_id: int, message: str) -> Dict:
        """Process user message and return response + actions"""
        
        active_tasks = self.db.get_tasks(user_id, completed=False)
        uk_time = datetime.now(ZoneInfo('Europe/London'))
        
        system_prompt = f"""You are a personal assistant. Your tone is natural, supportive, and efficient. You sound like a real person, not a hyper-enthusiastic bot.

Current time: {uk_time.isoformat()}

Active tasks:
{self._format_tasks_for_ai(active_tasks)}

YOUR STYLE AND RULES:
- **Source of Truth:** The 'Active tasks' list above is the ONLY source of truth for what is pending. Base your replies ONLY on this list. Do NOT infer task status from conversation history. If the list is empty, there are no active tasks.
- **Natural & Supportive:** Be personable and direct. Your goal is to help, not to distract.
- **Task Confirmation:** For simple task actions, be brief. "Got it, added 'Call Steve tomorrow'." or "Done."
- **Handle Dates Correctly:** When a due date is relative (e.g., 'in 2 minutes', 'tomorrow at 3pm'), you MUST calculate the exact ISO 8601 timestamp based on the current time and include it in the 'due_date' field.
- **Don't Overdo It:** Avoid repeatedly mentioning the number of tasks completed. A simple 'Great job' or 'Marked as complete' is enough.
- **Follow The User's Lead:** Don't ask "What's next?" after every action. If a user completes a task, confirm it and wait for their next instruction. Only prompt if they ask for guidance.

Return ONLY JSON with "reply" and "actions" keys.
{{
  "reply": "Your natural, personable response goes here.",
  "actions": [
    {{"type": "create_task", "title": "...", "due_date": "YYYY-MM-DDTHH:MM:SS+01:00 or null"}},
    {{"type": "complete_task", "task_id": 123}},
    {{"type": "delete_task", "task_id": 123}},
    {{"type": "reschedule_task", "task_id": 123, "new_due_date": "YYYY-MM-DDTHH:MM:SS+01:00"}}
  ]
}}"""

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    self.base_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": message}
                        ],
                        "temperature": 0.4,
                        "max_tokens": 400,
                        "response_format": {"type": "json_object"}
                    }
                )
                
                if response.status_code != 200:
                    error_detail = response.text
                    logger.error(f"AI API error: {response.status_code} - {error_detail}")
                    return {"reply": "Sorry, I'm having trouble. Can you try again?", "actions": []}
                
                content = response.json()['choices'][0]['message']['content'].strip()
                json_start = content.find('{')
                json_end = content.rfind('}') + 1
                
                if json_start != -1 and json_end > json_start:
                    result = json.loads(content[json_start:json_end])
                else:
                    return {"reply": content if content else "Sorry, couldn't process that.", "actions": []}

                if 'actions' not in result:
                    result['actions'] = []
                
                return result
                
        except Exception as e:
            logger.error(f"AI processing error: {e}")
            return {"reply": "Hit a snag there. Try again?", "actions": []}
    
    def _format_tasks_for_ai(self, tasks: List[Dict]) -> str:
        if not tasks:
            return "No active tasks"
        
        formatted = [f"- ID {task['id']}: {task['title']}" for task in tasks[:10]]
        return "\n".join(formatted)
    
    async def generate_check_in(self, user_id: int, check_in_type: str) -> Dict:
        """Generate proactive check-in message"""
        tasks = self.db.get_tasks(user_id, completed=False)
        uk_time = datetime.now(ZoneInfo('Europe/London'))
        
        if check_in_type == "morning":
            context = f"Generate a brief, direct morning check-in. It's {uk_time.strftime('%A morning')}. Here are the user's tasks:\n{self._format_tasks_for_ai(tasks)}"
        else:  # evening
            completed_today = self.db.get_tasks(user_id, completed=True)
            completed_today = [t for t in completed_today if t['completed_at'] and 
                             datetime.fromisoformat(str(t['completed_at'])).date() == uk_time.date()]
            context = f"Generate a brief evening reflection. User completed {len(completed_today)} tasks. Pending tasks:\n{self._format_tasks_for_ai(tasks)}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    self.base_url, headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "system", "content": "You are a direct, efficient personal assistant. Brief and to the point."}, {"role": "user", "content": context}], "temperature": 0.6, "max_tokens": 150}
                )
                if response.status_code == 200:
                    return {"message": response.json()['choices'][0]['message']['content'].strip()}
        except Exception as e:
            logger.error(f"Check-in generation error: {e}")
        
        return {"message": "Just checking in."}


class MotivationEngine:
    """Handles AI-powered motivational messages"""
    USER_QUOTES = [
        "You are the most important project you will ever work on. So just go and do it.",
        "Your future family is depending on the man you are becoming today. Do it tired, sad, heartbroken, unmotivated, scared, lonely. Do it for them.",
        "I WIN I WIN THATS MY JOB THATS WHAT I DO.",
        "No matter what life throws at you, you are unstoppable. No matter how rough it gets, I will not quit. No matter how worn out I am, I will not stop. Give it your best shot, but I am unstoppable.",
        "I will sacrifice what others wont, and endure what others wont. There's a price. Relationships strain, people wont understand. I will miss things I wont get back, but its all worth it at the end."
    ]

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"

    def get_random_user_quote(self) -> str:
        return random.choice(self.USER_QUOTES)

    async def get_ai_generated_quote(self) -> str:
        """Generates a motivational quote using an AI, with a fallback."""
        try:
            prompt = f"Generate a short, powerful motivational quote. Be original. The theme should be similar to these examples:\n\n- {self.USER_QUOTES[0]}\n- {self.USER_QUOTES[1]}\n- {self.USER_QUOTES[3]}"
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self.base_url, headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "system", "content": "You are a motivational coach. Provide only the quote, no extra text."}, {"role": "user", "content": prompt}], "temperature": 0.8, "max_tokens": 60}
                )
                if response.status_code == 200:
                    return response.json()['choices'][0]['message']['content'].strip().strip('"')
        except Exception as e:
            logger.error(f"AI quote generation failed: {e}")
        
        return self.get_random_user_quote()


class PersonalAssistantBot:
    """Conversation-first Personal Assistant"""
    
    def __init__(self, telegram_token: str, groq_api_key: str):
        self.app = Application.builder().token(telegram_token).build()
        self.scheduler = AsyncIOScheduler(timezone='Europe/London')
        self.db = Database()
        self.ai = ConversationAI(groq_api_key, self.db)
        self.motivation = MotivationEngine(groq_api_key)
        self.user_timezone = ZoneInfo('Europe/London')
        
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.app.add_handler(MessageReactionHandler(self.handle_reaction))
        
        self.schedule_jobs()
    
    def schedule_jobs(self):
        """Schedules all recurring jobs for the bot."""
        self.scheduler.add_job(self.send_morning_check_ins, trigger=CronTrigger(hour=7, minute=30), id='morning_check')
        self.scheduler.add_job(self.send_evening_check_ins, trigger=CronTrigger(hour=20, minute=0), id='evening_check')
        self.scheduler.add_job(self.send_random_check_ins, trigger=CronTrigger(hour='9-18', minute=0, jitter=1800), id='random_check') # Every hour 9-6, with 30min jitter
        logger.info("Scheduled jobs: Morning, Evening, and Random hourly check-ins.")

    async def reload_pending_reminders(self):
        """Reload all pending task reminders on startup"""
        logger.info("Reloading pending reminders...")
        active_users = self.db.get_active_users()
        reloaded_count = 0
        for user in active_users:
            pending_tasks = self.db.get_tasks(user['user_id'], completed=False)
            for task in pending_tasks:
                if task['due_date']:
                    due_date = datetime.fromisoformat(str(task['due_date']))
                    if due_date > datetime.now(self.user_timezone):
                        await self.schedule_reminder(task['id'], due_date, task['title'], task['chat_id'])
                        reloaded_count += 1
        logger.info(f"Reloaded {reloaded_count} pending reminders")

    async def send_morning_check_ins(self):
        """Send morning check-in to all active users"""
        for user in self.db.get_active_users():
            try:
                result = await self.ai.generate_check_in(user['user_id'], "morning")
                quote = await self.motivation.get_ai_generated_quote()
                message = f"{result['message']}\n\n{quote}"
                await self.app.bot.send_message(chat_id=user['chat_id'], text=message)
            except Exception as e:
                logger.error(f"Failed to send morning check-in to user {user['user_id']}: {e}")

    async def send_evening_check_ins(self):
        """Send evening check-in to all active users"""
        for user in self.db.get_active_users():
            try:
                result = await self.ai.generate_check_in(user['user_id'], "evening")
                message = f"{result['message']}\n\nYou did good today."
                await self.app.bot.send_message(chat_id=user['chat_id'], text=message)
            except Exception as e:
                logger.error(f"Failed to send evening check-in to user {user['user_id']}: {e}")

    async def send_random_check_ins(self):
        """Periodically sends a random check-in to active users."""
        if random.random() > 0.6:  # 40% chance to send a check-in each hour
            logger.info("Executing random check-in...")
            for user in self.db.get_active_users():
                try:
                    tasks = self.db.get_tasks(user['user_id'], completed=False)
                    quote = await self.motivation.get_ai_generated_quote()
                    
                    if not tasks:
                        message = f"Just checking in. Your plate is clear. Keep that momentum.\n\n{quote}"
                    else:
                        task_list = self.ai._format_tasks_for_ai(tasks)
                        message = f"Quick check-in. Here's what's on your agenda:\n{task_list}\n\n{quote}"
                    
                    await self.app.bot.send_message(chat_id=user['chat_id'], text=message)
                except Exception as e:
                    logger.error(f"Failed to send random check-in to user {user['user_id']}: {e}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all text messages through AI"""
        user_id, chat_id, message = update.effective_user.id, update.effective_chat.id, update.message.text
        self.db.register_user(user_id, chat_id)
        self.db.add_message(user_id, "user", message)
        await update.message.chat.send_action("typing")
        
        result = await self.ai.process_message(user_id, chat_id, message)
        
        for action in result.get('actions', []):
            await self.execute_action(user_id, chat_id, action)
        
        reply = result.get('reply', 'Got it.')
        self.db.add_message(user_id, "assistant", reply)
        await update.message.reply_text(reply)

    async def execute_action(self, user_id: int, chat_id: int, action: Dict):
        """Execute action returned by AI"""
        action_type = action.get('type')
        try:
            if action_type == 'create_task':
                due_date = self.parse_due_date(action.get('due_date'))
                task_id = self.db.add_task(user_id=user_id, chat_id=chat_id, title=action['title'], due_date=due_date)
                if due_date:
                    await self.schedule_reminder(task_id, due_date, action['title'], chat_id)
            elif action_type == 'complete_task' and action.get('task_id'):
                self.db.complete_task(action['task_id'])
                self.db.record_completion(user_id)
            elif action_type == 'delete_task' and action.get('task_id'):
                self.db.delete_task(action['task_id'])
            elif action_type == 'reschedule_task' and action.get('task_id') and action.get('new_due_date'):
                new_due = self.parse_due_date(action.get('new_due_date'))
                if new_due:
                    task_id = action['task_id']
                    self.db.update_task(task_id, due_date=new_due)
                    self.db.increment_push_count(task_id)
                    task = self.db.get_task(task_id)
                    await self.schedule_reminder(task_id, new_due, task['title'], chat_id)
        except Exception as e:
            logger.error(f"Action execution error: {action_type} - {e}")

    def parse_due_date(self, due_str: Optional[str]) -> Optional[datetime]:
        """Parse due date string to datetime"""
        if not due_str:
            return None
        try:
            dt = datetime.fromisoformat(due_str.replace('Z', '+00:00'))
            return dt.astimezone(self.user_timezone) if dt.tzinfo else dt.replace(tzinfo=self.user_timezone)
        except (ValueError, TypeError):
            return None
    
    async def schedule_reminder(self, task_id: int, due_date: datetime, title: str, chat_id: int):
        """Schedule task reminder"""
        job_id = f"task_{task_id}"
        now = datetime.now(self.user_timezone)
        if due_date > now:
            self.scheduler.add_job(self.send_task_reminder, trigger=DateTrigger(run_date=due_date),
                                   args=[chat_id, title, task_id], id=job_id, replace_existing=True)
            self.db.update_task(task_id, job_id=job_id)
            logger.info(f"Scheduled reminder for task {task_id} at {due_date}")
    
    async def send_task_reminder(self, chat_id: int, title: str, task_id: int):
        """Send task reminder with a cleaner message."""
        try:
            message = f"⏰ Reminder: {title}"
            sent_message = await self.app.bot.send_message(chat_id=chat_id, text=message)
            self.db.store_message_task_map(sent_message.message_id, task_id, chat_id)
            logger.info(f"Sent reminder for task {task_id} to chat {chat_id}")
        except Exception as e:
            logger.error(f"Reminder error for task {task_id}: {e}")
    
    async def handle_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle message reactions for task completion"""
        if not update.message_reaction or not update.message_reaction.new_reaction:
            return
        
        user_id = update.effective_user.id
        message_id = update.message_reaction.message_id
        
        if any(r.emoji in ['👍', '✅', '✔️', '🔥'] for r in update.message_reaction.new_reaction):
            task_id = self.db.get_task_from_message(message_id)
            if task_id:
                self.db.complete_task(task_id)
                self.db.record_completion(user_id)
                await context.bot.send_message(chat_id=update.effective_chat.id, text="✅ Marked complete")
                logger.info(f"Task {task_id} completed via reaction by user {user_id}")
            else:
                logger.warning(f"No task found for reaction on message {message_id}")
    
    async def post_init(self, application):
        """Called after bot starts - reload pending reminders"""
        await self.reload_pending_reminders()
    
    def run(self):
        """Start the bot"""
        self.scheduler.start()
        logger.info("=" * 60)
        logger.info("Personal Assistant Bot Starting")
        logger.info("=" * 60)
        logger.info(f"Timezone: Europe/London | Current time: {datetime.now(self.user_timezone)}")
        logger.info("=" * 60)
        
        self.app.post_init = self.post_init
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if not all([TELEGRAM_TOKEN, GROQ_API_KEY, DATABASE_URL]):
        logger.error("Missing environment variables! Required: TELEGRAM_BOT_TOKEN, GROQ_API_KEY, DATABASE_URL")
        exit(1)
    
    bot = PersonalAssistantBot(TELEGRAM_TOKEN, GROQ_API_KEY)
    bot.run()


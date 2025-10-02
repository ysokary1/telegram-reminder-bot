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
from telegram.ext import Application, MessageHandler, filters, ContextTypes
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
        
        conn.commit()
        conn.close()
    
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
        
        # Get context
        recent_messages = self.db.get_recent_messages(user_id, limit=6)
        active_tasks = self.db.get_tasks(user_id, completed=False)
        stats = self.db.get_stats(user_id)
        
        # Build context for AI
        uk_time = datetime.now(ZoneInfo('Europe/London'))
        
        system_prompt = f"""You are a direct, high-performance personal assistant.

Current: {uk_time.strftime('%A, %B %d at %I:%M %p')}

User has:
- {stats['active_tasks']} active tasks ({stats['overdue_tasks']} overdue)
- Completed {stats['completed_today']} today

Active tasks:
{self._format_tasks_for_ai(active_tasks[:5])}

Recent chat:
{self._format_conversation(recent_messages)}

Understand what user wants and return JSON:
{{
  "reply": "brief natural response",
  "actions": [
    {{"type": "create_task", "title": "...", "due_date": "ISO or null", "priority": "high/medium/low", "commitment": true/false}},
    {{"type": "complete_task", "task_id": 123}},
    {{"type": "show_tasks", "filter": "today/overdue/all"}},
    {{"type": "delete_task", "task_id": 123}},
    {{"type": "reschedule_task", "task_id": 123, "new_due_date": "ISO"}}
  ]
}}

Be brief. Parse dates naturally. Default priority: medium. If they say "I'll do X today" set commitment: true.
Return ONLY JSON."""

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
                        "temperature": 0.3,
                        "max_tokens": 500
                    }
                )
                
                if response.status_code != 200:
                    error_detail = response.text
                    logger.error(f"AI API error: {response.status_code} - {error_detail}")
                    return {
                        "reply": "Sorry, I'm having trouble processing that. Can you try again?",
                        "actions": []
                    }
                
                content = response.json()['choices'][0]['message']['content'].strip()
                
                # Clean up markdown formatting if present
                if content.startswith('```'):
                    content = content.split('```')[1]
                    if content.startswith('json'):
                        content = content[4:]
                    content = content.strip()
                
                result = json.loads(content)
                
                # Ensure actions is a list
                if 'actions' not in result:
                    result['actions'] = []
                
                return result
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}, content: {content}")
            return {
                "reply": "I understood you, but had a technical issue. Try rephrasing?",
                "actions": []
            }
        except Exception as e:
            logger.error(f"AI error: {e}")
            return {
                "reply": "Had a glitch there. Try again?",
                "actions": []
            }
    
    def _format_tasks_for_ai(self, tasks: List[Dict]) -> str:
        if not tasks:
            return "No active tasks"
        
        formatted = []
        for task in tasks[:10]:
            due_str = ""
            if task['due_date']:
                due = datetime.fromisoformat(str(task['due_date']))
                due_str = f" (due {due.strftime('%b %d at %I:%M%p')})"
            
            pushed = f" [pushed {task['times_pushed']}x]" if task['times_pushed'] > 0 else ""
            formatted.append(f"- ID {task['id']}: {task['title']}{due_str}{pushed}")
        
        return "\n".join(formatted)
    
    def _format_conversation(self, messages: List[Dict]) -> str:
        if not messages:
            return "No recent conversation"
        
        formatted = []
        for msg in messages[-6:]:
            formatted.append(f"{msg['role']}: {msg['message']}")
        return "\n".join(formatted)
    
    async def generate_check_in(self, user_id: int, check_in_type: str) -> Dict:
        """Generate proactive check-in message"""
        
        tasks = self.db.get_tasks(user_id, completed=False)
        stats = self.db.get_stats(user_id)
        uk_time = datetime.now(ZoneInfo('Europe/London'))
        
        # Get tasks for today
        today_tasks = self.db.get_tasks(user_id, completed=False, due_today=True)
        overdue_tasks = self.db.get_tasks(user_id, completed=False, overdue=True)
        
        if check_in_type == "morning":
            context = f"""Generate a morning check-in message.

Current time: {uk_time.strftime('%A, %B %d at %I:%M %p')}

Stats:
- {len(tasks)} active tasks total
- {len(today_tasks)} due today
- {len(overdue_tasks)} overdue
- {stats['completed_week']} completed this week
- {stats['consecutive_misses']} consecutive misses

Tasks due today:
{self._format_tasks_for_ai(today_tasks)}

Overdue tasks:
{self._format_tasks_for_ai(overdue_tasks)}

Create a morning check-in. Be direct and focused. If there are overdue tasks that keep getting pushed, call it out. If they're crushing it, acknowledge it. Keep it brief."""

        else:  # evening
            completed_today = self.db.get_tasks(user_id, completed=True)
            completed_today = [t for t in completed_today if t['completed_at'] and 
                             datetime.fromisoformat(str(t['completed_at'])).date() == uk_time.date()]
            
            context = f"""Generate an evening check-in.

Today's completion: {len(completed_today)} tasks completed
Still pending: {len(today_tasks)} tasks due today not completed
Stats: {stats['completed_week']} completed this week

Pending tasks:
{self._format_tasks_for_ai(today_tasks)}

Create an evening reflection. Brief and honest. Celebrate wins. If commitments weren't met, ask what happened. Always end with: "You did good today." """

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    self.base_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": [
                            {"role": "system", "content": "You are a direct, performance-focused personal assistant. Brief and honest."},
                            {"role": "user", "content": context}
                        ],
                        "temperature": 0.7,
                        "max_tokens": 200
                    }
                )
                
                if response.status_code == 200:
                    message = response.json()['choices'][0]['message']['content'].strip()
                    return {"message": message}
                
        except Exception as e:
            logger.error(f"Check-in generation error: {e}")
        
        # Fallback
        if check_in_type == "morning":
            return {"message": f"Morning. You have {len(today_tasks)} tasks today. Which one are you starting with?"}
        else:
            return {"message": "End of day. You did good today."}


class MotivationEngine:
    """Handles motivational messages"""
    
    USER_QUOTES = [
        "I am the most important project I will ever work on. So just go and do it.",
        "Your future family is depending on the man you are becoming today. Do it tired, sad, heartbroken, unmotivated, scared, lonely. Do it for them.",
        "I WIN I WIN THATS MY JOB THATS WHAT I DO",
        "No matter what life throws at you. You are unstoppable. No matter how rough it gets I will not quit. No matter how worn out I am I will not stop. Give it your best shot but I am unstoppable.",
        "I will sacrifice what others wont, and endure what others wont. There's a price relationships strain people wont understand I will miss things I wont get back but its all worth it at the end."
    ]
    
    @staticmethod
    def get_random_quote() -> str:
        return random.choice(MotivationEngine.USER_QUOTES)
    
    @staticmethod
    def get_personalized_motivation(stats: Dict) -> str:
        """Generate motivation based on user's actual performance"""
        messages = []
        
        if stats['completed_today'] >= 5:
            messages.append(f"You've crushed {stats['completed_today']} tasks today. That's momentum.")
        
        if stats['completed_week'] >= 20:
            messages.append(f"{stats['completed_week']} tasks this week. You're executing.")
        
        if stats['current_streak'] >= 3:
            messages.append(f"{stats['current_streak']} day streak. Keep that energy.")
        
        if stats['overdue_tasks'] == 0 and stats['active_tasks'] > 0:
            messages.append("Nothing overdue. You're on top of it.")
        
        if messages:
            return random.choice(messages)
        
        return MotivationEngine.get_random_quote()


class PersonalAssistantBot:
    """Conversation-first Personal Assistant"""
    
    def __init__(self, telegram_token: str, groq_api_key: str):
        self.app = Application.builder().token(telegram_token).build()
        self.scheduler = AsyncIOScheduler(timezone='Europe/London')
        self.db = Database()
        self.ai = ConversationAI(groq_api_key, self.db)
        self.user_timezone = ZoneInfo('Europe/London')
        
        # Handlers - simplified, conversation-first
        self.app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            self.handle_message
        ))
        
        # Reaction handler - disabled for now, needs proper implementation
        # Will add back with MessageReactionHandler once core functionality is working
        
        # Schedule check-ins
        self.schedule_check_ins()
    
    def schedule_check_ins(self):
        """Schedule morning and evening check-ins"""
        # Morning at 7:30 AM
        self.scheduler.add_job(
            self.send_morning_check_ins,
            trigger=CronTrigger(hour=7, minute=30),
            id='morning_check'
        )
        
        # Evening at 8:00 PM
        self.scheduler.add_job(
            self.send_evening_check_ins,
            trigger=CronTrigger(hour=20, minute=0),
            id='evening_check'
        )
        
        # Midday motivation at 1:00 PM
        self.scheduler.add_job(
            self.send_midday_boost,
            trigger=CronTrigger(hour=13, minute=0),
            id='midday_boost'
        )
    
    async def send_morning_check_ins(self):
        """Send morning check-in to all users"""
        # In production, iterate through all users
        # For now, this will be triggered but needs user management
        pass
    
    async def send_morning_check_in(self, chat_id: int, user_id: int):
        """Send morning check-in to specific user"""
        try:
            result = await self.ai.generate_check_in(user_id, "morning")
            
            # Add motivational quote
            quote = MotivationEngine.get_random_quote()
            message = f"{result['message']}\n\n{quote}"
            
            await self.app.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            logger.error(f"Morning check-in error: {e}")
    
    async def send_evening_check_ins(self):
        """Send evening check-in to all users"""
        pass
    
    async def send_evening_check_in(self, chat_id: int, user_id: int):
        """Send evening check-in to specific user"""
        try:
            result = await self.ai.generate_check_in(user_id, "evening")
            await self.app.bot.send_message(chat_id=chat_id, text=result['message'])
        except Exception as e:
            logger.error(f"Evening check-in error: {e}")
    
    async def send_midday_boost(self):
        """Send midday motivation"""
        # Would iterate through users
        pass
    
    async def send_midday_motivation(self, chat_id: int, user_id: int):
        """Send midday motivation to specific user"""
        try:
            stats = self.db.get_stats(user_id)
            
            # Only send if user is active and could use a boost
            if stats['consecutive_misses'] >= 2 or stats['completed_today'] >= 3:
                message = MotivationEngine.get_personalized_motivation(stats)
                await self.app.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            logger.error(f"Midday boost error: {e}")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all text messages through AI"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        message = update.message.text
        
        # Store user message
        self.db.add_message(user_id, "user", message)
        
        # Show typing indicator
        await update.message.chat.send_action("typing")
        
        # Process through AI
        result = await self.ai.process_message(user_id, chat_id, message)
        
        # Execute actions
        action_results = []
        for action in result.get('actions', []):
            action_result = await self.execute_action(user_id, chat_id, action)
            if action_result:
                action_results.append(action_result)
        
        # Send reply
        reply = result.get('reply', 'Got it.')
        
        # Store bot message
        self.db.add_message(user_id, "assistant", reply)
        
        sent_message = await update.message.reply_text(reply)
        
        # Store message ID for reaction handling if it's about a specific task
        if action_results:
            context.bot_data[f'last_message_{user_id}'] = {
                'message_id': sent_message.message_id,
                'task_id': action_results[0].get('task_id') if action_results else None
            }
    
    async def execute_action(self, user_id: int, chat_id: int, action: Dict) -> Optional[Dict]:
        """Execute action returned by AI"""
        action_type = action.get('type')
        
        try:
            if action_type == 'create_task':
                # Parse due date
                due_date = None
                if action.get('due_date'):
                    due_date = self.parse_due_date(action['due_date'])
                
                task_id = self.db.add_task(
                    user_id=user_id,
                    chat_id=chat_id,
                    title=action['title'],
                    due_date=due_date,
                    priority=action.get('priority', 'medium'),
                    commitment=action.get('commitment', False)
                )
                
                # Schedule reminder if due date exists
                if due_date:
                    await self.schedule_reminder(task_id, due_date, action['title'], chat_id)
                
                return {'task_id': task_id}
            
            elif action_type == 'complete_task':
                task_id = action.get('task_id')
                if task_id:
                    self.db.complete_task(task_id)
                    self.db.record_completion(user_id)
                    return {'completed': task_id}
            
            elif action_type == 'delete_task':
                task_id = action.get('task_id')
                if task_id:
                    self.db.delete_task(task_id)
                    return {'deleted': task_id}
            
            elif action_type == 'reschedule_task':
                task_id = action.get('task_id')
                new_due = self.parse_due_date(action.get('new_due_date'))
                if task_id and new_due:
                    self.db.update_task(task_id, due_date=new_due)
                    self.db.increment_push_count(task_id)
                    
                    # Reschedule reminder
                    task = self.db.get_task(task_id)
                    await self.schedule_reminder(task_id, new_due, task['title'], chat_id)
                    return {'rescheduled': task_id}
            
        except Exception as e:
            logger.error(f"Action execution error: {e}")
        
        return None
    
    def parse_due_date(self, due_str: str) -> Optional[datetime]:
        """Parse due date string to datetime"""
        try:
            # If it's already ISO format
            if 'T' in due_str or '-' in due_str:
                dt = datetime.fromisoformat(due_str.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=self.user_timezone)
                return dt
        except:
            pass
        
        return None
    
    async def schedule_reminder(self, task_id: int, due_date: datetime, title: str, chat_id: int):
        """Schedule task reminder"""
        job_id = f"task_{task_id}"
        
        # Ensure due_date is timezone-aware
        if due_date.tzinfo is None:
            due_date = due_date.replace(tzinfo=self.user_timezone)
        
        # Convert to UK timezone
        due_date = due_date.astimezone(self.user_timezone)
        
        # Only schedule if in future
        now = datetime.now(self.user_timezone)
        if due_date > now:
            self.scheduler.add_job(
                self.send_task_reminder,
                trigger=DateTrigger(run_date=due_date),
                args=[chat_id, title, task_id],
                id=job_id,
                replace_existing=True
            )
            
            self.db.update_task(task_id, job_id=job_id)
    
    async def send_task_reminder(self, chat_id: int, title: str, task_id: int):
        """Send task reminder"""
        try:
            message = f"⏰ {title}\n\nReact with 👍 to mark done, or tell me when to remind you again."
            
            sent_message = await self.app.bot.send_message(chat_id=chat_id, text=message)
            
            # Store for reaction handling
            # Note: This is simplified - in production you'd need persistent storage
            
        except Exception as e:
            logger.error(f"Reminder error: {e}")
    
    async def handle_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle message reactions for task completion"""
        if not update.message_reaction:
            return
        
        user_id = update.effective_user.id
        reaction = update.message_reaction
        
        # Check if it's a completion reaction
        completion_emojis = ['👍', '✅', '✔️', '🔥']
        
        if reaction.new_reaction:
            for emoji_reaction in reaction.new_reaction:
                emoji = emoji_reaction.emoji if hasattr(emoji_reaction, 'emoji') else str(emoji_reaction)
                
                if emoji in completion_emojis:
                    # Try to find the task associated with this message
                    # This is simplified - in production you'd store message_id -> task_id mapping
                    recent_tasks = self.db.get_tasks(user_id, completed=False)
                    
                    if recent_tasks:
                        # Complete the most recent task
                        task = recent_tasks[0]
                        self.db.complete_task(task['id'])
                        self.db.record_completion(user_id)
                        
                        # Send confirmation
                        confirm_msg = "✅ Marked complete"
                        if emoji == '🔥':
                            confirm_msg = "🔥 Crushed it"
                        
                        await self.app.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=confirm_msg
                        )
    
    def run(self):
        """Start the bot"""
        self.scheduler.start()
        logger.info("Conversation-first PA Bot starting...")
        logger.info(f"Timezone: Europe/London")
        logger.info(f"Current time: {datetime.now(self.user_timezone)}")
        
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    
    if not TELEGRAM_TOKEN or not GROQ_API_KEY:
        logger.error("Missing environment variables!")
        exit(1)
    
    bot = PersonalAssistantBot(TELEGRAM_TOKEN, GROQ_API_KEY)
    bot.run()

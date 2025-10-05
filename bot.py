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
    """Handles all database operations for the assistant."""
    
    def __init__(self):
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        self.init_db()
    
    def get_connection(self):
        return psycopg2.connect(self.database_url)
    
    def init_db(self):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
                        title TEXT NOT NULL, due_date TIMESTAMP WITH TIME ZONE, priority TEXT DEFAULT 'medium',
                        completed INTEGER DEFAULT 0, completed_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL, times_pushed INTEGER DEFAULT 0, job_id TEXT
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS conversation_history (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, role TEXT NOT NULL,
                        message TEXT NOT NULL, timestamp TIMESTAMP WITH TIME ZONE NOT NULL
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS active_users (
                        user_id BIGINT PRIMARY KEY, chat_id BIGINT NOT NULL,
                        last_interaction TIMESTAMP WITH TIME ZONE NOT NULL
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS message_task_map (
                        message_id BIGINT PRIMARY KEY, task_id INTEGER NOT NULL,
                        chat_id BIGINT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL
                    )
                ''')

    def register_user(self, user_id: int, chat_id: int):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now(ZoneInfo('Europe/London'))
                cursor.execute('''
                    INSERT INTO active_users (user_id, chat_id, last_interaction) VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET chat_id = EXCLUDED.chat_id, last_interaction = EXCLUDED.last_interaction
                ''', (user_id, chat_id, now))

    def get_tasks(self, user_id: int, completed: bool = False, recent_hours: int = 0) -> List[Dict]:
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = 'SELECT * FROM tasks WHERE user_id = %s AND completed = %s'
                params = [user_id, 1 if completed else 0]
                if completed and recent_hours > 0:
                    cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(hours=recent_hours)
                    query += ' AND completed_at > %s'
                    params.append(cutoff)
                query += ' ORDER BY due_date ASC NULLS LAST, created_at DESC'
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def add_task(self, user_id: int, chat_id: int, title: str, due_date: Optional[datetime] = None) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now(ZoneInfo('Europe/London'))
                cursor.execute(
                    'INSERT INTO tasks (user_id, chat_id, title, due_date, created_at) VALUES (%s, %s, %s, %s, %s) RETURNING id',
                    (user_id, chat_id, title, due_date, now)
                )
                return cursor.fetchone()[0]

    def update_task(self, task_id: int, **kwargs):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                updates = ', '.join([f"{key} = %s" for key in kwargs])
                values = list(kwargs.values()) + [task_id]
                cursor.execute(f"UPDATE tasks SET {updates} WHERE id = %s", values)

    def complete_task(self, task_id: int):
        self.update_task(task_id, completed=1, completed_at=datetime.now(ZoneInfo('Europe/London')))

    def delete_task(self, task_id: int):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM tasks WHERE id = %s', (task_id,))

    def add_message(self, user_id: int, role: str, message: str):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now(ZoneInfo('Europe/London'))
                cursor.execute(
                    'INSERT INTO conversation_history (user_id, role, message, timestamp) VALUES (%s, %s, %s, %s)',
                    (user_id, role, message, now)
                )
                cursor.execute('''
                    DELETE FROM conversation_history WHERE id IN (
                        SELECT id FROM conversation_history WHERE user_id = %s ORDER BY timestamp DESC OFFSET 20
                    )
                ''', (user_id,))
    
    def get_recent_messages(self, user_id: int, limit: int = 12) -> List[Dict]:
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    'SELECT role, message FROM conversation_history WHERE user_id = %s ORDER BY timestamp DESC LIMIT %s',
                    (user_id, limit)
                )
                return list(reversed([dict(row) for row in cursor.fetchall()]))

    def store_message_task_map(self, message_id: int, task_id: int, chat_id: int):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now(ZoneInfo('Europe/London'))
                cursor.execute(
                    'INSERT INTO message_task_map (message_id, task_id, chat_id, created_at) VALUES (%s, %s, %s, %s) ON CONFLICT (message_id) DO UPDATE SET task_id = EXCLUDED.task_id',
                    (message_id, task_id, chat_id, now)
                )

    def get_task_from_message(self, message_id: int) -> Optional[int]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT task_id FROM message_task_map WHERE message_id = %s', (message_id,))
                result = cursor.fetchone()
                return result[0] if result else None


class ConversationAI:
    def __init__(self, api_key: str, db: Database):
        self.api_key = api_key
        self.db = db
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={self.api_key}"

    def _format_tasks_for_ai(self, tasks: List[Dict], title: str) -> str:
        if not tasks:
            return f"{title}:\n- None"
        formatted = [f"- ID {t['id']}: {t['title']} (Due: {t['due_date'].strftime('%A, %b %d at %I:%M %p') if t['due_date'] else 'N/A'})" for t in tasks]
        return f"{title}:\n" + "\n".join(formatted)

    async def process_message(self, user_id: int, message: str) -> Dict:
        active_tasks = self.db.get_tasks(user_id, completed=False)
        completed_tasks = self.db.get_tasks(user_id, completed=True, recent_hours=6)
        conversation_history = self.db.get_recent_messages(user_id)
        
        system_prompt = f"""You are a world-class personal assistant. Your primary function is to reduce the user's mental load by being proactive, intelligent, and anticipating their needs. You are an active partner in their productivity.

Current time: {datetime.now(ZoneInfo('Europe/London')).isoformat()}

**CONTEXTUAL BRIEFING:**
{self._format_tasks_for_ai(active_tasks, "ACTIVE TASKS (Source of Truth)")}
{self._format_tasks_for_ai(completed_tasks, "RECENTLY COMPLETED TASKS")}

**CORE DIRECTIVES:**
1.  **Be Proactive, Not Passive:** Your most important job is to turn vague requests into fully actionable tasks. If a request is missing details (like a specific time or date), you MUST ask for clarification.
2.  **Use Full Context:** Analyze the conversation history, active tasks, and completed tasks to understand multi-step requests and provide informed responses.
3.  **Accuracy is Paramount:** The 'ACTIVE TASKS' list is your absolute source of truth for pending items. Carefully calculate all dates and times.
4.  **JSON Output Only:** Your entire response must be a single, valid JSON object.

**RESPONSE FORMAT (JSON ONLY):**
{{
  "reply": "Your natural, proactive, and intelligent response.",
  "actions": [
    {{"type": "create_task", "title": "...", "due_date": "YYYY-MM-DDTHH:MM:SS+01:00"}},
    {{"type": "complete_task", "task_id": 123}},
    {{"type": "delete_task", "task_id": 123}}
  ]
}}"""

        gemini_conversation = [{"role": "model" if msg['role'] == 'assistant' else 'user', "parts": [{"text": msg['message']}]} for msg in conversation_history]
        gemini_conversation.append({"role": "user", "parts": [{"text": message}]})

        payload = {
            "contents": gemini_conversation,
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
        }
        
        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                content_text = result['candidates'][0]['content']['parts'][0]['text']
                json_result = json.loads(content_text)
                json_result.setdefault('actions', [])
                return json_result
        except (httpx.HTTPStatusError, json.JSONDecodeError, IndexError, KeyError) as e:
            logger.error(f"AI response processing error: {e} - Response: {response.text if 'response' in locals() else 'N/A'}")
            return {"reply": "I had a technical hiccup. Could you rephrase that?", "actions": []}


class MotivationEngine:
    USER_QUOTES = ["Your future family is depending on the man you are becoming today. Do it for them.", "You are the most important project you will ever work on. So just go and do it."]

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={self.api_key}"

    async def get_ai_generated_quote(self) -> str:
        try:
            prompt = f"Generate a short, powerful motivational quote about discipline and long-term vision. Be original. Inspire action. Similar to: '{random.choice(self.USER_QUOTES)}'"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "systemInstruction": {"parts": [{"text": "You are a motivational coach. Provide only the quote, no extra text, no quotation marks."}]},
                "generationConfig": {"temperature": 0.9, "max_output_tokens": 80}
            }
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                return result['candidates'][0]['content']['parts'][0]['text'].strip()
        except Exception as e:
            logger.error(f"AI quote generation failed: {e}")
            return random.choice(self.USER_QUOTES)


class PersonalAssistantBot:
    def __init__(self, telegram_token: str, gemini_api_key: str):
        self.app = Application.builder().token(telegram_token).build()
        self.scheduler = AsyncIOScheduler(timezone='Europe/London')
        self.db = Database()
        self.ai = ConversationAI(gemini_api_key, self.db)
        self.motivation = MotivationEngine(gemini_api_key)
        self.user_timezone = ZoneInfo('Europe/London')
        
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.app.add_handler(MessageReactionHandler(self.handle_reaction))
        
        self.schedule_jobs()
    
    def schedule_jobs(self):
        self.scheduler.add_job(self.send_check_in, CronTrigger(hour=7, minute=30), id='morning_check', args=["morning"])
        self.scheduler.add_job(self.send_check_in, CronTrigger(hour=21, minute=0), id='evening_check', args=["evening"])
        self.scheduler.add_job(self.send_check_in, CronTrigger(hour='9-18', minute=0, jitter=1800), id='random_check', args=["random"])

    async def reload_pending_reminders(self):
        # Implementation to reschedule reminders from DB on startup
        pass

    async def send_check_in(self, check_in_type: str):
        active_users = self.db.get_active_users()
        for user in active_users:
            try:
                tasks = self.db.get_tasks(user['user_id'])
                task_list_str = self.ai._format_tasks_for_ai(tasks, "Pending Tasks")
                
                if check_in_type == "evening":
                    message = "Wrapping up the day.\n\nYou did good today."
                else: # Morning and Random
                    quote = await self.motivation.get_ai_generated_quote()
                    prompt = f"You are a world-class personal assistant. Your user has the following tasks: {task_list_str}. Craft a brief, natural, and conversational check-in message. Be proactive and encouraging."
                    # Simplified call for check-ins for now
                    message = f"Morning, here's your agenda:\n{task_list_str}\n\n{quote}"

                await self.app.bot.send_message(chat_id=user['chat_id'], text=message)
            except Exception as e:
                logger.error(f"Failed to send {check_in_type} check-in to user {user['user_id']}: {e}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id, chat_id, message = update.effective_user.id, update.effective_chat.id, update.message.text
        self.db.register_user(user_id, chat_id)
        self.db.add_message(user_id, "user", message)
        
        await context.bot.send_chat_action(chat_id=chat_id, action='typing')
        
        result = await self.ai.process_message(user_id, message)
        reply = result.get('reply', 'Got it.')
        
        self.db.add_message(user_id, "assistant", reply)
        
        sent_message = await update.message.reply_text(reply)
        
        for action in result.get('actions', []):
            task_id = await self.execute_action(user_id, chat_id, action)
            if action['type'] == 'create_task' and task_id:
                self.db.store_message_task_map(sent_message.message_id, task_id, chat_id)

    async def execute_action(self, user_id: int, chat_id: int, action: Dict) -> Optional[int]:
        action_type = action.get('type')
        try:
            if action_type == 'create_task':
                due_date = self.parse_due_date(action.get('due_date'))
                task_id = self.db.add_task(user_id=user_id, chat_id=chat_id, title=action['title'], due_date=due_date)
                if due_date:
                    await self.schedule_reminder(task_id, due_date, action['title'], chat_id)
                return task_id
            elif action_type == 'complete_task' and action.get('task_id'):
                self.db.complete_task(action['task_id'])
            elif action_type == 'delete_task' and action.get('task_id'):
                self.db.delete_task(action['task_id'])
        except Exception as e:
            logger.error(f"Action execution error: {action_type} - {e}")
        return None

    def parse_due_date(self, due_str: Optional[str]) -> Optional[datetime]:
        if not due_str: return None
        try:
            return datetime.fromisoformat(due_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return None
    
    async def schedule_reminder(self, task_id: int, due_date: datetime, title: str, chat_id: int):
        now = datetime.now(self.user_timezone)
        aware_due_date = due_date.astimezone(self.user_timezone) if due_date.tzinfo else self.user_timezone.localize(due_date)
        if aware_due_date > now:
            self.scheduler.add_job(self.send_task_reminder, DateTrigger(run_date=aware_due_date),
                                   args=[chat_id, title, task_id], id=f"task_{task_id}", replace_existing=True)

    async def send_task_reminder(self, chat_id: int, title: str, task_id: int):
        message = f"⏰ Reminder: {title}"
        sent_message = await self.app.bot.send_message(chat_id=chat_id, text=message)
        self.db.store_message_task_map(sent_message.message_id, task_id, chat_id)

    async def handle_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message_reaction or not update.message_reaction.new_reaction: return
        user_id = update.effective_user.id
        message_id = update.message_reaction.message_id
        if any(r.emoji in ['👍', '✅'] for r in update.message_reaction.new_reaction):
            task_id = self.db.get_task_from_message(message_id)
            if task_id:
                self.db.complete_task(task_id)
                await context.bot.send_message(chat_id=update.effective_chat.id, text="✅ Done.")

    def run(self):
        self.scheduler.start()
        logger.info("=" * 60)
        logger.info("Personal Assistant Bot (Total Context Edition) Starting")
        logger.info(f"Timezone: Europe/London | Current time: {datetime.now(self.user_timezone)}")
        logger.info("=" * 60)
        self.app.run_polling()


if __name__ == "__main__":
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") 
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if not all([TELEGRAM_TOKEN, GEMINI_API_KEY, DATABASE_URL]):
        logger.error("Missing required environment variables!")
        exit(1)
    
    bot = PersonalAssistantBot(TELEGRAM_TOKEN, GEMINI_API_KEY)
    bot.run()


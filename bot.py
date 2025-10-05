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
        if not self.database_url: raise ValueError("DATABASE_URL not set")
        self.init_db()

    def get_connection(self):
        return psycopg2.connect(self.database_url)

    def init_db(self):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
                        title TEXT NOT NULL, due_date TIMESTAMP WITH TIME ZONE,
                        completed INTEGER DEFAULT 0, completed_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL, job_id TEXT
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
                        message_id BIGINT PRIMARY KEY, task_id INTEGER NOT NULL
                    )
                ''')

    def _execute_query(self, query, params=None, fetch=None):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor if fetch else None) as cursor:
                cursor.execute(query, params)
                if fetch == 'one': return cursor.fetchone()
                if fetch == 'all': return [dict(row) for row in cursor.fetchall()]

    def register_user(self, user_id: int, chat_id: int):
        now = datetime.now(ZoneInfo('Europe/London'))
        self._execute_query(
            'INSERT INTO active_users (user_id, chat_id, last_interaction) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET chat_id = EXCLUDED.chat_id, last_interaction = EXCLUDED.last_interaction',
            (user_id, chat_id, now)
        )

    def get_tasks(self, user_id: int, completed: bool = False, limit: Optional[int] = None) -> List[Dict]:
        order_by = 'completed_at DESC' if completed else 'due_date ASC NULLS LAST, created_at DESC'
        query = f'SELECT * FROM tasks WHERE user_id = %s AND completed = %s ORDER BY {order_by}'
        params = [user_id, 1 if completed else 0]
        if limit:
            query += ' LIMIT %s'
            params.append(limit)
        return self._execute_query(query, params, fetch='all')

    def add_task(self, user_id: int, chat_id: int, title: str, due_date: Optional[datetime] = None) -> int:
        now = datetime.now(ZoneInfo('Europe/London'))
        result = self._execute_query(
            'INSERT INTO tasks (user_id, chat_id, title, due_date, created_at) VALUES (%s, %s, %s, %s, %s) RETURNING id',
            (user_id, chat_id, title, due_date, now), fetch='one'
        )
        return result[0]

    def update_task(self, task_id: int, **kwargs):
        updates = ', '.join([f"{key} = %s" for key in kwargs])
        self._execute_query(f"UPDATE tasks SET {updates} WHERE id = %s", list(kwargs.values()) + [task_id])

    def complete_task(self, task_id: int):
        self.update_task(task_id, completed=1, completed_at=datetime.now(ZoneInfo('Europe/London')))

    def delete_task(self, task_id: int):
        self._execute_query('DELETE FROM tasks WHERE id = %s', (task_id,))

    def add_message(self, user_id: int, role: str, message: str):
        now = datetime.now(ZoneInfo('Europe/London'))
        self._execute_query(
            'INSERT INTO conversation_history (user_id, role, message, timestamp) VALUES (%s, %s, %s, %s)',
            (user_id, role, message, now)
        )
        self._execute_query('DELETE FROM conversation_history WHERE id IN (SELECT id FROM conversation_history WHERE user_id = %s ORDER BY timestamp DESC OFFSET 20)', (user_id,))
    
    def get_recent_messages(self, user_id: int, limit: int = 12) -> List[Dict]:
        results = self._execute_query('SELECT role, message FROM conversation_history WHERE user_id = %s ORDER BY timestamp DESC LIMIT %s', (user_id, limit), fetch='all')
        return list(reversed(results))


    def store_message_task_map(self, message_id: int, task_id: int):
        self._execute_query('INSERT INTO message_task_map (message_id, task_id) VALUES (%s, %s) ON CONFLICT (message_id) DO UPDATE SET task_id = EXCLUDED.task_id', (message_id, task_id))

    def get_task_from_message(self, message_id: int) -> Optional[int]:
        result = self._execute_query('SELECT task_id FROM message_task_map WHERE message_id = %s', (message_id,), fetch='one')
        return result[0] if result else None
    
    def get_active_users(self) -> List[Dict]:
        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(days=7)
        return self._execute_query('SELECT user_id, chat_id FROM active_users WHERE last_interaction > %s', (cutoff,), fetch='all')


class ConversationAI:
    def __init__(self, api_key: str, db: Database, timezone: ZoneInfo):
        self.api_key = api_key
        self.db = db
        self.timezone = timezone
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={self.api_key}"

    def _format_tasks(self, tasks: List[Dict], title: str) -> str:
        if not tasks: return f"{title}:\n- None"
        
        formatted_tasks = []
        for t in tasks:
            due_str = 'Not scheduled'
            if t['due_date']:
                # Convert UTC time from DB to local timezone for display
                local_due = t['due_date'].astimezone(self.timezone)
                due_str = local_due.strftime('%A at %I:%M %p')
            formatted_tasks.append(f"- ID {t['id']}: {t['title']} (Due: {due_str})")
            
        return f"{title}:\n" + "\n".join(formatted_tasks)

    async def process_message(self, user_id: int, message: str) -> Dict:
        active_tasks = self.db.get_tasks(user_id)
        last_completed = self.db.get_tasks(user_id, completed=True, limit=1)
        history = self.db.get_recent_messages(user_id)
        
        system_prompt = f"""You are a hyper-intelligent, proactive personal assistant. Your primary function is to manage tasks and conversations with state-aware logic, reducing the user's mental load. You are concise and sound like a natural human.

Current time: {datetime.now(self.timezone).isoformat()}

**CONTEXTUAL BRIEFING (Source of Truth):**
{self._format_tasks(active_tasks, "ACTIVE TASKS")}
{self._format_tasks(last_completed, "LAST COMPLETED TASK")}

**CORE DIRECTIVES (NON-NEGOTIABLE):**
1.  **State Management is Key:** If a user's request modifies an existing task (e.g., "change that to 12pm"), you MUST use the `update_task` action. DO NOT create a duplicate task.
2.  **Be Proactive & Infer:** If a task is created without a specific time (e.g., "on Monday"), you MUST ask for clarification. If a time is given without a date ("at 10"), assume it's for today or the soonest logical future time. Ask simple, direct questions (e.g., "10 AM or PM?").
3.  **Use Full Context:** Your response MUST be informed by the conversation history AND the task lists.
4.  **Prioritize Recent Actions:** When asked "what did I just finish?", your answer MUST be based ONLY on the 'LAST COMPLETED TASK'.
5.  **JSON Output Only:** Your entire response must be a single, valid JSON object.

**RESPONSE FORMAT (JSON ONLY):**
{{
  "reply": "Your concise, intelligent, and natural response.",
  "actions": [
    {{"type": "create_task", "title": "...", "due_date": "YYYY-MM-DDTHH:MM:SS+01:00"}},
    {{"type": "update_task", "task_id": 123, "title": "(optional)", "due_date": "(optional)"}},
    {{"type": "delete_task", "task_id": 123}},
    {{"type": "complete_task", "task_id": 123}}
  ]
}}"""

        contents = [{"role": "model" if msg['role'] == 'assistant' else 'user', "parts": [{"text": msg['message']}]} for msg in history]
        contents.append({"role": "user", "parts": [{"text": message}]})

        payload = {
            "contents": contents,
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                content = json.loads(result['candidates'][0]['content']['parts'][0]['text'])
                content.setdefault('actions', [])
                return content
        except Exception as e:
            logger.error(f"AI processing error: {e}")
            return {"reply": "I'm having a bit of trouble right now. Please try again.", "actions": []}


class PersonalAssistantBot:
    def __init__(self, telegram_token: str, gemini_api_key: str):
        self.app = Application.builder().token(telegram_token).build()
        self.user_timezone = ZoneInfo('Europe/London')
        self.scheduler = AsyncIOScheduler(timezone=self.user_timezone)
        self.db = Database()
        self.ai = ConversationAI(gemini_api_key, self.db, self.user_timezone)
        
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.app.add_handler(MessageReactionHandler(self.handle_reaction))
        
        self.scheduler.start()
        logger.info(f"Personal Assistant Bot (Definitive Edition) started at {datetime.now(self.user_timezone)}")
    
    async def post_init(self, application: Application):
        """Reloads pending reminders after the application has been initialized."""
        await self.reload_pending_reminders()

    async def reload_pending_reminders(self):
        logger.info("Reloading pending reminders...")
        reloaded_count = 0
        users = self.db.get_active_users()
        for user in users:
            pending_tasks = self.db.get_tasks(user['user_id'], completed=False)
            for task in pending_tasks:
                if task.get('due_date'):
                    await self.schedule_reminder(task['id'], task['due_date'], task['title'], task['chat_id'])
                    reloaded_count += 1
        logger.info(f"Reloaded {reloaded_count} pending reminders.")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user, chat, message = update.effective_user, update.effective_chat, update.message.text
        self.db.register_user(user.id, chat.id)
        self.db.add_message(user.id, "user", message)
        
        await context.bot.send_chat_action(chat_id=chat.id, action='typing')
        
        result = await self.ai.process_message(user.id, message)
        reply = result.get('reply', 'Understood.')
        
        self.db.add_message(user.id, "assistant", reply)
        sent_message = await update.message.reply_text(reply)
        
        for action in result.get('actions', []):
            await self.execute_action(user.id, chat.id, action)

    async def execute_action(self, user_id: int, chat_id: int, action: Dict):
        action_type = action.get('type')
        task_id = action.get('task_id')
        try:
            if action_type == 'create_task':
                due_date = self.parse_due_date(action.get('due_date'))
                new_task_id = self.db.add_task(user_id, chat_id, action['title'], due_date)
                if due_date: await self.schedule_reminder(new_task_id, due_date, action['title'], chat_id)
            elif action_type == 'update_task' and task_id:
                update_data = {k: v for k, v in action.items() if k not in ['type', 'task_id']}
                if 'due_date' in update_data:
                    update_data['due_date'] = self.parse_due_date(update_data['due_date'])
                self.db.update_task(task_id, **update_data)
                
                if update_data.get('due_date'):
                    task = next((t for t in self.db.get_tasks(user_id) if t['id'] == task_id), None)
                    if task: self.schedule_reminder(task_id, update_data['due_date'], task['title'], chat_id)
            elif action_type == 'complete_task' and task_id:
                self.db.complete_task(task_id)
            elif action_type == 'delete_task' and task_id:
                self.db.delete_task(task_id)
        except Exception as e:
            logger.error(f"Action execution error ({action_type}): {e}")

    def parse_due_date(self, due_str: Optional[str]) -> Optional[datetime]:
        if not due_str: return None
        try: return datetime.fromisoformat(due_str.replace('Z', '+00:00'))
        except: return None
    
    def schedule_reminder(self, task_id: int, due_date: datetime, title: str, chat_id: int):
        # FIX: Use .replace(tzinfo=...) for zoneinfo, not .localize()
        aware_due = due_date if due_date.tzinfo else due_date.replace(tzinfo=self.user_timezone)
        if aware_due > datetime.now(self.user_timezone):
            self.scheduler.add_job(self.send_task_reminder, DateTrigger(run_date=aware_due),
                                   args=[chat_id, title, task_id], id=f"task_{task_id}", replace_existing=True)

    async def send_task_reminder(self, chat_id: int, title: str, task_id: int):
        message = f"⏰ Reminder: {title}"
        sent_message = await self.app.bot.send_message(chat_id=chat_id, text=message)
        self.db.store_message_task_map(sent_message.message_id, task_id)

    async def handle_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message_reaction or not update.message_reaction.new_reaction: return
        task_id = self.db.get_task_from_message(update.message_reaction.message_id)
        if task_id and any(r.emoji in ['👍', '✅'] for r in update.message_reaction.new_reaction):
            self.db.complete_task(task_id)
            await context.bot.send_message(chat_id=update.effective_chat.id, text="✅ Done.")

    def run(self):
        self.app.post_init = self.post_init
        self.app.run_polling()

if __name__ == "__main__":
    secrets = {key: os.environ.get(key) for key in ["TELEGRAM_BOT_TOKEN", "GEMINI_API_KEY", "DATABASE_URL"]}
    if not all(secrets.values()):
        logger.error(f"Missing environment variables! Required: {', '.join(secrets.keys())}")
        exit(1)
    
    bot = PersonalAssistantBot(secrets["TELEGRAM_BOT_TOKEN"], secrets["GEMINI_API_KEY"])
    bot.run()


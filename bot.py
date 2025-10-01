import os
import json
import logging
from datetime import datetime, timedelta, time
from typing import Optional, List, Dict
import re
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import httpx

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
EDIT_FIELD, SNOOZE_TIME = range(2)


class Database:
    """Comprehensive database for all PA features using PostgreSQL"""
    
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
        
        # Tasks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                priority TEXT DEFAULT 'medium',
                project TEXT DEFAULT 'inbox',
                labels TEXT,
                due_date TIMESTAMP,
                recurrence TEXT,
                parent_task_id INTEGER,
                completed INTEGER DEFAULT 0,
                completed_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP,
                job_id TEXT
            )
        ''')
        
        # Notes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notes (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                title TEXT,
                content TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        ''')
        
        # Habits table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS habits (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                goal_frequency TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        ''')
        
        # Habit completions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS habit_completions (
                id SERIAL PRIMARY KEY,
                habit_id INTEGER NOT NULL,
                completed_date DATE NOT NULL,
                FOREIGN KEY (habit_id) REFERENCES habits (id)
            )
        ''')
        
        # User preferences
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id BIGINT PRIMARY KEY,
                morning_briefing_time TEXT,
                evening_briefing_time TEXT,
                timezone TEXT DEFAULT 'UTC'
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_task(self, user_id: int, chat_id: int, title: str, description: str = None,
                 priority: str = 'medium', project: str = 'inbox', labels: str = None,
                 due_date: str = None, recurrence: str = None, parent_task_id: int = None,
                 job_id: str = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO tasks (user_id, chat_id, title, description, priority, project, 
                             labels, due_date, recurrence, parent_task_id, created_at, job_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (user_id, chat_id, title, description, priority, project, labels, due_date, 
              recurrence, parent_task_id, datetime.now(ZoneInfo('Europe/London')), job_id))
        
        task_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return task_id
    
    def update_task(self, task_id: int, **kwargs):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        updates = []
        values = []
        for key, value in kwargs.items():
            updates.append(f"{key} = %s")
            values.append(value)
        
        values.append(datetime.now(ZoneInfo('Europe/London')))
        values.append(task_id)
        
        query = f"UPDATE tasks SET {', '.join(updates)}, updated_at = %s WHERE id = %s"
        cursor.execute(query, values)
        conn.commit()
        conn.close()
    
    def get_task(self, task_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM tasks WHERE id = %s', (task_id,))
        result = cursor.fetchone()
        conn.close()
        return dict(result) if result else None
    
    def get_tasks(self, user_id: int, completed: bool = False, 
                  project: str = None, priority: str = None,
                  parent_task_id: int = None) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = 'SELECT * FROM tasks WHERE user_id = %s AND completed = %s'
        params = [user_id, 1 if completed else 0]
        
        if project:
            query += ' AND project = %s'
            params.append(project)
        
        if priority:
            query += ' AND priority = %s'
            params.append(priority)
        
        if parent_task_id is not None:
            query += ' AND parent_task_id = %s'
            params.append(parent_task_id)
        elif parent_task_id is None:
            query += ' AND parent_task_id IS NULL'
        
        query += ' ORDER BY due_date ASC, priority DESC, created_at DESC'
        
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def get_tasks_by_date(self, user_id: int, start_date: str, end_date: str) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # Add time boundaries to make date comparison work with timestamps
        start_datetime = f"{start_date} 00:00:00"
        end_datetime = f"{end_date} 23:59:59"
        cursor.execute('''
            SELECT * FROM tasks 
            WHERE user_id = %s AND completed = 0 
            AND due_date >= %s AND due_date <= %s
            ORDER BY due_date ASC, priority DESC
        ''', (user_id, start_datetime, end_datetime))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def complete_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE tasks SET completed = 1, completed_at = %s WHERE id = %s
        ''', (datetime.now(ZoneInfo('Europe/London')), task_id))
        conn.commit()
        conn.close()
    
    def delete_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks WHERE id = %s OR parent_task_id = %s', (task_id, task_id))
        conn.commit()
        conn.close()
    
    def search_tasks(self, user_id: int, query: str) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT * FROM tasks 
            WHERE user_id = %s AND completed = 0 
            AND (title ILIKE %s OR description ILIKE %s OR labels ILIKE %s)
            ORDER BY due_date ASC, priority DESC
        ''', (user_id, f'%{query}%', f'%{query}%', f'%{query}%'))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_subtasks(self, parent_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT * FROM tasks WHERE parent_task_id = %s', (parent_id,))
        results = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    # Notes
    def add_note(self, user_id: int, title: str, content: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO notes (user_id, title, content, created_at)
            VALUES (%s, %s, %s, %s)
        ''', (user_id, title, content, datetime.now(ZoneInfo('Europe/London'))))
        conn.commit()
        conn.close()
    
    def get_notes(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM notes WHERE user_id = %s ORDER BY created_at DESC', (user_id,))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    # Habits
    def add_habit(self, user_id: int, name: str, goal_frequency: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO habits (user_id, name, goal_frequency, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        ''', (user_id, name, goal_frequency, datetime.now(ZoneInfo('Europe/London'))))
        habit_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return habit_id
    
    def get_habits(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM habits WHERE user_id = %s', (user_id,))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def mark_habit_complete(self, habit_id: int, date: str = None):
        if not date:
            # Use UK timezone for habit completion
            date = datetime.now(ZoneInfo('Europe/London')).date().isoformat()
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Check if already marked
        cursor.execute('SELECT id FROM habit_completions WHERE habit_id = %s AND completed_date = %s',
                      (habit_id, date))
        if cursor.fetchone():
            conn.close()
            return False
        
        cursor.execute('INSERT INTO habit_completions (habit_id, completed_date) VALUES (%s, %s)',
                      (habit_id, date))
        conn.commit()
        conn.close()
        return True
    
    def get_habit_streak(self, habit_id: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT completed_date FROM habit_completions 
            WHERE habit_id = %s 
            ORDER BY completed_date DESC
        ''', (habit_id,))
        
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not dates:
            return 0
        
        streak = 0
        current_date = datetime.now(ZoneInfo('Europe/London')).date()
        
        for date_obj in dates:
            # Handle both date objects and strings
            if isinstance(date_obj, str):
                date = datetime.fromisoformat(date_obj).date()
            else:
                date = date_obj
                
            if date == current_date - timedelta(days=streak):
                streak += 1
            else:
                break
        
        return streak
    
    def get_habit_completions(self, habit_id: int, days: int = 30) -> List[str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = (datetime.now(ZoneInfo('Europe/London')) - timedelta(days=days)).date()
        cursor.execute('''
            SELECT completed_date FROM habit_completions 
            WHERE habit_id = %s AND completed_date >= %s
            ORDER BY completed_date DESC
        ''', (habit_id, start_date))
        
        results = [row[0].isoformat() if hasattr(row[0], 'isoformat') else row[0] for row in cursor.fetchall()]
        conn.close()
        return results
    
    # User preferences
    def set_briefing_times(self, user_id: int, morning: str = None, evening: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT user_id FROM user_preferences WHERE user_id = %s', (user_id,))
        exists = cursor.fetchone()
        
        if exists:
            updates = []
            params = []
            if morning:
                updates.append('morning_briefing_time = %s')
                params.append(morning)
            if evening:
                updates.append('evening_briefing_time = %s')
                params.append(evening)
            params.append(user_id)
            
            cursor.execute(f'UPDATE user_preferences SET {", ".join(updates)} WHERE user_id = %s', params)
        else:
            cursor.execute('''
                INSERT INTO user_preferences (user_id, morning_briefing_time, evening_briefing_time)
                VALUES (%s, %s, %s)
            ''', (user_id, morning, evening))
        
        conn.commit()
        conn.close()
    
    def get_user_preferences(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT * FROM user_preferences WHERE user_id = %s', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return dict(result) if result else None
    
    # Stats
    def get_stats(self, user_id: int, days: int = 7) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = (datetime.now(ZoneInfo('Europe/London')) - timedelta(days=days)).date()
        
        # Tasks completed
        cursor.execute('''
            SELECT COUNT(*) FROM tasks 
            WHERE user_id = %s AND completed = 1 AND completed_at >= %s
        ''', (user_id, start_date))
        completed = cursor.fetchone()[0]
        
        # Tasks created
        cursor.execute('''
            SELECT COUNT(*) FROM tasks 
            WHERE user_id = %s AND created_at >= %s
        ''', (user_id, start_date))
        created = cursor.fetchone()[0]
        
        # Active tasks
        cursor.execute('SELECT COUNT(*) FROM tasks WHERE user_id = %s AND completed = 0', (user_id,))
        active = cursor.fetchone()[0]
        
        # Overdue tasks
        cursor.execute('''
            SELECT COUNT(*) FROM tasks 
            WHERE user_id = %s AND completed = 0 AND due_date < %s
        ''', (user_id, datetime.now(ZoneInfo('Europe/London'))))
        overdue = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'completed': completed,
            'created': created,
            'active': active,
            'overdue': overdue,
            'completion_rate': round(completed / created * 100, 1) if created > 0 else 0
        }


class AIParser:
    """AI for complex natural language parsing"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
    
    async def parse_task(self, text: str) -> Optional[Dict]:
        # Get current UK time
        uk_time = datetime.now(ZoneInfo('Europe/London'))
        
        system_prompt = """Extract task information from natural language. Return ONLY valid JSON.

Current date/time (UK): {current_date}

Extract:
- "title": Main task (string)
- "description": Additional details or null
- "priority": "high", "medium", or "low"
- "project": Category like "work", "personal", "health", or null
- "labels": Comma-separated tags or null
- "due_date": ISO datetime or null
- "recurrence": "daily", "weekly", "monthly" or null

Examples:

"call Steve tomorrow at 3pm #urgent #work"
{{"title": "call Steve", "description": null, "priority": "high", "project": "work", "labels": "urgent,work", "due_date": "2025-10-02T15:00:00", "recurrence": null}}

"high priority: finish report by Friday"
{{"title": "finish report", "description": null, "priority": "high", "project": "work", "due_date": "2025-10-03", "recurrence": null}}

Return ONLY JSON.""".format(current_date=uk_time.strftime("%Y-%m-%d %H:%M %Z"))
        
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
                            {"role": "user", "content": text}
                        ],
                        "temperature": 0.1,
                        "max_tokens": 300
                    }
                )
                
                if response.status_code != 200:
                    return None
                
                content = response.json()['choices'][0]['message']['content'].strip()
                
                if content.startswith('```'):
                    content = content.split('```')[1]
                    if content.startswith('json'):
                        content = content[4:]
                    content = content.strip()
                
                return json.loads(content)
                
        except Exception as e:
            logger.error(f"AI error: {e}")
            return None


class PersonalAssistantBot:
    """Full-featured Personal Assistant Bot"""
    
    def __init__(self, telegram_token: str, groq_api_key: str):
        self.app = Application.builder().token(telegram_token).build()
        self.scheduler = AsyncIOScheduler(timezone='Europe/London')  # UK timezone
        self.db = Database()
        self.ai = AIParser(groq_api_key)
        self.user_timezone = ZoneInfo('Europe/London')  # Default to UK time
        
        # Conversation handler for editing
        edit_handler = ConversationHandler(
            entry_points=[CommandHandler('edit', self.edit_task_start)],
            states={
                EDIT_FIELD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.edit_task_field)]
            },
            fallbacks=[CommandHandler('cancel', self.cancel)]
        )
        
        # Command handlers
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("add", self.add_task_command))
        self.app.add_handler(CommandHandler("today", self.today_command))
        self.app.add_handler(CommandHandler("tomorrow", self.tomorrow_command))
        self.app.add_handler(CommandHandler("week", self.week_command))
        self.app.add_handler(CommandHandler("list", self.list_command))
        self.app.add_handler(CommandHandler("projects", self.projects_command))
        self.app.add_handler(CommandHandler("done", self.done_command))
        self.app.add_handler(CommandHandler("delete", self.delete_command))
        self.app.add_handler(CommandHandler("search", self.search_command))
        self.app.add_handler(CommandHandler("subtask", self.subtask_command))
        self.app.add_handler(CommandHandler("view", self.view_task_command))
        self.app.add_handler(edit_handler)
        
        # Habits
        self.app.add_handler(CommandHandler("habit", self.habit_command))
        self.app.add_handler(CommandHandler("habits", self.habits_command))
        self.app.add_handler(CommandHandler("check", self.check_habit_command))
        
        # Notes
        self.app.add_handler(CommandHandler("note", self.note_command))
        self.app.add_handler(CommandHandler("notes", self.notes_command))
        
        # Settings
        self.app.add_handler(CommandHandler("briefing", self.briefing_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        
        # Callback handlers
        self.app.add_handler(CallbackQueryHandler(self.button_callback))
        
        # Natural language handler
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Schedule daily briefings
        self.schedule_briefings()
    
    def schedule_briefings(self):
        """Schedule morning and evening briefings for all users"""
        # Morning briefing at 8 AM
        self.scheduler.add_job(
            self.send_morning_briefings,
            trigger=CronTrigger(hour=8, minute=0),
            id='morning_briefings'
        )
        
        # Evening check-in at 8 PM
        self.scheduler.add_job(
            self.send_evening_briefings,
            trigger=CronTrigger(hour=20, minute=0),
            id='evening_briefings'
        )
    
    async def send_morning_briefings(self):
        """Send morning briefing to all users"""
        # Get all users (would need a users table in production)
        pass  # Implement when you have multiple users
    
    async def send_morning_briefing(self, chat_id: int, user_id: int):
        """Send morning briefing to a user"""
        today = datetime.now(self.user_timezone).date()
        tasks = self.db.get_tasks_by_date(user_id, today.isoformat(), today.isoformat())
        
        message = "☀️ Good morning!\n\n"
        
        if tasks:
            message += f"You have {len(tasks)} tasks today:\n\n"
            for task in tasks[:5]:
                priority_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
                emoji = priority_emoji.get(task['priority'], '⚪')
                message += f"{emoji} {task['title']}\n"
            
            if len(tasks) > 5:
                message += f"\n... and {len(tasks) - 5} more"
        else:
            message += "No tasks scheduled for today. Enjoy your day!"
        
        await self.app.bot.send_message(chat_id=chat_id, text=message)
    
    async def send_evening_briefings(self):
        """Send evening check-in to all users"""
        pass  # Implement when you have multiple users
    
    async def send_evening_briefing(self, chat_id: int, user_id: int):
        """Send evening check-in"""
        habits = self.db.get_habits(user_id)
        today = datetime.now(self.user_timezone).date().isoformat()
        
        if not habits:
            return
        
        message = "🌙 Evening check-in!\n\nDid you complete your habits today?\n\n"
        
        keyboard = []
        for habit in habits:
            completions = self.db.get_habit_completions(habit['id'], days=1)
            if today not in completions:
                keyboard.append([InlineKeyboardButton(
                    f"✓ {habit['name']}", 
                    callback_data=f"habit_{habit['id']}"
                )])
        
        if keyboard:
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.app.bot.send_message(
                chat_id=chat_id, 
                text=message, 
                reply_markup=reply_markup
            )
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("📋 Today's Tasks", callback_data="today")],
            [InlineKeyboardButton("➕ Add Task", callback_data="add")],
            [InlineKeyboardButton("📊 Stats", callback_data="stats")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "👋 Welcome to your Personal Assistant!\n\n"
            "I can help you manage tasks, track habits, take notes, and stay organized.\n\n"
            "Use /help to see all commands, or just start talking to me naturally!",
            reply_markup=reply_markup
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = """**Personal Assistant Commands**

**Tasks:**
/add [task] - Add task
/today - Today's tasks
/tomorrow - Tomorrow's tasks
/week - This week
/list - All active tasks
/projects - View by project
/done [id] - Mark complete
/delete [id] - Delete task
/edit [id] - Edit task
/view [id] - View details
/subtask [parent_id] [task] - Add subtask
/search [query] - Find tasks

**Habits:**
/habit [name] - Add habit to track
/habits - View all habits
/check [habit_id] - Mark habit complete

**Notes:**
/note [text] - Quick note
/notes - View all notes

**Stats & Settings:**
/stats - Your productivity stats
/briefing - Set up daily briefings

**Natural Language:**
Just type: "call Steve tomorrow at 3pm" or "high priority: finish report #work"
"""
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def add_task_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /add Your task here")
            return
        
        text = ' '.join(context.args)
        await self.create_task_from_text(update, text)
    
    async def create_task_from_text(self, update: Update, text: str):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Try simple parsing first
        simple_task = self.parse_simple_task(text)
        
        if simple_task:
            parsed = simple_task
        else:
            # Use AI
            await update.message.chat.send_action("typing")
            parsed = await self.ai.parse_task(text)
        
        if not parsed or 'title' not in parsed:
            await update.message.reply_text("Couldn't understand. Try: /add Task description")
            return
        
        # Add to database
        task_id = self.db.add_task(
            user_id=user_id,
            chat_id=chat_id,
            title=parsed['title'],
            description=parsed.get('description'),
            priority=parsed.get('priority', 'medium'),
            project=parsed.get('project', 'inbox'),
            labels=parsed.get('labels'),
            due_date=parsed.get('due_date'),
            recurrence=parsed.get('recurrence')
        )
        
        # Schedule reminder
        if parsed.get('due_date'):
            await self.schedule_task_reminder(task_id, parsed, chat_id)
            logger.info(f"Scheduled reminder for task {task_id} at {parsed.get('due_date')}")
        
        # Response
        priority_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        response = f"{priority_emoji.get(parsed.get('priority', 'medium'), '⚪')} Task added\n\n"
        response += f"**{parsed['title']}**\n"
        
        if parsed.get('due_date'):
            due_dt = datetime.fromisoformat(parsed['due_date'])
            response += f"📅 {due_dt.strftime('%b %d at %I:%M %p')}\n"
        
        if parsed.get('project'):
            response += f"📁 {parsed['project'].title()}\n"
        
        if parsed.get('labels'):
            response += f"🏷️ {parsed['labels']}\n"
        
        response += f"\nID: {task_id}"
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
    def parse_simple_task(self, text: str) -> Optional[Dict]:
        """Quick pattern matching without AI"""
        priority = 'medium'
        if re.search(r'\b(urgent|important|high priority)\b', text, re.I):
            priority = 'high'
        elif re.search(r'\b(low priority)\b', text, re.I):
            priority = 'low'
        
        # Extract hashtags as labels
        labels = re.findall(r'#(\w+)', text)
        text_clean = re.sub(r'#\w+', '', text).strip()
        
        if not re.search(r'\b(tomorrow|today|next|monday|tuesday|wednesday|thursday|friday|saturday|sunday|\d)', text, re.I):
            return {
                'title': text_clean,
                'priority': priority,
                'project': 'inbox',
                'labels': ','.join(labels) if labels else None
            }
        
        return None
    
    async def schedule_task_reminder(self, task_id: int, parsed: Dict, chat_id: int):
        """Schedule reminder for task"""
        job_id = f"task_{task_id}"
        
        # Parse the due_date string and make it timezone-aware
        due_date_str = parsed['due_date']
        
        # Handle both string and datetime inputs
        if isinstance(due_date_str, str):
            # Parse ISO format string
            if 'T' in due_date_str:
                # Has time component
                due_dt = datetime.fromisoformat(due_date_str.replace('Z', '+00:00'))
            else:
                # Date only - add default time
                due_dt = datetime.fromisoformat(due_date_str)
                due_dt = due_dt.replace(hour=9, minute=0)  # Default to 9 AM
            
            # Make timezone-aware if it isn't already
            if due_dt.tzinfo is None:
                due_dt = due_dt.replace(tzinfo=self.user_timezone)
        else:
            due_dt = due_date_str
        
        # Convert to UK timezone if in different timezone
        if due_dt.tzinfo != self.user_timezone:
            due_dt = due_dt.astimezone(self.user_timezone)
        
        # Update task with the job_id
        self.db.update_task(task_id, job_id=job_id)
        
        if parsed.get('recurrence'):
            trigger = self.get_recurrence_trigger(due_dt, parsed['recurrence'])
            if trigger:
                self.scheduler.add_job(
                    self.send_task_reminder,
                    trigger=trigger,
                    args=[chat_id, parsed['title'], task_id],
                    id=job_id,
                    replace_existing=True
                )
        else:
            # Only schedule if the time is in the future
            now = datetime.now(self.user_timezone)
            if due_dt > now:
                self.scheduler.add_job(
                    self.send_task_reminder,
                    trigger=DateTrigger(run_date=due_dt),
                    args=[chat_id, parsed['title'], task_id],
                    id=job_id,
                    replace_existing=True
                )
    
    def get_recurrence_trigger(self, start_time: datetime, recurrence: str):
        """Create scheduler trigger for recurring tasks"""
        if recurrence == 'daily':
            return CronTrigger(hour=start_time.hour, minute=start_time.minute)
        elif recurrence == 'weekly':
            return CronTrigger(day_of_week=start_time.weekday(), hour=start_time.hour, minute=start_time.minute)
        elif recurrence == 'monthly':
            return CronTrigger(day=start_time.day, hour=start_time.hour, minute=start_time.minute)
        return None
    
    async def today_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        today = datetime.now(self.user_timezone).date()
        tasks = self.db.get_tasks_by_date(user_id, today.isoformat(), today.isoformat())
        await self.send_task_list(update, tasks, "📅 Today's Tasks")
    
    async def tomorrow_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        tomorrow = (datetime.now(self.user_timezone) + timedelta(days=1)).date()
        tasks = self.db.get_tasks_by_date(user_id, tomorrow.isoformat(), tomorrow.isoformat())
        await self.send_task_list(update, tasks, "📅 Tomorrow")
    
    async def week_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        today = datetime.now(self.user_timezone).date()
        week_end = today + timedelta(days=7)
        tasks = self.db.get_tasks_by_date(user_id, today.isoformat(), week_end.isoformat())
        await self.send_task_list(update, tasks, "📅 This Week")
    
    async def list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        tasks = self.db.get_tasks(user_id)
        await self.send_task_list(update, tasks, "📋 All Tasks")
    
    async def projects_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        tasks = self.db.get_tasks(user_id)
        
        projects = {}
        for task in tasks:
            proj = task['project'] or 'inbox'
            if proj not in projects:
                projects[proj] = []
            projects[proj].append(task)
        
        message = "**📁 Projects**\n\n"
        for project, project_tasks in projects.items():
            message += f"**{project.title()}**\n"
            for task in project_tasks[:3]:
                message += f"  • {task['title']} (ID: {task['id']})\n"
            if len(project_tasks) > 3:
                message += f"  ... +{len(project_tasks) - 3} more\n"
            message += "\n"
        
        await update.message.reply_text(message or "No tasks yet.", parse_mode='Markdown')
    
    async def send_task_list(self, update: Update, tasks: List[Dict], title: str):
        if not tasks:
            await update.message.reply_text(f"{title}\n\nNo tasks found.")
            return
        
        priority_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        
        message = f"**{title}**\n\n"
        for task in tasks[:15]:
            emoji = priority_emoji.get(task['priority'], '⚪')
            message += f"{emoji} {task['title']}\n"
            
            if task['due_date']:
                due = datetime.fromisoformat(task['due_date'])
                message += f"  📅 {due.strftime('%b %d, %I:%M %p')}\n"
            
            if task['project'] and task['project'] != 'inbox':
                message += f"  📁 {task['project']}\n"
            
            # Check for subtasks
            subtasks = self.db.get_subtasks(task['id'])
            if subtasks:
                completed_sub = sum(1 for s in subtasks if s['completed'])
                message += f"  🔎 {completed_sub}/{len(subtasks)} subtasks\n"
            
            message += f"  ID: {task['id']}\n\n"
        
        if len(tasks) > 15:
            message += f"... and {len(tasks) - 15} more"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def done_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /done [task_id]")
            return
        
        try:
            task_id = int(context.args[0])
            self.db.complete_task(task_id)
            await update.message.reply_text(f"✅ Task completed!")
        except ValueError:
            await update.message.reply_text("Invalid task ID.")
    
    async def delete_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /delete [task_id]")
            return
        
        try:
            task_id = int(context.args[0])
            task = self.db.get_task(task_id)
            if not task:
                await update.message.reply_text("Task not found.")
                return
            
            self.db.delete_task(task_id)
            await update.message.reply_text(f"🗑️ Task deleted!")
        except ValueError:
            await update.message.reply_text("Invalid task ID.")
    
    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /search [query]")
            return
        
        user_id = update.effective_user.id
        query = ' '.join(context.args)
        tasks = self.db.search_tasks(user_id, query)
        await self.send_task_list(update, tasks, f"🔍 '{query}'")
    
    async def view_task_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /view [task_id]")
            return
        
        try:
            task_id = int(context.args[0])
            task = self.db.get_task(task_id)
            
            if not task:
                await update.message.reply_text("Task not found.")
                return
            
            priority_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}
            
            message = f"{priority_emoji.get(task['priority'], '⚪')} **Task**\n\n"
            message += f"**{task['title']}**\n\n"
            
            if task['description']:
                message += f"{task['description']}\n\n"
            
            message += f"**Priority:** {task['priority'].title()}\n"
            message += f"**Project:** {task['project'].title()}\n"
            
            if task['labels']:
                message += f"**Labels:** {task['labels']}\n"
            
            if task['due_date']:
                due = datetime.fromisoformat(task['due_date'])
                message += f"**Due:** {due.strftime('%B %d at %I:%M %p')}\n"
            
            if task['recurrence']:
                message += f"**Repeats:** {task['recurrence'].title()}\n"
            
            # Subtasks
            subtasks = self.db.get_subtasks(task['id'])
            if subtasks:
                message += f"\n**Subtasks ({len(subtasks)}):**\n"
                for sub in subtasks:
                    status = "✅" if sub['completed'] else "⬜"
                    message += f"{status} {sub['title']}\n"
            
            message += f"\nID: {task_id}"
            
            keyboard = [
                [InlineKeyboardButton("✅ Complete", callback_data=f"done_{task_id}"),
                 InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_id}")],
                [InlineKeyboardButton("⏰ Snooze", callback_data=f"snooze_{task_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(message, parse_mode='Markdown', reply_markup=reply_markup)
        except ValueError:
            await update.message.reply_text("Invalid task ID.")
    
    async def edit_task_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /edit [task_id]")
            return ConversationHandler.END
        
        try:
            task_id = int(context.args[0])
            task = self.db.get_task(task_id)
            
            if not task:
                await update.message.reply_text("Task not found.")
                return ConversationHandler.END
            
            context.user_data['editing_task_id'] = task_id
            
            keyboard = [
                [InlineKeyboardButton("Title", callback_data="edit_title")],
                [InlineKeyboardButton("Priority", callback_data="edit_priority")],
                [InlineKeyboardButton("Project", callback_data="edit_project")],
                [InlineKeyboardButton("Due Date", callback_data="edit_due_date")],
                [InlineKeyboardButton("Cancel", callback_data="edit_cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"Editing task: {task['title']}\n\nWhat would you like to edit?",
                reply_markup=reply_markup
            )
            return EDIT_FIELD
            
        except ValueError:
            await update.message.reply_text("Invalid task ID.")
            return ConversationHandler.END
    
    async def edit_task_field(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        task_id = context.user_data.get('editing_task_id')
        field = context.user_data.get('editing_field')
        new_value = update.message.text
        
        if field == 'title':
            self.db.update_task(task_id, title=new_value)
        elif field == 'priority':
            priority = new_value.lower()
            if priority in ['high', 'medium', 'low']:
                self.db.update_task(task_id, priority=priority)
            else:
                await update.message.reply_text("Priority must be: high, medium, or low")
                return EDIT_FIELD
        elif field == 'project':
            self.db.update_task(task_id, project=new_value.lower())
        
        await update.message.reply_text(f"✅ Task updated!")
        return ConversationHandler.END
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END
    
    async def subtask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /subtask [parent_id] [subtask title]")
            return
        
        try:
            parent_id = int(context.args[0])
            title = ' '.join(context.args[1:])
            
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            
            subtask_id = self.db.add_task(
                user_id=user_id,
                chat_id=chat_id,
                title=title,
                parent_task_id=parent_id
            )
            
            await update.message.reply_text(f"✅ Subtask added\nID: {subtask_id}")
        except ValueError:
            await update.message.reply_text("Invalid parent task ID.")
    
    async def habit_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /habit [habit name]")
            return
        
        user_id = update.effective_user.id
        name = ' '.join(context.args)
        habit_id = self.db.add_habit(user_id, name, 'daily')
        
        await update.message.reply_text(f"✅ Habit '{name}' added (ID: {habit_id})")
    
    async def habits_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        habits = self.db.get_habits(user_id)
        
        if not habits:
            await update.message.reply_text("No habits yet. Use /habit to add one.")
            return
        
        message = "**🎯 Your Habits**\n\n"
        for habit in habits:
            streak = self.db.get_habit_streak(habit['id'])
            completions = self.db.get_habit_completions(habit['id'], days=7)
            
            message += f"**{habit['name']}** (ID: {habit['id']})\n"
            message += f"  🔥 {streak} day streak\n"
            message += f"  ✓ {len(completions)}/7 this week\n\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def check_habit_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /check [habit_id]")
            return
        
        try:
            habit_id = int(context.args[0])
            success = self.db.mark_habit_complete(habit_id)
            
            if success:
                streak = self.db.get_habit_streak(habit_id)
                await update.message.reply_text(f"✅ Habit completed! 🔥 {streak} day streak")
            else:
                await update.message.reply_text("Already completed today!")
        except ValueError:
            await update.message.reply_text("Invalid habit ID.")
    
    async def note_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /note [your note]")
            return
        
        user_id = update.effective_user.id
        content = ' '.join(context.args)
        self.db.add_note(user_id, None, content)
        await update.message.reply_text("📝 Note saved!")
    
    async def notes_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        notes = self.db.get_notes(user_id)
        
        if not notes:
            await update.message.reply_text("No notes yet.")
            return
        
        message = "**📝 Notes**\n\n"
        for note in notes[:10]:
            created = datetime.fromisoformat(note['created_at'])
            message += f"**{created.strftime('%b %d')}**\n{note['content']}\n\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def briefing_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Set your daily briefing times:\n\n"
            "Morning briefing shows your tasks for the day\n"
            "Evening check-in asks about habit completion\n\n"
            "Use: /briefing morning 08:00\n"
            "Use: /briefing evening 20:00"
        )
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        stats = self.db.get_stats(user_id, days=7)
        
        message = "**📊 Your Stats (Last 7 Days)**\n\n"
        message += f"✅ Completed: {stats['completed']} tasks\n"
        message += f"➕ Created: {stats['created']} tasks\n"
        message += f"📋 Active: {stats['active']} tasks\n"
        message += f"⚠️ Overdue: {stats['overdue']} tasks\n"
        message += f"📈 Completion rate: {stats['completion_rate']}%\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def send_task_reminder(self, chat_id: int, title: str, task_id: int):
        """Send task reminder with actions"""
        try:
            keyboard = [
                [InlineKeyboardButton("✅ Done", callback_data=f"done_{task_id}")],
                [InlineKeyboardButton("⏰ 5 min", callback_data=f"snooze5_{task_id}"),
                 InlineKeyboardButton("⏰ 1 hour", callback_data=f"snooze60_{task_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.app.bot.send_message(
                chat_id=chat_id,
                text=f"🔔 {title}",
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Reminder error: {e}")
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all button callbacks"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data.startswith("done_"):
            task_id = int(data.split("_")[1])
            self.db.complete_task(task_id)
            await query.edit_message_text(f"✅ Task completed!")
        
        elif data.startswith("delete_"):
            task_id = int(data.split("_")[1])
            self.db.delete_task(task_id)
            await query.edit_message_text(f"🗑️ Task deleted!")
        
        elif data.startswith("snooze5_"):
            task_id = int(data.split("_")[1])
            task = self.db.get_task(task_id)
            if task and task['due_date']:
                # Parse current due_date
                if isinstance(task['due_date'], str):
                    current_due = datetime.fromisoformat(task['due_date'])
                else:
                    current_due = task['due_date']
                
                # Make timezone-aware if needed
                if current_due.tzinfo is None:
                    current_due = current_due.replace(tzinfo=self.user_timezone)
                
                # Add 5 minutes
                new_due = current_due + timedelta(minutes=5)
                self.db.update_task(task_id, due_date=new_due)
                
                # Reschedule the reminder
                job_id = f"task_{task_id}"
                try:
                    self.scheduler.remove_job(job_id)
                except:
                    pass
                
                self.scheduler.add_job(
                    self.send_task_reminder,
                    trigger=DateTrigger(run_date=new_due),
                    args=[query.message.chat_id, task['title'], task_id],
                    id=job_id,
                    replace_existing=True
                )
                
                await query.edit_message_text(f"⏰ Snoozed 5 minutes")
        
        elif data.startswith("snooze60_"):
            task_id = int(data.split("_")[1])
            task = self.db.get_task(task_id)
            if task and task['due_date']:
                # Parse current due_date
                if isinstance(task['due_date'], str):
                    current_due = datetime.fromisoformat(task['due_date'])
                else:
                    current_due = task['due_date']
                
                # Make timezone-aware if needed
                if current_due.tzinfo is None:
                    current_due = current_due.replace(tzinfo=self.user_timezone)
                
                # Add 1 hour
                new_due = current_due + timedelta(hours=1)
                self.db.update_task(task_id, due_date=new_due)
                
                # Reschedule the reminder
                job_id = f"task_{task_id}"
                try:
                    self.scheduler.remove_job(job_id)
                except:
                    pass
                
                self.scheduler.add_job(
                    self.send_task_reminder,
                    trigger=DateTrigger(run_date=new_due),
                    args=[query.message.chat_id, task['title'], task_id],
                    id=job_id,
                    replace_existing=True
                )
                
                await query.edit_message_text(f"⏰ Snoozed 1 hour")
        
        elif data.startswith("habit_"):
            habit_id = int(data.split("_")[1])
            self.db.mark_habit_complete(habit_id)
            streak = self.db.get_habit_streak(habit_id)
            await query.edit_message_text(f"✅ Habit completed! 🔥 {streak} day streak")
        
        elif data == "today":
            # Handle inline button for today's tasks
            pass
        
        elif data.startswith("edit_"):
            field = data.split("_")[1]
            if field == "cancel":
                await query.edit_message_text("Edit cancelled.")
                return ConversationHandler.END
            
            context.user_data['editing_field'] = field
            await query.edit_message_text(f"Send new value for {field}:")
            return EDIT_FIELD
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle natural language"""
        text = update.message.text
        
        task_indicators = ['remind', 'task', 'todo', 'call', 'meeting', 'buy', 'do', 'finish']
        
        if any(word in text.lower() for word in task_indicators):
            await self.create_task_from_text(update, text)
        else:
            await update.message.reply_text(
                "I can help with tasks, habits, and notes!\n"
                "Try saying: 'call Steve tomorrow at 3pm'\n"
                "Or use /help for all commands."
            )
    
    def run(self):
        """Start the bot"""
        self.scheduler.start()
        logger.info("Personal Assistant Bot starting...")
        logger.info(f"Scheduler started: {self.scheduler.running}")
        logger.info(f"Using timezone: Europe/London")
        logger.info(f"Current time: {datetime.now(self.user_timezone)}")
        
        # Set up command descriptions
        async def set_commands():
            commands = [
                BotCommand("start", "Start the bot"),
                BotCommand("help", "Show all commands"),
                BotCommand("add", "Add a new task"),
                BotCommand("today", "View today's tasks"),
                BotCommand("tomorrow", "View tomorrow's tasks"),
                BotCommand("week", "View this week's tasks"),
                BotCommand("list", "View all active tasks"),
                BotCommand("projects", "View tasks by project"),
                BotCommand("done", "Mark task as complete"),
                BotCommand("delete", "Delete a task"),
                BotCommand("edit", "Edit a task"),
                BotCommand("view", "View task details"),
                BotCommand("search", "Search tasks"),
                BotCommand("subtask", "Add a subtask"),
                BotCommand("habit", "Add a habit to track"),
                BotCommand("habits", "View all habits"),
                BotCommand("check", "Mark habit complete"),
                BotCommand("note", "Save a quick note"),
                BotCommand("notes", "View all notes"),
                BotCommand("stats", "View your productivity stats"),
                BotCommand("briefing", "Set daily briefing times")
            ]
            await self.app.bot.set_my_commands(commands)
        
        # Run the command setup
        import asyncio
        asyncio.get_event_loop().run_until_complete(set_commands())
        
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    
    if not TELEGRAM_TOKEN or not GROQ_API_KEY:
        logger.error("Missing environment variables!")
        exit(1)
    
    bot = PersonalAssistantBot(TELEGRAM_TOKEN, GROQ_API_KEY)
    bot.run()

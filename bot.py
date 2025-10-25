import os
import json
import logging
import signal
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
import traceback

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, MessageReactionHandler, filters, ContextTypes, CallbackQueryHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
import httpx

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url: raise ValueError("DATABASE_URL not set")
        self.init_db()

    def get_connection(self):
        return psycopg2.connect(self.database_url)

    def init_db(self):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute('ALTER TABLE message_task_map DROP COLUMN chat_id, DROP COLUMN created_at;')
                    logger.info("Successfully migrated message_task_map table structure.")
                except psycopg2.Error:
                    conn.rollback()

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
                        title TEXT NOT NULL, due_date TIMESTAMP WITH TIME ZONE,
                        completed INTEGER DEFAULT 0, completed_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL, job_id TEXT,
                        recurrence_rule TEXT
                    )
                ''')
                
                # Add recurrence_rule column if it doesn't exist
                try:
                    cursor.execute('ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recurrence_rule TEXT')
                    conn.commit()
                except psycopg2.Error:
                    conn.rollback()
                
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
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_facts (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        fact_key TEXT NOT NULL,
                        fact_value TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        UNIQUE(user_id, fact_key)
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS goals (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        title TEXT NOT NULL,
                        description TEXT,
                        target_date TIMESTAMP WITH TIME ZONE,
                        status TEXT DEFAULT 'active',
                        progress INTEGER DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        completed_at TIMESTAMP WITH TIME ZONE
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS habits (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        title TEXT NOT NULL,
                        frequency TEXT NOT NULL,
                        current_streak INTEGER DEFAULT 0,
                        longest_streak INTEGER DEFAULT 0,
                        last_completed TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        active INTEGER DEFAULT 1
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS habit_completions (
                        id SERIAL PRIMARY KEY,
                        habit_id INTEGER NOT NULL,
                        completed_at TIMESTAMP WITH TIME ZONE NOT NULL
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS insights (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        insight_type TEXT NOT NULL,
                        content TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        acknowledged INTEGER DEFAULT 0
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS check_ins (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        check_in_type TEXT NOT NULL,
                        last_sent TIMESTAMP WITH TIME ZONE NOT NULL,
                        UNIQUE(user_id, check_in_type)
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_preferences (
                        user_id BIGINT PRIMARY KEY,
                        working_hours_start INTEGER DEFAULT 9,
                        working_hours_end INTEGER DEFAULT 17,
                        energy_peak TEXT DEFAULT 'morning',
                        focus_style TEXT,
                        motivation_style TEXT
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

    def get_task_by_id(self, task_id: int) -> Optional[Dict]:
        return self._execute_query('SELECT * FROM tasks WHERE id = %s', (task_id,), fetch='one')

    def get_tasks_for_today(self, user_id: int) -> List[Dict]:
        now = datetime.now(ZoneInfo('Europe/London'))
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        
        query = """
            SELECT * FROM tasks 
            WHERE user_id = %s 
            AND completed = 0 
            AND due_date >= %s 
            AND due_date < %s
            ORDER BY due_date ASC
        """
        return self._execute_query(query, (user_id, start_of_day, end_of_day), fetch='all')

    def add_task(self, user_id: int, chat_id: int, title: str, due_date: Optional[datetime] = None, recurrence_rule: Optional[str] = None) -> int:
        now = datetime.now(ZoneInfo('Europe/London'))
        result = self._execute_query(
            'INSERT INTO tasks (user_id, chat_id, title, due_date, created_at, recurrence_rule) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id',
            (user_id, chat_id, title, due_date, now, recurrence_rule), fetch='one'
        )
        return result['id']

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
        return result['task_id'] if result else None
    
    def get_active_users(self) -> List[Dict]:
        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(days=7)
        return self._execute_query('SELECT user_id, chat_id FROM active_users WHERE last_interaction > %s', (cutoff,), fetch='all')

    def add_user_fact(self, user_id: int, key: str, value: str):
        now = datetime.now(ZoneInfo('Europe/London'))
        self._execute_query(
            'INSERT INTO user_facts (user_id, fact_key, fact_value, created_at) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, fact_key) DO UPDATE SET fact_value = EXCLUDED.fact_value',
            (user_id, key.lower(), value, now)
        )

    def get_user_facts(self, user_id: int) -> List[Dict]:
        return self._execute_query(
            'SELECT fact_key, fact_value FROM user_facts WHERE user_id = %s ORDER BY fact_key',
            (user_id,),
            fetch='all'
        )

    # Goal methods
    def add_goal(self, user_id: int, title: str, description: str = None, target_date: datetime = None) -> int:
        now = datetime.now(ZoneInfo('Europe/London'))
        result = self._execute_query(
            'INSERT INTO goals (user_id, title, description, target_date, created_at) VALUES (%s, %s, %s, %s, %s) RETURNING id',
            (user_id, title, description, target_date, now), fetch='one'
        )
        return result['id']

    def get_goals(self, user_id: int, status: str = 'active') -> List[Dict]:
        return self._execute_query(
            'SELECT * FROM goals WHERE user_id = %s AND status = %s ORDER BY target_date ASC NULLS LAST',
            (user_id, status),
            fetch='all'
        )

    def update_goal(self, goal_id: int, **kwargs):
        updates = ', '.join([f"{key} = %s" for key in kwargs])
        self._execute_query(f"UPDATE goals SET {updates} WHERE id = %s", list(kwargs.values()) + [goal_id])

    def complete_goal(self, goal_id: int):
        self.update_goal(goal_id, status='completed', completed_at=datetime.now(ZoneInfo('Europe/London')))

    # Habit methods
    def add_habit(self, user_id: int, title: str, frequency: str = 'daily') -> int:
        now = datetime.now(ZoneInfo('Europe/London'))
        result = self._execute_query(
            'INSERT INTO habits (user_id, title, frequency, created_at) VALUES (%s, %s, %s, %s) RETURNING id',
            (user_id, title, frequency, now), fetch='one'
        )
        return result['id']

    def get_habits(self, user_id: int, active_only: bool = True) -> List[Dict]:
        query = 'SELECT * FROM habits WHERE user_id = %s'
        if active_only:
            query += ' AND active = 1'
        query += ' ORDER BY current_streak DESC, title'
        return self._execute_query(query, (user_id,), fetch='all')

    def complete_habit(self, habit_id: int):
        now = datetime.now(ZoneInfo('Europe/London'))
        habit = self._execute_query('SELECT * FROM habits WHERE id = %s', (habit_id,), fetch='one')
        if not habit:
            return

        # Check if already completed today
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        recent = self._execute_query(
            'SELECT * FROM habit_completions WHERE habit_id = %s AND completed_at >= %s',
            (habit_id, today_start), fetch='one'
        )

        if recent:
            return  # Already completed today

        # Record completion
        self._execute_query(
            'INSERT INTO habit_completions (habit_id, completed_at) VALUES (%s, %s)',
            (habit_id, now)
        )

        # Update streak
        last_completed = habit.get('last_completed')
        current_streak = habit.get('current_streak', 0)

        if last_completed:
            yesterday_start = today_start - timedelta(days=1)
            if last_completed >= yesterday_start:
                current_streak += 1
            else:
                current_streak = 1
        else:
            current_streak = 1

        longest_streak = max(current_streak, habit.get('longest_streak', 0))
        self._execute_query(
            'UPDATE habits SET current_streak = %s, longest_streak = %s, last_completed = %s WHERE id = %s',
            (current_streak, longest_streak, now, habit_id)
        )

    def get_habit_stats(self, habit_id: int, days: int = 30) -> Dict:
        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(days=days)
        completions = self._execute_query(
            'SELECT COUNT(*) as count FROM habit_completions WHERE habit_id = %s AND completed_at >= %s',
            (habit_id, cutoff), fetch='one'
        )
        return {'completions_last_30_days': completions['count'] if completions else 0}

    # Insight methods
    def add_insight(self, user_id: int, insight_type: str, content: str):
        now = datetime.now(ZoneInfo('Europe/London'))
        self._execute_query(
            'INSERT INTO insights (user_id, insight_type, content, created_at) VALUES (%s, %s, %s, %s)',
            (user_id, insight_type, content, now)
        )

    def get_unacknowledged_insights(self, user_id: int, limit: int = 3) -> List[Dict]:
        return self._execute_query(
            'SELECT * FROM insights WHERE user_id = %s AND acknowledged = 0 ORDER BY created_at DESC LIMIT %s',
            (user_id, limit), fetch='all'
        )

    def acknowledge_insight(self, insight_id: int):
        self._execute_query('UPDATE insights SET acknowledged = 1 WHERE id = %s', (insight_id,))

    # Check-in methods
    def record_check_in(self, user_id: int, check_in_type: str):
        now = datetime.now(ZoneInfo('Europe/London'))
        self._execute_query(
            'INSERT INTO check_ins (user_id, check_in_type, last_sent) VALUES (%s, %s, %s) ON CONFLICT (user_id, check_in_type) DO UPDATE SET last_sent = EXCLUDED.last_sent',
            (user_id, check_in_type, now)
        )

    def should_send_check_in(self, user_id: int, check_in_type: str, hours_between: int = 24) -> bool:
        result = self._execute_query(
            'SELECT last_sent FROM check_ins WHERE user_id = %s AND check_in_type = %s',
            (user_id, check_in_type), fetch='one'
        )
        if not result:
            return True

        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(hours=hours_between)
        return result['last_sent'] < cutoff

    # Analytics methods
    def get_completion_rate(self, user_id: int, days: int = 7) -> float:
        """Get task completion rate over the last N days"""
        cutoff = datetime.now(ZoneInfo('Europe/London')) - timedelta(days=days)
        total = self._execute_query(
            'SELECT COUNT(*) as count FROM tasks WHERE user_id = %s AND created_at >= %s',
            (user_id, cutoff), fetch='one'
        )
        completed = self._execute_query(
            'SELECT COUNT(*) as count FROM tasks WHERE user_id = %s AND completed = 1 AND completed_at >= %s',
            (user_id, cutoff), fetch='one'
        )

        if not total or total['count'] == 0:
            return 0.0
        return (completed['count'] / total['count']) * 100

    def get_productivity_patterns(self, user_id: int) -> Dict:
        """Analyze when user completes most tasks"""
        completed_tasks = self._execute_query(
            'SELECT completed_at FROM tasks WHERE user_id = %s AND completed = 1 AND completed_at IS NOT NULL ORDER BY completed_at DESC LIMIT 100',
            (user_id,), fetch='all'
        )

        if not completed_tasks:
            return {}

        morning = afternoon = evening = 0
        for task in completed_tasks:
            hour = task['completed_at'].hour
            if 5 <= hour < 12:
                morning += 1
            elif 12 <= hour < 17:
                afternoon += 1
            else:
                evening += 1

        total = len(completed_tasks)
        return {
            'morning_pct': round((morning / total) * 100, 1),
            'afternoon_pct': round((afternoon / total) * 100, 1),
            'evening_pct': round((evening / total) * 100, 1),
            'most_productive': max([('morning', morning), ('afternoon', afternoon), ('evening', evening)], key=lambda x: x[1])[0]
        }


class ConversationAI:
    def __init__(self, api_key: str, db: Database, timezone: ZoneInfo):
        self.api_key = api_key
        self.db = db
        self.timezone = timezone
        # Using Gemini 2.5 Flash (stable version)
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={self.api_key}"

    def _format_tasks(self, tasks: List[Dict], title: str) -> str:
        if not tasks: return f"{title}:\n- None"
        formatted_tasks = []
        for t in tasks:
            recurrence = f" [Recurring: {t['recurrence_rule']}]" if t.get('recurrence_rule') else ""
            due_info = f"Due: {t['due_date'].astimezone(self.timezone).strftime('%A at %I:%M %p (%d %b)')}" if t['due_date'] else 'Not scheduled'
            formatted_tasks.append(f"- ID {t['id']}: {t['title']} ({due_info}){recurrence}")
        return f"{title}:\n" + "\n".join(formatted_tasks)

    def _format_facts(self, facts: List[Dict]) -> str:
        if not facts: return "PERSONAL FACTS:\n- None"
        formatted_facts = [f"- {fact['fact_key']}: {fact['fact_value']}" for fact in facts]
        return "PERSONAL FACTS:\n" + "\n".join(formatted_facts)

    def _format_goals(self, goals: List[Dict]) -> str:
        if not goals: return "GOALS:\n- None"
        formatted_goals = []
        for g in goals:
            progress = f" [{g['progress']}% complete]" if g.get('progress', 0) > 0 else ""
            target = f"Target: {g['target_date'].astimezone(self.timezone).strftime('%d %b %Y')}" if g.get('target_date') else 'No deadline'
            formatted_goals.append(f"- ID {g['id']}: {g['title']} ({target}){progress}")
        return "GOALS:\n" + "\n".join(formatted_goals)

    def _format_habits(self, habits: List[Dict]) -> str:
        if not habits: return "HABITS:\n- None"
        formatted_habits = []
        for h in habits:
            streak_info = f"🔥 {h['current_streak']} day streak" if h['current_streak'] > 0 else "No streak yet"
            formatted_habits.append(f"- ID {h['id']}: {h['title']} ({h['frequency']}) - {streak_info}")
        return "HABITS:\n" + "\n".join(formatted_habits)

    async def process_message(self, user_id: int, message: str, replied_task_id: Optional[int] = None) -> Dict:
        active_tasks = self.db.get_tasks(user_id)
        last_completed = self.db.get_tasks(user_id, completed=True, limit=1)
        goals = self.db.get_goals(user_id)
        habits = self.db.get_habits(user_id)
        history = self.db.get_recent_messages(user_id)
        user_facts = self.db.get_user_facts(user_id)
        completion_rate = self.db.get_completion_rate(user_id, days=7)
        productivity_patterns = self.db.get_productivity_patterns(user_id)
        
        replied_task_context = ""
        if replied_task_id:
            task = self.db.get_task_by_id(replied_task_id)
            if task:
                replied_task_context = f"\n\n**IMPORTANT: The user is replying to a specific message about task ID {replied_task_id} ('{task['title']}'). Any modifications they request should apply to THIS task.**"
        
        # Generate analytics summary
        analytics_summary = ""
        if productivity_patterns:
            peak_time = productivity_patterns.get('most_productive', 'unknown')
            analytics_summary = f"**PRODUCTIVITY ANALYTICS:**\n- Completion Rate (7 days): {completion_rate:.0f}%\n- Peak Productivity Time: {peak_time}\n- Morning: {productivity_patterns.get('morning_pct', 0)}% | Afternoon: {productivity_patterns.get('afternoon_pct', 0)}% | Evening: {productivity_patterns.get('evening_pct', 0)}%\n"

        system_prompt = f"""You are an ELITE personal executive coach and AI assistant - like a combination of a top-tier executive assistant, life coach, and productivity expert. You're proactive, insightful, and deeply invested in helping your user achieve their full potential.

═══════════════════════════════════════════════════════════════
🎯 YOUR MISSION 🎯
═══════════════════════════════════════════════════════════════
You are NOT just a task manager. You are a PERFORMANCE MULTIPLIER. You help your user:
- Achieve their biggest goals by breaking them into actionable steps
- Build lasting habits that compound over time
- Make better decisions by providing insights and patterns
- Stay focused on what truly matters
- Maintain momentum and motivation through celebrations and encouragement
- Learn and improve continuously through reflection

═══════════════════════════════════════════════════════════════
🚨 ABSOLUTE RULES - THESE OVERRIDE EVERYTHING ELSE 🚨
═══════════════════════════════════════════════════════════════

1. **NEVER ASSUME TIMES. EVER.**
   - If user says "remind me tomorrow" without a time → ASK what time
   - If user says "call John later" without a time → ASK when
   - DO NOT create tasks with default times like 9am, 10am, etc.
   - ONLY create a task with a due_date if the user EXPLICITLY gave you a time

2. **BE PROACTIVE BUT SMART.**
   - Notice patterns and suggest improvements
   - Celebrate wins (completed tasks, streak milestones, goal progress)
   - Connect tasks to goals when relevant
   - Gently remind about abandoned goals or broken habits (but don't nag)
   - Suggest habit formation for recurring patterns

3. **BE INSIGHTFUL.**
   - Use productivity analytics to give personalized advice
   - Notice when user is most productive and suggest scheduling important tasks then
   - Identify tasks that align with goals and highlight those connections
   - Point out potential conflicts or overcommitment

4. **BE HONEST AND PRECISE.**
   - Never lie or make up information
   - Always check existing data before claiming something exists
   - Ask clarifying questions when ambiguous

═══════════════════════════════════════════════════════════════

**USER'S TIMEZONE:** Europe/London (currently BST, UTC+1)
**Current time:** {datetime.now(self.timezone).strftime('%A, %B %d, %Y at %I:%M %p %Z')}

**CURRENT STATE:**
{self._format_tasks(active_tasks, "ACTIVE TASKS")}
{self._format_goals(goals)}
{self._format_habits(habits)}
{self._format_tasks(last_completed, "LAST COMPLETED TASK")}
{self._format_facts(user_facts)}
{analytics_summary}{replied_task_context}

**OPERATIONAL GUIDELINES:**

**Timezone Conversion:**
- User times are in Europe/London (BST = UTC+1)
- "10am" from user → "09:00:00Z" in due_date
- "2:30pm" from user → "13:30:00Z" in due_date
- Calculate relative times correctly (tomorrow, next week, etc.)

**Task Management:**
- Update existing tasks with `update_task` - NEVER create duplicates
- Only act on explicit USER instructions, not your own past messages
- For recurring tasks use: "daily", "weekdays", "every Monday", "weekly"
- Link tasks to goals when relevant: use remember_fact with key "task_{task_id}_goal" and value "{goal_id}"

**Goal Management:**
- Help users set SMART goals (Specific, Measurable, Achievable, Relevant, Time-bound)
- Break big goals into milestone tasks
- Track progress and celebrate achievements
- Gently check in on goals that haven't been updated recently
- When tasks are completed that relate to goals, update goal progress

**Habit Formation:**
- Suggest turning recurring patterns into tracked habits
- Celebrate streak milestones (7 days, 30 days, 100 days)
- If a habit streak breaks, be encouraging and help restart
- Daily habits should have frequency="daily", weekly ones "weekly"

**Proactive Intelligence:**
- Notice if user has too many tasks and suggest prioritization
- If completion rate is dropping, ask if they're overcommitted
- Suggest scheduling important tasks during their peak productivity time
- If user mentions something repeatedly, suggest making it a habit or goal
- Celebrate wins: "That's your 5th completed task this week - great momentum!"

**Context Awareness:**
- Use conversation history intelligently
- Apply personal facts to personalize responses
- Reference productivity patterns to give better advice
- Connect the dots between tasks, habits, and goals

**Communication Style:**
- Be conversational, warm, and motivational (but not fake)
- Use occasional energy and enthusiasm when celebrating wins
- Be concise but insightful
- Ask thoughtful questions that help user reflect
- Be genuinely helpful, like a coach who cares

**EXAMPLES OF PROACTIVE INTELLIGENCE:**
✅ User completes 3rd task today → "You're on fire today! That's 3 tasks down. Want to tackle one more before your afternoon?"
✅ User creates similar tasks repeatedly → "I notice you call clients every week. Want me to track 'Call clients' as a weekly habit?"
✅ User has a goal "Launch product" and creates task "Design landing page" → "Great! This task moves you closer to launching your product."
✅ User's completion rate drops → "I noticed your completion rate dipped this week. Feeling overloaded? Let's prioritize your most important tasks."
✅ User completes task during morning (their peak time) → "Perfect timing - mornings are when you're most productive!"

**RESPONSE FORMAT (JSON ONLY):**
{{
  "reply": "Your insightful, coaching-oriented response",
  "actions": [
    {{"type": "create_task", "title": "...", "due_date": "YYYY-MM-DDTHH:MM:SSZ or null", "recurrence_rule": "optional"}},
    {{"type": "update_task", "task_id": 123, "title": "optional", "due_date": "optional", "recurrence_rule": "optional"}},
    {{"type": "delete_task", "task_id": 123}},
    {{"type": "complete_task", "task_id": 123}},
    {{"type": "create_goal", "title": "...", "description": "optional", "target_date": "YYYY-MM-DDTHH:MM:SSZ or null"}},
    {{"type": "update_goal", "goal_id": 123, "progress": 50}},
    {{"type": "complete_goal", "goal_id": 123}},
    {{"type": "create_habit", "title": "...", "frequency": "daily|weekly"}},
    {{"type": "complete_habit", "habit_id": 123}},
    {{"type": "remember_fact", "key": "category", "value": "fact"}}
  ]
}}

**CRITICAL REMINDERS:**
- NO TIME SPECIFIED = ask for it or use null due_date
- Be proactive with insights but never pushy
- Celebrate wins and progress genuinely
- Connect tasks/habits to goals when possible
- Use analytics to personalize advice"""

        contents = [{"role": "model" if msg['role'] == 'assistant' else 'user', "parts": [{"text": msg['message']}]} for msg in history]
        contents.append({"role": "user", "parts": [{"text": message}]})

        payload = {
            "contents": contents,
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "generationConfig": {"response_mime_type": "application/json", "temperature": 0.1}
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(self.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                content = json.loads(result['candidates'][0]['content']['parts'][0]['text'])
                content.setdefault('actions', [])
                
                # VALIDATION: Check for suspicious time assumptions
                for action in content.get('actions', []):
                    if action.get('type') == 'create_task':
                        due_date = action.get('due_date')
                        # Check if AI created a task with a suspiciously round time (9am, 10am, etc.)
                        # when the user message doesn't contain explicit time information
                        if due_date:
                            # Check if user's message contains time keywords
                            time_keywords = ['am', 'pm', 'o\'clock', 'at', 'morning', 'afternoon', 'evening', 'noon', 'midnight']
                            has_time_info = any(keyword in message.lower() for keyword in time_keywords)
                            # Check for specific time patterns like "3pm", "10:30", etc.
                            import re
                            has_time_pattern = bool(re.search(r'\d{1,2}(:\d{2})?\s*(am|pm|AM|PM)', message))
                            
                            if not has_time_info and not has_time_pattern:
                                # AI is trying to assume a time - flag this
                                logger.warning(f"AI attempted to assume time for task '{action.get('title')}' - blocking and requesting clarification")
                                return {
                                    "reply": "What time should I set that for?",
                                    "actions": []
                                }
                
                return content
        except Exception as e:
            logger.error(f"AI processing error: {e}\nResponse: {response.text if 'response' in locals() else 'No response'}")
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
        self.app.add_handler(CallbackQueryHandler(self.handle_button_press))
        
    async def post_init(self, application: Application):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started.")
        await self.reload_pending_reminders()
        
        self.scheduler.add_job(
            self.run_daily_briefings,
            trigger=CronTrigger(hour=8, minute=0, timezone=self.user_timezone),
            id="daily_briefing_job",
            replace_existing=True
        )
        logger.info("Scheduled daily briefing job for 8:00 AM.")

        self.scheduler.add_job(
            self.run_evening_check_ins,
            trigger=CronTrigger(hour=18, minute=0, timezone=self.user_timezone),
            id="evening_check_in_job",
            replace_existing=True
        )
        logger.info("Scheduled evening check-in job for 6:00 PM.")

        self.scheduler.add_job(
            self.run_weekly_reviews,
            trigger=CronTrigger(day_of_week='sun', hour=19, minute=0, timezone=self.user_timezone),
            id="weekly_review_job",
            replace_existing=True
        )
        logger.info("Scheduled weekly review job for Sundays at 7:00 PM.")

        await application.bot.delete_my_commands()
        logger.info("Cleared any existing bot commands.")

    async def reload_pending_reminders(self):
        logger.info("Reloading pending reminders...")
        reloaded_count = 0
        users = self.db.get_active_users()
        for user in users:
            pending_tasks = self.db.get_tasks(user['user_id'], completed=False)
            for task in pending_tasks:
                if task.get('due_date'):
                    self.schedule_reminder(task['id'], task['due_date'], task['title'], task['chat_id'], task.get('recurrence_rule'))
                    reloaded_count += 1
        logger.info(f"Reloaded {reloaded_count} pending reminders.")

    async def run_daily_briefings(self):
        logger.info("Running daily briefing job for all active users...")
        users = self.db.get_active_users()
        for user in users:
            await self.send_daily_briefing(user['user_id'], user['chat_id'])
        logger.info(f"Daily briefing job completed for {len(users)} users.")

    async def send_daily_briefing(self, user_id: int, chat_id: int):
        try:
            tasks = self.db.get_tasks_for_today(user_id)
            all_pending = self.db.get_tasks(user_id, completed=False)
            goals = self.db.get_goals(user_id)
            habits = self.db.get_habits(user_id)
            completion_rate = self.db.get_completion_rate(user_id, days=7)

            task_list_str = ""
            if tasks:
                for task in tasks:
                    time_str = task['due_date'].astimezone(self.user_timezone).strftime('%I:%M %p')
                    task_list_str += f"- {task['title']} (Due: {time_str})\n"

            context_info = f"""
Today's scheduled tasks: {len(tasks)}
Total pending tasks: {len(all_pending)}
Active goals: {len(goals)}
Active habits: {len(habits)}
Recent completion rate: {completion_rate:.0f}%
"""

            briefing_prompt = f"""You are an elite executive coach and personal assistant. Create an energizing, insightful morning briefing.

**Context:**
{context_info}

**Today's Tasks:**
{task_list_str if task_list_str else "None scheduled for today"}

**Instructions:**
- Start with a warm, personalized greeting
- Summarize today's tasks (group by time/theme if multiple)
- If completion rate is high (>70%), give encouragement
- If completion rate is low (<50%), offer gentle motivation
- If they have active goals, briefly mention focusing on goal-related work
- If no tasks today but pending tasks exist, gently suggest reviewing priorities
- Keep it concise (3-4 sentences max), energizing, and actionable
- Sound like a supportive coach, not a robot
"""
            async with httpx.AsyncClient(timeout=30.0) as client:
                payload = {
                    "contents": [{"parts": [{"text": briefing_prompt}]}],
                    "generationConfig": {"temperature": 0.4}
                }
                response = await client.post(self.ai.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                summary = result['candidates'][0]['content']['parts'][0]['text']

                await self.app.bot.send_message(chat_id, f"☀️ {summary}")

        except Exception as e:
            logger.error(f"Failed to send daily briefing to user {user_id}: {e}")

    async def run_evening_check_ins(self):
        logger.info("Running evening check-in job for all active users...")
        users = self.db.get_active_users()
        for user in users:
            if self.db.should_send_check_in(user['user_id'], 'evening', hours_between=24):
                await self.send_evening_check_in(user['user_id'], user['chat_id'])
        logger.info(f"Evening check-in job completed for {len(users)} users.")

    async def send_evening_check_in(self, user_id: int, chat_id: int):
        try:
            # Check if there are uncompleted tasks for today
            today_tasks = self.db.get_tasks_for_today(user_id)
            habits = self.db.get_habits(user_id)

            # Check which habits haven't been completed today
            now = datetime.now(self.user_timezone)
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            incomplete_habits = []

            for habit in habits:
                last_completed = habit.get('last_completed')
                if not last_completed or last_completed < today_start:
                    incomplete_habits.append(habit)

            if not today_tasks and not incomplete_habits:
                return  # Don't send check-in if nothing to do

            prompt = f"""You are a supportive personal coach. It's evening time. Create a gentle, encouraging check-in message.

**Context:**
- Uncompleted tasks from today: {len(today_tasks)}
- Incomplete habits for today: {len(incomplete_habits)}

**Instructions:**
- If there are uncompleted tasks/habits, gently ask how their day went
- Don't be pushy - just a friendly check-in
- Suggest they could still complete some habits if it's not too late
- Keep it brief (2-3 sentences) and warm
- If they accomplished things, ask about it
"""
            async with httpx.AsyncClient(timeout=30.0) as client:
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.5}
                }
                response = await client.post(self.ai.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                message = result['candidates'][0]['content']['parts'][0]['text']

                await self.app.bot.send_message(chat_id, f"🌆 {message}")
                self.db.record_check_in(user_id, 'evening')

        except Exception as e:
            logger.error(f"Failed to send evening check-in to user {user_id}: {e}")

    async def run_weekly_reviews(self):
        logger.info("Running weekly review job for all active users...")
        users = self.db.get_active_users()
        for user in users:
            await self.send_weekly_review(user['user_id'], user['chat_id'])
        logger.info(f"Weekly review job completed for {len(users)} users.")

    async def send_weekly_review(self, user_id: int, chat_id: int):
        try:
            # Get stats for the week
            completed_this_week = self.db.get_tasks(user_id, completed=True, limit=100)
            now = datetime.now(self.user_timezone)
            week_start = now - timedelta(days=7)

            weekly_completed = [t for t in completed_this_week if t.get('completed_at') and t['completed_at'] >= week_start]

            goals = self.db.get_goals(user_id)
            habits = self.db.get_habits(user_id)
            completion_rate = self.db.get_completion_rate(user_id, days=7)
            patterns = self.db.get_productivity_patterns(user_id)

            habit_summary = ""
            for habit in habits:
                streak = habit.get('current_streak', 0)
                habit_summary += f"- {habit['title']}: {streak} day streak\n"

            goal_summary = ""
            for goal in goals:
                progress = goal.get('progress', 0)
                goal_summary += f"- {goal['title']}: {progress}% complete\n"

            prompt = f"""You are an insightful executive coach. Create a thoughtful weekly review for your client.

**This Week's Stats:**
- Tasks completed: {len(weekly_completed)}
- Completion rate: {completion_rate:.0f}%
- Active goals: {len(goals)}
- Most productive time: {patterns.get('most_productive', 'N/A') if patterns else 'N/A'}

**Habits Progress:**
{habit_summary if habit_summary else "None tracked"}

**Goals Progress:**
{goal_summary if goal_summary else "None set"}

**Instructions:**
- Celebrate wins and progress (be genuine, not fake)
- Point out patterns or insights
- If completion rate is low, ask what obstacles they faced
- Suggest one concrete action for improvement next week
- Ask a reflective question about their biggest win or lesson
- Keep it conversational (4-5 sentences max)
- Sound like a supportive coach who genuinely cares
"""
            async with httpx.AsyncClient(timeout=30.0) as client:
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.5}
                }
                response = await client.post(self.ai.base_url, json=payload)
                response.raise_for_status()
                result = response.json()
                review = result['candidates'][0]['content']['parts'][0]['text']

                await self.app.bot.send_message(chat_id, f"📊 Weekly Review\n\n{review}")

        except Exception as e:
            logger.error(f"Failed to send weekly review to user {user_id}: {e}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user, chat, message = update.effective_user, update.effective_chat, update.message.text
        self.db.register_user(user.id, chat.id)
        self.db.add_message(user.id, "user", message)
        
        # Check if user is replying to a specific message
        replied_task_id = None
        if update.message.reply_to_message:
            replied_message_id = update.message.reply_to_message.message_id
            replied_task_id = self.db.get_task_from_message(replied_message_id)
            if replied_task_id:
                logger.info(f"User is replying to message about task {replied_task_id}")
        
        await context.bot.send_chat_action(chat_id=chat.id, action='typing')
        
        result = await self.ai.process_message(user.id, message, replied_task_id)
        reply = result.get('reply', 'Understood.')
        
        self.db.add_message(user.id, "assistant", reply)
        sent_message = await update.message.reply_text(reply)
        
        # Execute actions and track which task was discussed
        task_id_mentioned = None
        for action in result.get('actions', []):
            action_task_id = await self.execute_action(user.id, chat.id, action)
            if action_task_id:
                task_id_mentioned = action_task_id
        
        # Store mapping of this reply message to the task
        if task_id_mentioned:
            self.db.store_message_task_map(sent_message.message_id, task_id_mentioned)

    async def execute_action(self, user_id: int, chat_id: int, action: Dict) -> Optional[int]:
        action_type = action.get('type')
        task_id = action.get('task_id')
        try:
            if action_type == 'create_task':
                due_date = self.parse_due_date(action.get('due_date'))
                recurrence = action.get('recurrence_rule')
                new_task_id = self.db.add_task(user_id, chat_id, action['title'], due_date, recurrence)

                if due_date:
                    self.schedule_reminder(new_task_id, due_date, action['title'], chat_id, recurrence)
                    logger.info(f"Created task {new_task_id} with due date: {due_date.astimezone(self.user_timezone).strftime('%I:%M %p on %A, %b %d')}")
                else:
                    logger.info(f"Created task {new_task_id} without due date (user will specify later)")

                return new_task_id

            elif action_type == 'update_task' and task_id:
                update_data = {k: v for k, v in action.items() if k not in ['type', 'task_id']}
                if 'due_date' in update_data:
                    update_data['due_date'] = self.parse_due_date(update_data['due_date'])
                self.db.update_task(task_id, **update_data)

                task = self.db.get_task_by_id(task_id)
                if task:
                    # Reschedule with updated info
                    new_due = update_data.get('due_date', task['due_date'])
                    new_recurrence = update_data.get('recurrence_rule', task.get('recurrence_rule'))
                    if new_due:
                        self.schedule_reminder(task_id, new_due, task['title'], chat_id, new_recurrence)
                return task_id

            elif action_type == 'complete_task' and task_id:
                task = self.db.get_task_by_id(task_id)
                if task and task.get('recurrence_rule'):
                    # Recurring task - reschedule instead of completing
                    next_due = self.calculate_next_occurrence(task['due_date'], task['recurrence_rule'])
                    if next_due:
                        self.db.update_task(task_id, due_date=next_due)
                        self.schedule_reminder(task_id, next_due, task['title'], chat_id, task['recurrence_rule'])
                        logger.info(f"Rescheduled recurring task {task_id} to {next_due}")
                else:
                    self.db.complete_task(task_id)
                return task_id

            elif action_type == 'delete_task' and task_id:
                self.db.delete_task(task_id)
                return task_id

            elif action_type == 'create_goal':
                target_date = self.parse_due_date(action.get('target_date'))
                goal_id = self.db.add_goal(user_id, action['title'], action.get('description'), target_date)
                logger.info(f"Created goal {goal_id}: {action['title']}")
                return goal_id

            elif action_type == 'update_goal':
                goal_id = action.get('goal_id')
                if goal_id:
                    update_data = {k: v for k, v in action.items() if k not in ['type', 'goal_id']}
                    if 'target_date' in update_data:
                        update_data['target_date'] = self.parse_due_date(update_data['target_date'])
                    self.db.update_goal(goal_id, **update_data)
                    logger.info(f"Updated goal {goal_id}")
                    return goal_id

            elif action_type == 'complete_goal':
                goal_id = action.get('goal_id')
                if goal_id:
                    self.db.complete_goal(goal_id)
                    logger.info(f"Completed goal {goal_id}")
                    return goal_id

            elif action_type == 'create_habit':
                habit_id = self.db.add_habit(user_id, action['title'], action.get('frequency', 'daily'))
                logger.info(f"Created habit {habit_id}: {action['title']}")
                return habit_id

            elif action_type == 'complete_habit':
                habit_id = action.get('habit_id')
                if habit_id:
                    self.db.complete_habit(habit_id)
                    logger.info(f"Completed habit {habit_id}")
                    return habit_id

            elif action_type == 'remember_fact' and 'key' in action and 'value' in action:
                self.db.add_user_fact(user_id, action['key'], action['value'])
                logger.info(f"Remembered new fact for user {user_id}: {action['key']}")

        except Exception as e:
            logger.error(f"Action execution error ({action_type}): {e}\n{traceback.format_exc()}")

        return None

    def calculate_next_occurrence(self, current_due: datetime, recurrence_rule: str) -> Optional[datetime]:
        """Calculate the next occurrence based on recurrence rule"""
        if not current_due or not recurrence_rule:
            return None
            
        rule_lower = recurrence_rule.lower()
        now = datetime.now(self.user_timezone)
        
        # Ensure current_due is timezone aware
        if not current_due.tzinfo:
            current_due = current_due.replace(tzinfo=ZoneInfo('UTC'))
        current_due = current_due.astimezone(self.user_timezone)
        
        try:
            if rule_lower == "daily":
                next_due = current_due + timedelta(days=1)
                # If that's in the past, advance to tomorrow from now
                while next_due <= now:
                    next_due += timedelta(days=1)
                return next_due
                
            elif rule_lower == "weekdays":
                next_due = current_due + timedelta(days=1)
                while next_due.weekday() >= 5 or next_due <= now:  # Skip weekends
                    next_due += timedelta(days=1)
                return next_due
                
            elif "every monday" in rule_lower:
                days_ahead = (0 - current_due.weekday()) % 7
                if days_ahead == 0:
                    days_ahead = 7
                next_due = current_due + timedelta(days=days_ahead)
                while next_due <= now:
                    next_due += timedelta(days=7)
                return next_due
                
            elif rule_lower in ["weekly", "every week"]:
                next_due = current_due + timedelta(days=7)
                while next_due <= now:
                    next_due += timedelta(days=7)
                return next_due
                
            # Add more patterns as needed
            
        except Exception as e:
            logger.error(f"Error calculating next occurrence: {e}")
        
        return None

    def parse_due_date(self, due_str: Optional[str]) -> Optional[datetime]:
        if not due_str: return None
        try: 
            # Parse as UTC datetime
            dt = datetime.fromisoformat(due_str.replace('Z', '+00:00'))
            logger.info(f"Parsed due_date '{due_str}' as {dt.astimezone(self.user_timezone).strftime('%Y-%m-%d %I:%M %p %Z')}")
            return dt
        except (ValueError, TypeError): 
            logger.warning(f"Could not parse due_date string: {due_str}")
            return None
    
    def schedule_reminder(self, task_id: int, due_date: datetime, title: str, chat_id: int, recurrence_rule: Optional[str] = None):
        if not due_date.tzinfo:
            due_date = due_date.replace(tzinfo=ZoneInfo('UTC'))

        aware_due = due_date.astimezone(self.user_timezone)
        now_aware = datetime.now(self.user_timezone)

        if aware_due > now_aware:
            job_id = f"task_{task_id}"
            self.scheduler.add_job(
                self.send_task_reminder, 
                DateTrigger(run_date=aware_due),
                args=[chat_id, title, task_id, recurrence_rule], 
                id=job_id, 
                replace_existing=True
            )
            recurrence_note = f" (Recurring: {recurrence_rule})" if recurrence_rule else ""
            logger.info(f"Scheduled reminder for task {task_id} ('{title}') at {aware_due.strftime('%Y-%m-%d %I:%M %p %Z')}{recurrence_note}")
        else:
            logger.warning(f"Did not schedule reminder for task {task_id} as its due date {aware_due} is in the past.")

    async def send_task_reminder(self, chat_id: int, title: str, task_id: int, recurrence_rule: Optional[str] = None):
        try:
            keyboard = [
                [
                    InlineKeyboardButton("Done ✅", callback_data=f"complete:{task_id}"),
                    InlineKeyboardButton("Snooze 5min", callback_data=f"snooze:{task_id}"),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            recurrence_emoji = "🔄 " if recurrence_rule else ""
            message_text = f"{recurrence_emoji}⏰ Reminder: {title}"
            sent_message = await self.app.bot.send_message(
                chat_id=chat_id, text=message_text, reply_markup=reply_markup
            )
            # Store mapping for replies
            self.db.store_message_task_map(sent_message.message_id, task_id)
            logger.info(f"Successfully sent reminder for task {task_id} to chat {chat_id}.")
        except Exception as e:
            logger.error(f"Failed to send reminder for task {task_id}: {e}")

    async def handle_button_press(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        action, task_id_str = query.data.split(":")
        task_id = int(task_id_str)

        task = self.db.get_task_by_id(task_id)
        if not task:
            await query.edit_message_text(text=f"This task no longer exists.")
            return

        if action == "complete":
            if task.get('recurrence_rule'):
                # Recurring task - reschedule
                next_due = self.calculate_next_occurrence(task['due_date'], task['recurrence_rule'])
                if next_due:
                    self.db.update_task(task_id, due_date=next_due)
                    self.schedule_reminder(task_id, next_due, task['title'], task['chat_id'], task['recurrence_rule'])
                    next_time = next_due.strftime('%I:%M %p on %A, %b %d')
                    await query.edit_message_text(text=f"✅ Done! Rescheduled for {next_time}: {task['title']}")
                    logger.info(f"Completed and rescheduled recurring task {task_id}")
            else:
                self.db.complete_task(task_id)
                await query.edit_message_text(text=f"✅ Done: {task['title']}")
                logger.info(f"Completed task {task_id} via button press.")
        
        elif action == "snooze":
            new_due_date = datetime.now(self.user_timezone) + timedelta(minutes=5)
            self.db.update_task(task_id, due_date=new_due_date)
            self.schedule_reminder(task_id, new_due_date, task['title'], task['chat_id'], task.get('recurrence_rule'))
            await query.edit_message_text(text=f"Snoozed for 5 minutes: {task['title']}")
            logger.info(f"Snoozed task {task_id} for 5 minutes via button press.")

    async def handle_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Ignoring reaction, as buttons are the primary method for task completion.")
        return

    def run(self):
        self.app.post_init = self.post_init
        logger.info(f"Starting Personal Assistant Bot at {datetime.now(self.user_timezone)}")
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    secrets = {key: os.environ.get(key) for key in ["TELEGRAM_BOT_TOKEN", "GEMINI_API_KEY", "DATABASE_URL"]}
    if not all(secrets.values()):
        missing = [key for key, value in secrets.items() if not value]
        logger.error(f"Missing environment variables! Required: {', '.join(missing)}")
        exit(1)
    
    bot = PersonalAssistantBot(secrets["TELEGRAM_BOT_TOKEN"], secrets["GEMINI_API_KEY"])
    
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot execution stopped manually.")
    finally:
        if bot.scheduler and bot.scheduler.running:
            bot.scheduler.shutdown()
            logger.info("Scheduler shut down.")

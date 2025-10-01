import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
import sqlite3
import asyncio

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import httpx

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=ває.INFO
)
logger = logging.getLogger(__name__)


class ReminderDatabase:
    """Database for storing reminders"""
    
    def __init__(self, db_path="reminders.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                reminder_type TEXT NOT NULL,
                schedule_info TEXT NOT NULL,
                job_id TEXT UNIQUE,
                created_at TEXT NOT NULL,
                is_active INTEGER DEFAULT 1
            )
        ''')
        conn.commit()
        conn.close()
    
    def add_reminder(self, user_id: int, chat_id: int, message: str, 
                     reminder_type: str, schedule_info: dict, job_id: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO reminders 
            (user_id, chat_id, message, reminder_type, schedule_info, job_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            chat_id,
            message,
            reminder_type,
            json.dumps(schedule_info),
            job_id,
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
    
    def get_active_reminders(self, user_id: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, message, reminder_type, schedule_info, created_at
            FROM reminders
            WHERE user_id = ? AND is_active = 1
            ORDER BY created_at DESC
        ''', (user_id,))
        results = cursor.fetchall()
        conn.close()
        return results
    
    def deactivate_reminder(self, job_id: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE reminders SET is_active = 0 WHERE job_id = ?', (job_id,))
        conn.commit()
        conn.close()


class GroqParser:
    """Uses Groq AI to parse natural language reminders"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
    
    async def parse_reminder(self, text: str) -> Optional[dict]:
        """Use Groq to parse reminder text into structured format"""
        
        system_prompt = """You are a reminder parsing assistant. Extract reminder information from natural language and return ONLY a valid JSON object.

Current date and time: {current_time}

Parse the user's message and return a JSON object with these fields:
- "message": The thing to remind about (string)
- "type": One of: "one_time", "daily", "weekly", "custom_interval", "specific_days"
- "start_datetime": ISO 8601 datetime when reminders should start (string)
- "end_datetime": ISO 8601 datetime when reminders should end, or null (string or null)
- "days_of_week": For weekly reminders, array of day numbers [0=Monday, 6=Sunday], or null
- "interval_days": For custom intervals (like "every 3 days"), number of days, or null
- "time_of_day": Time in HH:MM format (24-hour), or null for one-time reminders

Examples:

Input: "remind me to call Steve in 30 minutes"
Output: {{"message": "call Steve", "type": "one_time", "start_datetime": "2025-10-01T19:00:00", "end_datetime": null, "days_of_week": null, "interval_days": null, "time_of_day": null}}

Input: "every day for 2 weeks remind me to exercise"
Output: {{"message": "exercise", "type": "daily", "start_datetime": "2025-10-02T09:00:00", "end_datetime": "2025-10-16T09:00:00", "days_of_week": null, "interval_days": null, "time_of_day": "09:00"}}

Input: "every Thursday at 3pm remind me about team meeting"
Output: {{"message": "team meeting", "type": "weekly", "start_datetime": "2025-10-03T15:00:00", "end_datetime": null, "days_of_week": [3], "interval_days": null, "time_of_day": "15:00"}}

Input: "every 3 days remind me to water plants"
Output: {{"message": "water plants", "type": "custom_interval", "start_datetime": "2025-10-01T09:00:00", "end_datetime": null, "days_of_week": null, "interval_days": 3, "time_of_day": "09:00"}}

Input: "starting next Monday, remind me daily to take vitamins"
Output: {{"message": "take vitamins", "type": "daily", "start_datetime": "2025-10-07T09:00:00", "end_datetime": null, "days_of_week": null, "interval_days": null, "time_of_day": "09:00"}}

Return ONLY the JSON object, no other text.""".format(
            current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.base_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "llama-3.1-70b-versatile",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": text}
                        ],
                        "temperature": 0.1,
                        "max_tokens": 500
                    }
                )
                
                if response.status_code != 200:
                    logger.error(f"Groq API error: {response.status_code} - {response.text}")
                    return None
                
                result = response.json()
                content = result['choices'][0]['message']['content'].strip()
                
                # Remove markdown code blocks if present
                if content.startswith('```'):
                    content = content.split('```')[1]
                    if content.startswith('json'):
                        content = content[4:]
                    content = content.strip()
                
                parsed = json.loads(content)
                return parsed
                
        except Exception as e:
            logger.error(f"Error calling Groq API: {e}")
            return None


class TelegramReminderBot:
    """Main bot class"""
    
    def __init__(self, telegram_token: str, groq_api_key: str):
        self.telegram_token = telegram_token
        self.app = Application.builder().token(telegram_token).build()
        self.scheduler = AsyncIOScheduler()
        self.db = ReminderDatabase()
        self.parser = GroqParser(groq_api_key)
        
        # Add handlers
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("list", self.list_reminders))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 Hi! I'm your AI-powered reminder bot!\n\n"
            "Just tell me what you want to be reminded about in plain English!\n\n"
            "Examples:\n"
            "• Remind me to call Steve in 30 mins\n"
            "• Every day for the next 2 weeks remind me to exercise\n"
            "• Every Thursday remind me about team meeting\n"
            "• Starting next Monday, remind me daily to take vitamins\n"
            "• Remind me every 3 days to water plants\n\n"
            "I use AI to understand your natural language, so feel free to phrase it however you want!\n\n"
            "Use /list to see your active reminders."
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🔔 **Reminder Bot Help**\n\n"
            "I understand natural language! Just tell me what you want and when.\n\n"
            "**Examples:**\n"
            "• In 30 minutes, remind me to call John\n"
            "• Every day at 9am for a week, remind me to exercise\n"
            "• Every Thursday at 3pm, team meeting\n"
            "• Every other day, water the plants\n"
            "• Starting next Monday, daily reminder to take vitamins\n"
            "• Remind me about the dentist appointment next Friday at 2pm\n\n"
            "**Commands:**\n"
            "/list - See your active reminders\n"
            "/help - Show this help message\n\n"
            "Just type naturally - I'll figure it out! 🤖",
            parse_mode='Markdown'
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        text = update.message.text
        
        # Show typing indicator
        await update.message.chat.send_action("typing")
        
        # Parse with Groq AI
        parsed = await self.parser.parse_reminder(text)
        
        if not parsed or 'message' not in parsed:
            await update.message.reply_text(
                "❌ I couldn't understand that reminder. Could you try rephrasing?\n\n"
                "Examples:\n"
                "• 'Remind me to call John in 2 hours'\n"
                "• 'Every day for a week remind me to exercise'\n"
                "• 'Every Thursday at 3pm, team meeting'"
            )
            return
        
        # Schedule the reminder
        job_id = f"reminder_{user_id}_{datetime.now().timestamp()}"
        
        try:
            reminder_message = parsed['message']
            reminder_type = parsed['type']
            start_dt = datetime.fromisoformat(parsed['start_datetime'])
            end_dt = datetime.fromisoformat(parsed['end_datetime']) if parsed.get('end_datetime') else None
            
            if reminder_type == "one_time":
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=DateTrigger(run_date=start_dt),
                    args=[chat_id, reminder_message, job_id],
                    id=job_id
                )
                await update.message.reply_text(
                    f"✅ Got it! I'll remind you on {start_dt.strftime('%B %d at %I:%M %p')}"
                )
            
            elif reminder_type == "daily":
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=CronTrigger(
                        hour=start_dt.hour,
                        minute=start_dt.minute,
                        start_date=start_dt,
                        end_date=end_dt
                    ),
                    args=[chat_id, reminder_message, job_id],
                    id=job_id
                )
                if end_dt:
                    await update.message.reply_text(
                        f"✅ Daily reminder set from {start_dt.strftime('%b %d')} "
                        f"to {end_dt.strftime('%b %d')} at {start_dt.strftime('%I:%M %p')}"
                    )
                else:
                    await update.message.reply_text(
                        f"✅ Daily reminder set starting {start_dt.strftime('%b %d at %I:%M %p')}"
                    )
            
            elif reminder_type == "weekly":
                days_of_week = parsed.get('days_of_week', [])
                days_str = ",".join(str(d) for d in days_of_week)
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=CronTrigger(
                        day_of_week=days_str,
                        hour=start_dt.hour,
                        minute=start_dt.minute,
                        start_date=start_dt,
                        end_date=end_dt
                    ),
                    args=[chat_id, reminder_message, job_id],
                    id=job_id
                )
                day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                days_list = [day_names[d] for d in days_of_week]
                await update.message.reply_text(
                    f"✅ Weekly reminder set for {', '.join(days_list)} at {start_dt.strftime('%I:%M %p')}"
                )
            
            elif reminder_type == "custom_interval":
                interval_days = parsed.get('interval_days', 1)
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=CronTrigger(
                        day=f'*/{interval_days}',
                        hour=start_dt.hour,
                        minute=start_dt.minute,
                        start_date=start_dt,
                        end_date=end_dt
                    ),
                    args=[chat_id, reminder_message, job_id],
                    id=job_id
                )
                await update.message.reply_text(
                    f"✅ Reminder set every {interval_days} days starting {start_dt.strftime('%b %d')}"
                )
            
            elif reminder_type == "specific_days":
                days_of_week = parsed.get('days_of_week', [])
                days_str = ",".join(str(d) for d in days_of_week)
                self.scheduler.add_job(
                    self.send_reminder,
                    trigger=CronTrigger(
                        day_of_week=days_str,
                        hour=start_dt.hour,
                        minute=start_dt.minute,
                        start_date=start_dt,
                        end_date=end_dt
                    ),
                    args=[chat_id, reminder_message, job_id],
                    id=job_id
                )
                day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                days_list = [day_names[d] for d in days_of_week]
                await update.message.reply_text(
                    f"✅ Reminder set for {', '.join(days_list)} at {start_dt.strftime('%I:%M %p')}"
                )
            
            # Save to database
            self.db.add_reminder(user_id, chat_id, reminder_message, reminder_type, parsed, job_id)
            
        except Exception as e:
            logger.error(f"Error scheduling reminder: {e}")
            await update.message.reply_text(
                "❌ Sorry, there was an error setting up your reminder. Please try again."
            )
    
    async def send_reminder(self, chat_id: int, message: str, job_id: str):
        """Send the actual reminder message"""
        try:
            await self.app.bot.send_message(
                chat_id=chat_id,
                text=f"🔔 **REMINDER**\n\n{message}",
                parse_mode='Markdown'
            )
            # Deactivate one-time reminders
            self.db.deactivate_reminder(job_id)
        except Exception as e:
            logger.error(f"Error sending reminder: {e}")
    
    async def list_reminders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        reminders = self.db.get_active_reminders(user_id)
        
        if not reminders:
            await update.message.reply_text("You have no active reminders.")
            return
        
        message = "📋 **Your Active Reminders:**\n\n"
        for reminder in reminders[:10]:  # Show max 10
            reminder_id, msg, r_type, schedule_info, created = reminder
            message += f"• {msg}\n  Type: {r_type.replace('_', ' ').title()}\n\n"
        
        if len(reminders) > 10:
            message += f"... and {len(reminders) - 10} more"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    def run(self):
        """Start the bot"""
        self.scheduler.start()
        logger.info("Bot is starting...")
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    # Get tokens from environment variables
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    
    if not TELEGRAM_TOKEN or not GROQ_API_KEY:
        logger.error("Missing required environment variables!")
        logger.error("Please set TELEGRAM_BOT_TOKEN and GROQ_API_KEY")
        exit(1)
    
    bot = TelegramReminderBot(TELEGRAM_TOKEN, GROQ_API_KEY)
    bot.run()

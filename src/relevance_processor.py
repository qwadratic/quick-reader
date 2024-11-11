import asyncio
import csv
import io
from datetime import datetime, timedelta, timezone
from time import sleep
from anthropic import Anthropic
from config import read_config
from db_schema import Message, get_session
from pyrogram import Client
from pyrogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

config = read_config()
ANTHROPIC_API_KEY = config['anthropic']['api_key']
ANTHROPIC_MODEL = config['anthropic']['model']
TELEGRAM_BOT_TOKEN = config['telegram']['bot_token']
TELEGRAM_API_ID = config['telegram']['api_id']
TELEGRAM_API_HASH = config['telegram']['api_hash']
TELEGRAM_ADMIN_ID = config['telegram']['admin_id']

def build_prompt(messages):
    PROMPT = [
"""
You will be categorizing a message history. The goal is to identify messages that match one or more topics provided.

<topics>
""",
f"""
{"\n".join(["- "+topic for topic in config['topics']])}
""",
f"""
</topics>

<message_history>
Author,Timestamp,Content,Source
{"\n".join([
    f"{message.author},{message.timestamp},{message.content},{message.channel_name}"
    for message in messages
])}
</message_history>

Please follow these steps:

1. Carefully read through the entire message history.

2. Analyze the content and identify if a message matches one or more topics from the list provided.

3. The output should be CSV keeping only rows where the message matches one or more topics from the list provided.

4. The output CSV should contain additional column with topics that were matched. The column name should be "Topics".

Don't include anything besides the resulting CSV in your response
Remember to consider the context, tone, and content of the messages when identifying if the message matches the topic. 
"""]
    return [{"role": "user", "content": "\n".join(PROMPT)}]


async def process_messages():
    anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)
    session = get_session()
    bot = Client(
        name="relevance_processor", 
        api_id=TELEGRAM_API_ID, 
        api_hash=TELEGRAM_API_HASH, 
        bot_token=TELEGRAM_BOT_TOKEN)
    
    # Get unprocessed messages
    messages = session.query(Message) \
        .filter_by(processed=False) \
        .order_by(Message.timestamp.asc()) \
        .limit(50) \
        .all()

    try:
        # Use Claude to determine relevance
        response = anthropic.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=8192,
            messages=build_prompt(messages)
        )
        reader = csv.DictReader(io.StringIO(response.content[0].text))
        for row in reader:
            notification_text = f"""
Relevant message found!
Author: <code>{row['Author']}</code>
Timestamp: <code>{datetime.fromtimestamp(int(row['Timestamp']), tz=timezone.utc).astimezone(timezone(timedelta(hours=2))).strftime('%Y-%m-%d %H:%M:%S')}</code>
Content:
<tg-spoiler>
{row['Content']}
</tg-spoiler>

Topics: {row['Topics']}
Source: {row['Source']}
"""
            async with bot:
                await bot.send_message(
                    chat_id=TELEGRAM_ADMIN_ID,
                    text=notification_text,
                    parse_mode=ParseMode.HTML)
                sleep(1)
        # session.commit()
        
    except Exception as e:
        session.rollback()
        raise e

    session.close()

if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        process_messages, 'interval', hours=4, 
        next_run_time=datetime.now() + timedelta(seconds=1))
    scheduler.start()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
   


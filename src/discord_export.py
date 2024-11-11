import subprocess
import csv
import os
from datetime import datetime, timedelta, timezone
from config import read_config
from db_schema import Message, get_session, init_db
from apscheduler.schedulers.blocking import BlockingScheduler


def timestamp_to_str(timestamp):
    return datetime.fromtimestamp(
        timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def get_latest_timestamp(session):
    latest_message = session.query(Message).order_by(
        Message.timestamp.desc()
    ).first()
    latest_timestamp = 0
    if latest_message:
        latest_timestamp = latest_message.timestamp
    return latest_timestamp

def export_channels_w_threads(token, guild_id):
    cmd = [
        'docker',
        'run',
        '--rm',
        'tyrrrz/discordchatexporter:stable',
        'channels',
        '-g', str(guild_id),
        '-t', token,
        '--include-threads', 'All'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout

def export_discord_chat(token, channel_id, date_from, date_to):
    cmd = [
        'docker',
        'run',
        '--rm',
        '-v', './data:/out',
        'tyrrrz/discordchatexporter:stable',
        'export',
        '-c', str(channel_id),
        '-t', token,
        '-f', 'Csv',
        '-o', '%g:%c:%a:%b.csv',
        '--after', date_from,
        '--before', date_to
    ]
    
    subprocess.run(cmd)

def parse_developer_forum_threads(discord_data, forum_id):
    thread_ids = []
    in_dev_forum = False
    
    lines = discord_data.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Check if we've entered the developer forum section
        if str(forum_id) in line:
            in_dev_forum = True
            continue
            
        # Check if we've left the developer forum section (next main section)
        if in_dev_forum and line and not line.startswith("*"):
            break
            
        # Extract thread IDs while in developer forum section
        if in_dev_forum and line.startswith("*"):
            # Split by | and get the first part (ID)
            thread_id = line.split("|")[0].strip("*").strip()
            thread_ids.append(int(thread_id))
    
    return thread_ids

def import_to_db(session, latest_timestamp, data_folder='data'):
    for filename in os.listdir(data_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_folder, filename)
            parts = filename.split(':')
            if len(parts) == 4:
                _, c, _, _ = parts
                with open(file_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        date_str = row['Date']
                        date_str = date_str[:26] + date_str[-6:]
                        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                        timestamp = int(date_obj.timestamp())

                        if timestamp > latest_timestamp:
                            message = Message(
                                content=row['Content'],
                                author=row['Author'],
                                channel_id=c,
                                channel_name=CHANNELS[int(c)],
                                timestamp=timestamp,
                                processed=False
                            )
                            session.add(message)
                    
                session.commit()
                os.remove(file_path)
    
    session.close()

def export_discord_chats():
    session = get_session() 
    latest_timestamp = get_latest_timestamp(session)
    
    date_from = "2024-11-01 00:00:00"
    if latest_timestamp:
        date_from = timestamp_to_str(latest_timestamp)

    latest_date_obj = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
    latest_date_obj += timedelta(days=1)
    date_to = latest_date_obj.strftime("%Y-%m-%d %H:%M:%S")

    for channel in CHANNELS.keys():
        print(f"Exporting channel {channel} from {date_from} to {date_to}")
        export_discord_chat(DISCORD_TOKEN, channel, date_from, date_to)

    print("Importing to db")
    import_to_db(session, latest_timestamp)
    print('Done')


config = read_config()
DISCORD_TOKEN = config['discord']['token']
CHANNELS = {c: n for c, n in config['discord']['channels'].items()}
for g, d in config['discord']['forums'].items():
    for c, n in d.items():
        channels_output = export_channels_w_threads(DISCORD_TOKEN, int(g))
        thread_ids = parse_developer_forum_threads(channels_output, int(c))
        for cc in thread_ids:
            CHANNELS[cc] = n

if __name__ == '__main__':
    init_db()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        export_discord_chats, 'interval', hours=1, 
        next_run_time=datetime.now() + timedelta(seconds=1))
    scheduler.start()

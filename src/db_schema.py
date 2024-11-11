from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from config import read_config

Base = declarative_base()

class Message(Base):
    __tablename__ = 'discord_messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(String)
    channel_name = Column(String)
    content = Column(Text)
    author = Column(String)
    timestamp = Column(Integer)
    processed = Column(Boolean, default=False)
    is_relevant = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    config = read_config()
    engine = create_engine(f'sqlite:///{config['database']['path']}')
    Base.metadata.create_all(engine)

def get_session():
    config = read_config()
    engine = create_engine(f'sqlite:///{config['database']['path']}')
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
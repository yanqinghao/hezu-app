from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, func

# 创建一个基类
Base = declarative_base()


# 定义模型
class HezuRecords(Base):
    __tablename__ = 'hezu_records'
    id = Column(Integer, primary_key=True)
    message = Column(String)
    channel_message_id = Column(String)
    group_message_id = Column(String)
    price = Column(String)
    owner_username = Column(String)
    owner_id = Column(String)
    sender_username = Column(String)
    sender_id = Column(String)
    service_name = Column(String)
    user_num = Column(Integer)
    expiration_date = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

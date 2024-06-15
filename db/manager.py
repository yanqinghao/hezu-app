from env import environment
from db.table import Base, HezuRecords
from urllib.parse import quote_plus
from sqlalchemy import create_engine, func, cast, Integer
from sqlalchemy.orm import sessionmaker


class DBManager:
    def __init__(self, host, port, user, password, db) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.connection = None
        password = quote_plus(self.password)
        database_url = f'postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.db}'  # noqa
        self.engine = create_engine(database_url)
        self.session = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        Base.metadata.create_all(bind=self.engine)

    def add_record(self, message_info: dict):
        session = self.session()
        try:
            db_record = HezuRecords(**message_info)
            session.add(db_record)
            session.commit()
            session.refresh(db_record)
            return db_record
        finally:
            session.close()

    def get_user_info(self, user_id: int):
        session = self.session()
        try:
            usernames = (
                session.query(HezuRecords.owner_username)
                .filter(HezuRecords.owner_id == user_id)
                .distinct()
                .all()
            )
            return [username[0] for username in usernames]
        finally:
            session.close()

    def count_non_null_channel_message_id(self, user_id):
        session = self.session()
        try:
            # 获取 owner_id 或 sender_id 等于 user_id 的所有记录
            count = (
                session.query(func.count(HezuRecords.id))
                .filter(
                    (HezuRecords.owner_id == user_id)
                    | (HezuRecords.sender_id == user_id),
                    HezuRecords.channel_message_id.isnot(None),
                )
                .scalar()
            )
            return count
        finally:
            session.close()

    def count_non_null_group_message_id(self, user_id):
        session = self.session()
        try:
            # 获取 owner_id 或 sender_id 等于 user_id 的所有记录
            count = (
                session.query(func.count(HezuRecords.id))
                .filter(
                    (HezuRecords.owner_id == user_id)
                    | (HezuRecords.sender_id == user_id),
                    HezuRecords.group_message_id.isnot(None),
                )
                .scalar()
            )
            return count
        finally:
            session.close()

    def add_records(self, records: list):
        session = self.session()
        try:
            db_records = [HezuRecords(**record) for record in records]
            session.add_all(db_records)
            session.commit()
            # 返回插入的记录
            return db_records
        finally:
            session.close()

    def get_latest_channel_message_id(self):
        session = self.session()
        try:
            # 获取最大 message_id
            max_message_id = session.query(
                func.max(cast(HezuRecords.channel_message_id, Integer))
            ).scalar()
            return max_message_id
        finally:
            session.close()

    def get_latest_group_message_id(self):
        session = self.session()
        try:
            # 获取最大 message_id
            max_message_id = session.query(
                func.max(cast(HezuRecords.group_message_id, Integer))
            ).scalar()
            return max_message_id
        finally:
            session.close()

    def close(self):
        self.engine.dispose()


db_manager = DBManager(
    environment.db_host,
    environment.db_port,
    environment.db_user,
    environment.db_password,
    environment.db_name,
)

from env import environment
from db.table import Base, HezuRecords
from urllib.parse import quote_plus
from sqlalchemy import create_engine, func, cast, Integer, case
from sqlalchemy.orm import sessionmaker, aliased


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
            # 获取所有匹配的用户名
            usernames = self.get_usernames_by_user_id(user_id)

            # 查找记录数量
            records = (
                session.query(HezuRecords)
                .filter(
                    (HezuRecords.owner_id == user_id)
                    | (HezuRecords.sender_id == user_id)
                    | (HezuRecords.sender_username.in_(usernames))
                    | (HezuRecords.owner_username.in_(usernames)),
                    HezuRecords.channel_message_id.isnot(None),
                )
                .all()
            )

            # 在内存中处理过滤逻辑
            count = len(records)

            return count
        finally:
            session.close()

    def count_non_null_channel_message_id_by_name(self, user_name):
        session = self.session()
        try:

            # 查找记录数量
            records = (
                session.query(HezuRecords)
                .filter(
                    (HezuRecords.sender_username == user_name)
                    | (HezuRecords.owner_username == user_name),
                    HezuRecords.channel_message_id.isnot(None),
                )
                .all()
            )

            # 在内存中处理过滤逻辑
            count = len(records)

            return count
        finally:
            session.close()

    def get_user_id_by_username(self, username):
        session = self.session()
        try:
            # 查找 sender_username 匹配的 user_id
            sender_id = (
                session.query(HezuRecords.sender_id)
                .filter(
                    HezuRecords.sender_username == username,
                    HezuRecords.sender_id.isnot(None),
                )
                .first()
            )

            if sender_id:
                return sender_id[0]

            # 查找 owner_username 匹配的 user_id
            owner_id = (
                session.query(HezuRecords.owner_id)
                .filter(
                    HezuRecords.owner_username == username,
                    HezuRecords.owner_id.isnot(None),
                )
                .first()
            )

            if owner_id:
                return owner_id[0]

            return None
        finally:
            session.close()

    def get_usernames_by_user_id(self, user_id):
        session = self.session()
        try:
            # 创建别名用于用户查询
            owner_alias = aliased(HezuRecords)
            sender_alias = aliased(HezuRecords)

            # 查找 user_id 对应的所有用户名
            usernames = (
                session.query(
                    case(
                        (
                            owner_alias.owner_username.isnot(None),
                            owner_alias.owner_username,
                        ),
                        else_=sender_alias.sender_username,
                    ).label('username')
                )
                .filter(
                    (owner_alias.owner_id == user_id)
                    | (sender_alias.sender_id == user_id)
                )
                .distinct()
                .all()
            )
            return [username[0] for username in usernames if username]
        finally:
            session.close()

    def count_non_null_group_message_id_by_name(self, user_name):
        session = self.session()
        try:

            # 查找记录数量
            records = (
                session.query(HezuRecords)
                .filter(
                    (HezuRecords.sender_username == user_name)
                    | (HezuRecords.owner_username == user_name),
                    HezuRecords.group_message_id.isnot(None),
                )
                .all()
            )

            # 在内存中处理过滤逻辑
            count = len(records)

            return count
        finally:
            session.close()

    def count_non_null_group_message_id(self, user_id):
        session = self.session()
        try:
            # 获取所有匹配的用户名
            usernames = self.get_usernames_by_user_id(user_id)

            # 查找记录数量
            records = (
                session.query(HezuRecords)
                .filter(
                    (HezuRecords.owner_id == user_id)
                    | (HezuRecords.sender_id == user_id)
                    | (HezuRecords.sender_username.in_(usernames))
                    | (HezuRecords.owner_username.in_(usernames)),
                    HezuRecords.group_message_id.isnot(None),
                )
                .all()
            )

            # 在内存中处理过滤逻辑
            count = len(records)

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

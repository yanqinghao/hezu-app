import os
from dotenv import load_dotenv

load_dotenv('private/env')


class Environment(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self):
        self.api_id = os.environ.get('API_ID')
        self.api_hash = os.environ.get('API_HASH')
        self.session = os.environ.get('SESSION')
        self.hezu_group_chatid = os.environ.get('HEZU_GROUP_CHATID')
        self.hezu_channel_chatid = os.environ.get('HEZU_CHANNEL_CHATID')
        self.hezu_summary_chatid = os.environ.get('HEZU_SUMMARY_CHATID')
        self.db_type = os.environ.get('DB_TYPE')
        self.db_host = os.environ.get('DB_HOST')
        self.db_port = os.environ.get('DB_PORT')
        self.db_user = os.environ.get('DB_USER')
        self.db_password = os.environ.get('DB_PASSWORD')
        self.db_name = os.environ.get('DB_NAME')


environment = Environment()

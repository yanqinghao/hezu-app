import asyncio
import traceback
from telethon import TelegramClient
from telethon.errors import FloodError


from log import logger
from env import environment
from db.manager import db_manager

CHANNEL = 0
GROUP = 1


class BaseHandler:
    def __init__(self, client: TelegramClient) -> None:
        self.client = client

    async def send_message_to_a_channel(self, message, retry=3):
        try:
            await self.client.send_message(
                int(environment.hezu_summary_chatid), message
            )
        except FloodError:
            logger.error('Get a FloodError in sending message to a channel')
            logger.info('Waiting for 300 seconds, and retry...')
            retry -= 1
            await asyncio.sleep(300)
            await self.send_message_to_a_channel(message, retry)
        except Exception as e:
            logger.error(
                f'''Error in send message to a channel: {e}\n
                {traceback.format_exc()}'''
            )
            logger.error('Failed in sending message and stop sending.')

    async def parse_message(self, message, channel_or_group=CHANNEL):
        try:
            if '@' in message.text:
                owner_username = message.text.split('@')[-1].strip()
            else:
                owner_username = None
        except Exception as e:
            owner_username = None
            logger.error(
                f'Error parsing owner_username: {e}\n{traceback.format_exc()}'
            )
        try:
            if owner_username:
                owner_id = await self.client.get_peer_id(owner_username)
            else:
                owner_id = None
        except Exception as e:
            owner_id = None
            logger.error(
                f'Error getting owner_id: {e}\n{traceback.format_exc()}'
            )
        try:
            service_name = (
                message.text.replace('#非审核车', '')
                .strip()
                .split(' ')[0]
                .strip('#')
                .strip()
            )
        except Exception as e:
            service_name = None
            logger.error(
                f'Error parsing service_name: {e}\n{traceback.format_exc()}'
            )
        if message.sender is None:
            sender_id = message.from_id.user_id
            try:
                user = await self.client.get_entity(sender_id)
                sender_username = user.username
            except Exception as e:
                sender_username = None
                logger.error(
                    f'''Error getting sender_username: {e}\n
                     {traceback.format_exc()}'''
                )
        else:
            sender_id = message.sender.id
            try:
                user = await self.client.get_entity(sender_id)
                sender_username = user.username
            except Exception as e:
                sender_username = None
                logger.error(
                    f'''Error getting sender_username: {e}\n
                    {traceback.format_exc()}'''
                )
        message_info = (
            {
                'message': message.text,
                'channel_message_id': message.id,
                # "price": message.text,
                'owner_username': owner_username,
                'owner_id': owner_id,
                'sender_username': sender_username,
                'sender_id': sender_id,
                'service_name': service_name,
                'send_at': message.date,
                # "user_num": 1,
                # "expiration_date": message.text
            }
            if channel_or_group == CHANNEL
            else {
                'message': message.text,
                'group_message_id': message.id,
                # "price": message.text,
                'owner_username': owner_username,
                'owner_id': owner_id,
                'sender_username': sender_username,
                'sender_id': sender_id,
                'service_name': service_name,
                'send_at': message.date,
                # "user_num": 1,
                # "expiration_date": message.text
            }
        )
        return message_info


class MessageHandler(BaseHandler):
    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)

    def count_used_username(self):
        pass

    def count_non_audit_times(self):
        pass

    def count_audit_times(self):
        pass

    def generate_notice(self):
        pass

    def send_to_channel(self):
        pass

    async def run(self, message, channel_or_group):
        try:
            logger.info(f'receive message for channel: {message.text}')
            await self.client.get_dialogs()
            if (
                (channel_or_group == CHANNEL)
                and (
                    message.text[0] == '#'
                    and not message.text.startswith('#恰饭广告')
                )
            ) or (
                (channel_or_group == GROUP)
                and (message.text.startswith('#非审核车'))
            ):
                parsed_message = await self.parse_message(
                    message, channel_or_group
                )
                logger.debug(f'Parse Message: {parsed_message}')
                db_manager.add_record(parsed_message)
                user_id = parsed_message['owner_id']
                user_name = parsed_message['owner_username']
                if user_id is None and user_name is not None:
                    user_id = db_manager.get_user_id_by_username(user_name)
                if user_id:
                    try:
                        usernames = db_manager.get_usernames_by_user_id(
                            str(user_id)
                        )
                    except Exception as e:
                        usernames = []
                        logger.error(
                            f'Error getting usernames: {e}\n{traceback.format_exc()}'  # noqa
                        )
                    if usernames:
                        usernames_str = ','.join(usernames)
                    else:
                        usernames_str = '无改名记录'
                    try:
                        if user_id:
                            channel_count = (
                                db_manager.count_non_null_channel_message_id(
                                    str(user_id)
                                )
                            )
                            group_count = (
                                db_manager.count_non_null_group_message_id(
                                    str(user_id)
                                )
                            )
                        else:
                            channel_count = '未查到相关记录'
                            group_count = '未查到相关记录'
                    except Exception as e:
                        channel_count = '未查到相关记录'
                        group_count = '未查到相关记录'
                        logger.error(
                            f'''Error counting messages: {e}\n
                            {traceback.format_exc()}'''
                        )
                elif user_name is not None:
                    usernames = []
                    usernames_str = '无改名记录'
                    try:
                        channel_count = db_manager.count_non_null_channel_message_id_by_name(  # noqa
                            user_name
                        )
                        group_count = (
                            db_manager.count_non_null_group_message_id_by_name(
                                user_name
                            )
                        )

                    except Exception as e:
                        channel_count = '未查到相关记录'
                        group_count = '未查到相关记录'
                        logger.error(
                            f'Error counting messages: {e}\n{traceback.format_exc()}'  # noqa
                        )
                else:
                    usernames = []
                    usernames_str = '无改名记录'
                    channel_count = '未查到相关记录'
                    group_count = '未查到相关记录'
                message = f'{message.text}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
                logger.debug(f'Ready to Transfer Channel Message: {message}')
                self.send_message_to_a_channel(message)
        except Exception as e:
            logger.error(
                f'Error in hezu_channel_handler: {e}\n{traceback.format_exc()}'
            )


class BatchProcessHandler(BaseHandler):
    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)

    def get_last_message_id(self, channel_or_group):
        if channel_or_group == CHANNEL:
            return db_manager.get_latest_channel_message_id()
        else:
            return db_manager.get_latest_group_message_id()

    async def run_history(self, channel_or_group):
        last_message_id = self.get_last_message_id(channel_or_group)
        records = []
        entity = (
            int(environment.hezu_channel_chatid)
            if channel_or_group == CHANNEL
            else int(environment.hezu_group_chatid)
        )
        limit = (
            environment.channel_message_limit
            if channel_or_group == CHANNEL
            else environment.group_message_limit
        )
        async for message in self.client.iter_messages(
            entity,
            limit=limit,
        ):
            try:
                if (channel_or_group == CHANNEL) and (
                    message.text[0] != '#' or message.text.startswith('#恰饭广告')
                ):
                    continue
                if (channel_or_group == GROUP) and (
                    not message.text.startswith('#非审核车')
                ):
                    continue
                parsed_message = await self.parse_message(
                    message, channel_or_group
                )
                current_message_id = (
                    parsed_message['channel_message_id']
                    or parsed_message['group_message_id']
                )
                if last_message_id and current_message_id <= int(
                    last_message_id
                ):
                    break
                records.append(parsed_message)
                if len(records) >= int(environment.batch_size):
                    logger.info(
                        f'write {environment.batch_size} channel message rows to table'  # noqa
                    )
                    db_manager.add_records(records)
                    records = []
            except Exception as e:
                logger.error(
                    f'Error in init_channel_messages: {e}\n{traceback.format_exc()}'  # noqa
                )
        if records:
            logger.info('write remain channel message rows to table')
            db_manager.add_records(records)
            records = []

    async def run_sync(self, channel_or_group):
        last_message_id = self.get_last_message_id()
        entity = (
            int(environment.hezu_channel_chatid)
            if channel_or_group == CHANNEL
            else int(environment.hezu_group_chatid)
        )
        limit = (
            environment.channel_message_limit
            if channel_or_group == CHANNEL
            else environment.group_message_limit
        )
        async for message in self.client.iter_messages(
            entity,
            limit=limit,
        ):
            try:
                if (channel_or_group == CHANNEL) and (
                    message.text[0] != '#' or message.text.startswith('#恰饭广告')
                ):
                    continue
                if (channel_or_group == GROUP) and (
                    not message.text.startswith('#非审核车')
                ):
                    continue
                parsed_message = await self.parse_message(
                    message, channel_or_group
                )
                current_message_id = (
                    parsed_message['channel_message_id']
                    or parsed_message['group_message_id']
                )
                if last_message_id and current_message_id <= int(
                    last_message_id
                ):
                    break
                logger.debug(f'Parse Message: {parsed_message}')
                db_manager.add_record(parsed_message)
                user_id = parsed_message['owner_id']
                user_name = parsed_message['owner_username']
                if user_id is None and user_name is not None:
                    user_id = db_manager.get_user_id_by_username(user_name)
                if user_id:
                    try:
                        usernames = db_manager.get_usernames_by_user_id(
                            str(user_id)
                        )
                    except Exception as e:
                        usernames = []
                        logger.error(
                            f'Error getting usernames: {e}\n{traceback.format_exc()}'  # noqa
                        )
                    if usernames:
                        usernames_str = ','.join(usernames)
                    else:
                        usernames_str = '无改名记录'
                    try:
                        if user_id:
                            channel_count = (
                                db_manager.count_non_null_channel_message_id(
                                    str(user_id)
                                )
                            )
                            group_count = (
                                db_manager.count_non_null_group_message_id(
                                    str(user_id)
                                )
                            )
                        else:
                            channel_count = '未查到相关记录'
                            group_count = '未查到相关记录'
                    except Exception as e:
                        channel_count = '未查到相关记录'
                        group_count = '未查到相关记录'
                        logger.error(
                            f'''Error counting messages: {e}\n
                            {traceback.format_exc()}'''
                        )
                elif user_name is not None:
                    usernames = []
                    usernames_str = '无改名记录'
                    try:
                        channel_count = db_manager.count_non_null_channel_message_id_by_name(  # noqa
                            user_name
                        )
                        group_count = (
                            db_manager.count_non_null_group_message_id_by_name(
                                user_name
                            )
                        )

                    except Exception as e:
                        channel_count = '未查到相关记录'
                        group_count = '未查到相关记录'
                        logger.error(
                            f'Error counting messages: {e}\n{traceback.format_exc()}'  # noqa
                        )
                else:
                    usernames = []
                    usernames_str = '无改名记录'
                    channel_count = '未查到相关记录'
                    group_count = '未查到相关记录'
                summary_message = f'{message.text}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
                logger.debug(
                    f'Ready to Transfer Channel Message: {summary_message}'
                )
                self.send_message_to_a_channel(message)
            except Exception as e:
                logger.error(
                    f'Error in lookup_group_messages: {e}\n{traceback.format_exc()}'  # noqa
                )

import logging
import asyncio
import traceback
from env import environment
from db.manager import db_manager
from telethon import TelegramClient, events
from telethon.errors import FloodError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Remember to use your own values from my.telegram.org!
client = TelegramClient(
    environment.session,
    environment.api_id,
    environment.api_hash,
    # proxy={'proxy_type': 'socks5', 'addr': '127.0.0.1', 'port': '1080'},
)


async def parse_message(message, is_channel=True):
    try:
        owner_username = message.text.split('@')[-1].strip()
    except Exception as e:
        owner_username = None
        logger.error(
            f'Error parsing owner_username: {e}\n{traceback.format_exc()}'
        )
    try:
        if owner_username:
            owner_id = await client.get_peer_id(owner_username)
        else:
            owner_id = None
    except Exception as e:
        owner_id = None
        logger.error(f'Error getting owner_id: {e}\n{traceback.format_exc()}')
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
            user = await client.get_entity(sender_id)
            sender_username = user.username
        except Exception as e:
            sender_username = None
            logger.error(
                f'Error getting sender_username: {e}\n{traceback.format_exc()}'
            )
    else:
        sender_id = message.sender.id
        try:
            user = await client.get_entity(sender_id)
            sender_username = user.username
        except Exception as e:
            sender_username = None
            logger.error(
                f'Error getting sender_username: {e}\n{traceback.format_exc()}'
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
        if is_channel
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


async def init_channel_messages():
    last_channel_id = db_manager.get_latest_channel_message_id()
    records = []
    async for message in client.iter_messages(
        int(environment.hezu_channel_chatid),
        limit=environment.channel_message_limit,
    ):
        try:
            if message.text[0] != '#' or message.text.startswith('#恰饭广告'):
                continue
            parsed_message = await parse_message(message)
            if last_channel_id and parsed_message['channel_message_id'] <= int(
                last_channel_id
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


async def init_group_messages():
    last_group_id = db_manager.get_latest_group_message_id()
    records = []

    async for message in client.iter_messages(
        int(environment.hezu_group_chatid),
        limit=environment.group_message_limit,
    ):
        try:
            if not message.text.startswith('#非审核车'):
                continue
            parsed_message = await parse_message(message, is_channel=False)
            if (
                last_group_id
                and parsed_message['group_message_id'] <= last_group_id
            ):
                break
            records.append(parsed_message)
            if len(records) >= int(environment.batch_size):
                logger.info(
                    f'write {environment.batch_size} group message rows to table'  # noqa
                )
                db_manager.add_records(records)
                records = []
        except Exception as e:
            logger.error(
                f'Error in init_group_messages: {e}\n{traceback.format_exc()}'
            )

    if records:
        logger.info('write remain group message rows to table')
        db_manager.add_records(records)
        records = []


async def main():
    try:
        me = await client.get_me()
        logger.info(me)
        await init_channel_messages()
        logger.info('channel message update success')
        await init_group_messages()
        logger.info('group message update success')
    except Exception as e:
        logger.error(f'Error in main: {e}\n{traceback.format_exc()}')


@client.on(
    events.NewMessage(
        pattern='(?i)#非审核车+', chats=int(environment.hezu_group_chatid)
    )
)
async def hezu_group_handler(event):
    try:
        logger.info(f'receive message for group: {event.message.text}')
        await client.get_dialogs()
        if event.message.text.startswith('#非审核车'):
            parsed_message = await parse_message(
                event.message, is_channel=False
            )
            logger.debug(f'Parse Group Message: {parsed_message}')
            db_manager.add_record(parsed_message)
            user_id = parsed_message['owner_id'] or parsed_message['sender_id']
            user_name = (
                parsed_message['owner_username']
                or parsed_message['sender_username']
            )
            if user_id is None and user_name is not None:
                user_id = db_manager.get_user_id_by_username(user_name)
            if user_id:
                try:
                    usernames = db_manager.get_user_info(user_id)
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
                        f'Error counting messages: {e}\n{traceback.format_exc()}'  # noqa
                    )
            elif user_name is not None:
                usernames = []
                usernames_str = '无改名记录'
                try:
                    channel_count = (
                        db_manager.count_non_null_channel_message_id_by_name(
                            user_name
                        )
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
            message = f'{event.message.text}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
            logger.debug(f'Ready to Transfer Group Message: {message}')
            try:
                await client.send_message(
                    int(environment.hezu_summary_chatid), message
                )
            except FloodError as e:
                logger.error(
                    f'Error in hezu_group_handler: {e}\n{traceback.format_exc()}'  # noqa
                )
                logger.info('Waiting for 300 seconds, and retry...')
                await asyncio.sleep(300)
                await client.send_message(
                    int(environment.hezu_summary_chatid), message
                )
    except Exception as e:
        logger.error(
            f'Error in hezu_group_handler: {e}\n{traceback.format_exc()}'
        )


@client.on(
    events.NewMessage(
        pattern='(?i)#+', chats=int(environment.hezu_channel_chatid)
    )
)
async def hezu_channel_handler(event):
    try:
        logger.info(f'receive message for channel: {event.message.text}')
        await client.get_dialogs()
        if event.message.text[0] != '#' or event.message.text.startswith(
            '#恰饭广告'
        ):
            pass
        else:
            parsed_message = await parse_message(event.message)
            logger.debug(f'Parse Channel Message: {parsed_message}')
            db_manager.add_record(parsed_message)
            user_id = parsed_message['owner_id']
            user_name = parsed_message['owner_username']
            if user_id is None and user_name is not None:
                user_id = db_manager.get_user_id_by_username(user_name)
            if user_id:
                try:
                    usernames = db_manager.get_user_info(user_id)
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
                        f'Error counting messages: {e}\n{traceback.format_exc()}'  # noqa
                    )
            elif user_name is not None:
                usernames = []
                usernames_str = '无改名记录'
                try:
                    channel_count = (
                        db_manager.count_non_null_channel_message_id_by_name(
                            user_name
                        )
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
            message = f'{event.message.text}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
            logger.debug(f'Ready to Transfer Channel Message: {message}')
            try:
                await client.send_message(
                    int(environment.hezu_summary_chatid), message
                )
            except FloodError as e:
                logger.error(
                    f'Error in hezu_group_handler: {e}\n{traceback.format_exc()}'  # noqa
                )
                logger.info('Waiting for 300 seconds, and retry...')
                await asyncio.sleep(300)
                await client.send_message(
                    int(environment.hezu_summary_chatid), message
                )
    except Exception as e:
        logger.error(
            f'Error in hezu_channel_handler: {e}\n{traceback.format_exc()}'
        )


if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())
        client.run_until_disconnected()

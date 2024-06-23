import asyncio
import traceback
from log import logger
from env import environment
from message.handler import BatchProcessHandler, MessageHandler, CHANNEL, GROUP
from telethon import TelegramClient, events

# Remember to use your own values from my.telegram.org!
client = TelegramClient(
    environment.session,
    environment.api_id,
    environment.api_hash,
    # proxy={'proxy_type': 'socks5', 'addr': '127.0.0.1', 'port': '1080'},
)

batch_processer = BatchProcessHandler(client)
message_processer = MessageHandler(client)


async def parse_message(message, is_channel=True):
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
    await batch_processer.run_history(CHANNEL)


async def init_group_messages():
    await batch_processer.run_history(GROUP)


async def sync_channel_messages():
    await batch_processer.run_sync(CHANNEL)


async def sync_group_messages():
    await batch_processer.run_sync(GROUP)


async def scheduled_task():
    await asyncio.sleep(3600)  # 一小时后再运行
    while True:
        try:
            logger.info('start to sync channel and group messages...')
            await sync_channel_messages()
            logger.info('sync channel messages complete')
            await sync_group_messages()
            logger.info('sync group messages complete')
        except Exception as e:
            logger.error(
                f'Error in scheduled_task: {e}\n{traceback.format_exc()}'
            )
        await asyncio.sleep(3600)  # 每小时运行一次


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
    await message_processer.run(event.message, GROUP)


@client.on(
    events.NewMessage(
        pattern='(?i)#+', chats=int(environment.hezu_channel_chatid)
    )
)
async def hezu_channel_handler(event):
    await message_processer.run(event.message, CHANNEL)


if __name__ == '__main__':
    with client:
        client.loop.create_task(scheduled_task())
        client.loop.run_until_complete(main())
        client.run_until_disconnected()

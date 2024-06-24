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


async def scheduled_task():
    await asyncio.sleep(3600)  # 一小时后再运行
    while True:
        try:
            logger.info('start to sync channel and group messages...')
            await batch_processer.run_sync(CHANNEL)
            logger.info('sync channel messages complete')
            await batch_processer.run_sync(GROUP)
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
        await batch_processer.run_history(CHANNEL)
        logger.info('channel message update success')
        await batch_processer.run_history(GROUP)
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

from env import environment
from db.manager import db_manager
from telethon import TelegramClient, events

# Remember to use your own values from my.telegram.org!
client = TelegramClient(
    environment.session,
    environment.api_id,
    environment.api_hash,
    proxy={'proxy_type': 'socks5', 'addr': '127.0.0.1', 'port': '1080'},
)


async def parse_message(message, is_channel=True):
    try:
        owner_username = message.text.split('@')[-1].strip()
    except:
        owner_username = None
    try:
        if owner_username:
            owner_id = await client.get_peer_id(owner_username)
        else:
            owner_id = None
    except:
        owner_id = None
    try:
        service_name = message.text.split(' ')[0].strip('#').strip()
    except:
        service_name = None
    message_info = (
        {
            'message': message.text,
            'channel_message_id': message.id,
            # "price": message.text,
            'owner_username': owner_username,
            'owner_id': owner_id,
            'sender_username': message.sender.username,
            'sender_id': message.sender.id,
            'service_name': service_name,
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
            'sender_username': message.sender.username,
            'sender_id': message.sender.id,
            'service_name': service_name,
            # "user_num": 1,
            # "expiration_date": message.text
        }
    )
    return message_info


async def init_channel_messages():
    # 获取DB最新消息
    last_channel_id = db_manager.get_latest_channel_message_id()
    records = []

    async for message in client.iter_messages(
        int(environment.hezu_channel_chatid),
        search='(?i)#+',
        limit=environment.channel_message_limit,
    ):  # noqa
        if message.text[0] != '#':
            continue
        parsed_message = await parse_message(message)
        if (
            last_channel_id
            and parsed_message['channel_message_id'] == last_channel_id
        ):
            if records:
                db_manager.add_records(records)
            break
        records.append(parsed_message)
        if len(records) >= int(environment.batch_size):
            db_manager.add_records(records)


async def init_group_messages():
    # 获取DB最新消息
    last_channel_id = db_manager.get_latest_channel_message_id()
    records = []

    async for message in client.iter_messages(
        int(environment.hezu_channel_chatid),
        search='(?i)#非审核车+',
        limit=environment.channel_message_limit,
    ):  # noqa
        if message.text[0] != '#':
            continue
        parsed_message = await parse_message(message, is_channel=False)
        if (
            last_channel_id
            and parsed_message['channel_message_id'] == last_channel_id
        ):
            if records:
                db_manager.add_records(records)
            break
        records.append(parsed_message)
        if len(records) >= int(environment.batch_size):
            db_manager.add_records(records)


async def main():

    await init_channel_messages()
    # Getting information about yourself
    me = await client.get_me()

    # "me" is a user object. You can pretty-print
    # any Telegram object with the "stringify" method:
    print(me.stringify())

    # When you print something, you see a representation of it.
    # You can access all attributes of Telegram objects with
    # the dot operator. For example, to get the username:
    username = me.username
    print(username)
    print(me.phone)
    print(me.id)
    await client.get_peer_id()

    # You can print all the dialogs/conversations that you are part of:
    async for dialog in client.iter_dialogs():
        print(dialog.name, 'has ID', dialog.id)

    # # You can, of course, use markdown in your messages:
    # message = await client.send_message(
    #     'me',
    #     'This message has **bold**, `code`, __italics__ and '
    #     'a [nice website](https://example.com)!',
    #     link_preview=False
    # )

    # # Sending a message returns the sent message object, which you can use
    # print(message.raw_text)

    # # You can reply to messages directly if you have a message object
    # await message.reply('Cool!')

    # # Or send files, songs, documents, albums...
    # await client.send_file('me', './private/images.jpeg')

    # You can print the message history of any chat:
    # async for message in client.iter_messages(environment.hezu_channel_chatid, limit=10): # noqa
    #     print(message.id, message.text)


@client.on(
    events.NewMessage(
        pattern='(?i)#非审核车+', chats=environment.hezu_group_chatid
    )
)
async def hezu_group_handler(event):
    # Respond whenever someone says "Hello" and something else
    # await event.reply('Hey!')
    # 获取到最新消息
    # 数据落库
    # 查询数据库中该用户信息：几次更改昵称，几次开审核车，几次开非审核车，最近一周开车x次，最近一月开车x次，该车平均价位，该区平均价位
    # 推送汇总频道：当前合租信息，结合历史数据分析结果
    print(event.message.message)
    await client.send_message(
        environment.hezu_summary_chatid, event.message.message
    )
    await client.send_message(
        environment.hezu_summary_chatid, event.message.message
    )


@client.on(
    events.NewMessage(pattern='(?i)#+', chats=environment.hezu_channel_chatid)
)
async def hezu_channel_handler(event):
    # Respond whenever someone says "Hello" and something else
    # await event.reply('Hey!')
    # 获取到最新消息
    # 数据落库
    # 查询数据库中该用户信息：几次更改昵称，几次开审核车，几次开非审核车，最近一周开车x次，最近一月开车x次，该车平均价位，该区平均价位
    # 推送汇总频道：当前合租信息，结合历史数据分析结果
    print(event.message.message)
    await client.send_message(
        environment.hezu_summary_chatid, event.message.message
    )
    await client.send_message(
        environment.hezu_summary_chatid, event.message.message
    )


with client:
    client.loop.run_until_complete(main())
    client.run_until_disconnected()

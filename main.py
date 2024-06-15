from env import environment
from db.manager import db_manager
from telethon import TelegramClient, events

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
    except:
        owner_username = None
    try:
        if owner_username:
            await client.get_dialogs()
            owner_id = await client.get_peer_id(owner_username)
        else:
            owner_id = None
    except:
        owner_id = None
    try:
        service_name = (
            message.text.replace('#非审核车', '')
            .strip()
            .split(' ')[0]
            .strip('#')
            .strip()
        )
    except:
        service_name = None
    if message.sender is None:
        sender_id = message.from_id.user_id
        try:
            await client.get_dialogs()
            user = await client.get_entity(owner_username)
            sender_username = user.username
        except:
            sender_username = None
    else:
        sender_id = message.sender.id
        sender_username = message.sender.username
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
    # 获取DB最新消息
    last_channel_id = db_manager.get_latest_channel_message_id()
    records = []
    async for message in client.iter_messages(
        int(environment.hezu_channel_chatid),
        limit=environment.channel_message_limit,
    ):  # noqa
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
                print(
                    f'write {environment.batch_size} channel message rows to table'  # noqa
                )
                db_manager.add_records(records)
                records = []
        except Exception as e:
            print(e)
    if records:
        print('write remain channel message rows to table')
        db_manager.add_records(records)
        records = []


async def init_group_messages():
    # 获取DB最新消息
    last_group_id = db_manager.get_latest_group_message_id()
    records = []

    async for message in client.iter_messages(
        int(environment.hezu_group_chatid),
        limit=environment.group_message_limit,
    ):  # noqa
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
                print(
                    f'write {environment.batch_size} group message rows to table'  # noqa
                )
                db_manager.add_records(records)
                records = []
        except Exception as e:
            print(e)

    if records:
        print('write remain group message rows to table')
        db_manager.add_records(records)
        records = []


async def main():
    # Getting information about yourself
    me = await client.get_me()
    print(me)
    await init_channel_messages()
    print('channel message update success')
    await init_group_messages()
    print('group message update success')


@client.on(
    events.NewMessage(
        pattern='(?i)#非审核车+', chats=int(environment.hezu_group_chatid)
    )
)
async def hezu_group_handler(event):
    # Respond whenever someone says "Hello" and something else
    # await event.reply('Hey!')
    # 获取到最新消息
    # 数据落库
    # 查询数据库中该用户信息：几次更改昵称，几次开审核车，几次开非审核车，最近一周开车x次，最近一月开车x次，该车平均价位，该区平均价位
    # 推送汇总频道：当前合租信息，结合历史数据分析结果
    try:
        print(f'receive message for group: {event.message.text}')
        if event.message.text.startswith('#非审核车'):
            parsed_message = await parse_message(event.message)
            db_manager.add_record(parsed_message)
            owner_id = parsed_message['owner_id']
            try:
                usernames = db_manager.get_user_info(owner_id)
            except:
                usernames = []
            usernames_str = ','.join(usernames)
            try:
                if owner_id:
                    channel_count = (
                        db_manager.count_non_null_channel_message_id(owner_id)
                    )
                    group_count = db_manager.count_non_null_group_message_id(
                        owner_id
                    )
                else:
                    channel_count = '未查到相关记录'
                    group_count = '未查到相关记录'
            except:
                channel_count = '未查到相关记录'
                group_count = '未查到相关记录'
            message = f'{event.message.text}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
            await client.send_message(
                int(environment.hezu_summary_chatid), message
            )
    except Exception as e:
        print(e)


@client.on(
    events.NewMessage(
        pattern='(?i)#+', chats=int(environment.hezu_channel_chatid)
    )
)
async def hezu_channel_handler(event):
    # Respond whenever someone says "Hello" and something else
    # await event.reply('Hey!')
    # 获取到最新消息
    # 数据落库
    # 查询数据库中该用户信息：几次更改昵称，几次开审核车，几次开非审核车，最近一周开车x次，最近一月开车x次，该车平均价位，该区平均价位
    # 推送汇总频道：当前合租信息，结合历史数据分析结果
    try:
        print(f'receive message for channel: {event.message.text}')
        if event.message.text[0] != '#' or event.message.text.startswith(
            '#恰饭广告'
        ):
            pass
        else:
            parsed_message = await parse_message(event.message)
            db_manager.add_record(parsed_message)
            owner_id = parsed_message['owner_id']
            try:
                usernames = db_manager.get_user_info(owner_id)
            except:
                usernames = []
            usernames_str = ','.join(usernames)
            try:
                if owner_id:
                    channel_count = (
                        db_manager.count_non_null_channel_message_id(owner_id)
                    )
                    group_count = db_manager.count_non_null_group_message_id(
                        owner_id
                    )
                else:
                    channel_count = '未查到相关记录'
                    group_count = '未查到相关记录'
            except:
                channel_count = '未查到相关记录'
                group_count = '未查到相关记录'
            message = f'{event.message.message}\n该用户改名次数：{len(usernames)}\n该用户历史名字：{usernames_str}\n该用户开审核车次数：{channel_count}\n该用户开非审核车次数：{group_count}'  # noqa
            await client.send_message(
                int(environment.hezu_summary_chatid), message
            )
    except Exception as e:
        print(e)


with client:
    client.loop.run_until_complete(main())
    client.run_until_disconnected()

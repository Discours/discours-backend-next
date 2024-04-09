import json

from orm.notification import Notification
from services.db import local_session
from services.rediscache import redis
from services.logger import root_logger as logger

def save_notification(action: str, entity: str, payload):
    with local_session() as session:
        n = Notification(action=action, entity=entity, payload=payload)
        session.add(n)
        session.commit()


async def notify_reaction(reaction, action: str = 'create'):
    channel_name = 'reaction'
    data = {'payload': reaction, 'action': action}
    try:
        save_notification(action, channel_name, data.get('payload'))
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        logger.error(f'Failed to publish to channel {channel_name}: {e}')


async def notify_shout(shout, action: str = 'update'):
    channel_name = 'shout'
    data = {'payload': shout, 'action': action}
    try:
        save_notification(action, channel_name, data.get('payload'))
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        logger.error(f'Failed to publish to channel {channel_name}: {e}')


async def notify_follower(follower: dict, author_id: int, action: str = 'follow'):
    channel_name = f'follower:{author_id}'
    try:
        # Simplify dictionary before publishing
        simplified_follower = {k: follower[k] for k in ['id', 'name', 'slug', 'pic']}
        data = {'payload': simplified_follower, 'action': action}
        # save in channel
        save_notification(action, channel_name, data.get('payload'))

        # Convert data to JSON string
        json_data = json.dumps(data)

        # Ensure the data is not empty before publishing
        if  json_data:
            # Use the 'await' keyword when publishing
            await redis.publish(channel_name, json_data)


    except Exception as e:
        # Log the error and re-raise it
        logger.error(f'Failed to publish to channel {channel_name}: {e}')

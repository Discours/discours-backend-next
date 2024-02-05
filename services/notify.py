import json

from services.rediscache import redis


async def notify_reaction(reaction, action: str = 'create'):
    channel_name = 'reaction'
    data = {'payload': reaction, 'action': action}
    try:
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        print(f'[services.notify] Failed to publish to channel {channel_name}: {e}')


async def notify_shout(shout, action: str = 'update'):
    channel_name = 'shout'
    data = {'payload': shout, 'action': action}
    try:
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        print(f'[services.notify] Failed to publish to channel {channel_name}: {e}')


async def notify_follower(follower: dict, author_id: int, action: str = 'follow'):
    channel_name = f'follower:{author_id}'
    try:
        # Simplify dictionary before publishing
        simplified_follower = {k: follower[k] for k in ['id', 'name', 'slug', 'pic']}

        data = {'payload': simplified_follower, 'action': action}

        # Convert data to JSON string
        json_data = json.dumps(data)

        # Ensure the data is not empty before publishing
        if not json_data:
            raise ValueError('Empty data to publish.')

        # Use the 'await' keyword when publishing
        await redis.publish(channel_name, json_data)

    except Exception as e:
        # Log the error and re-raise it
        print(f'[services.notify] Failed to publish to channel {channel_name}: {e}')
        raise

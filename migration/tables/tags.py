import json
from datetime import datetime
from orm.base import local_session
from orm import Topic
from dateutil.parser import parse as date_parse

def migrate(entry):
    '''
    type Topic {
        slug: String! # ID
        createdBy: Int! # User
        createdAt: DateTime!
        title: String
        parents: [String] # NOTE: topic can have parent topics
        children: [String] # and children
    }
    '''
    if type(entry['createdAt']) == type(''):
        ts = date_parse(entry['createdAt'])
    else:
        ts = datetime.fromtimestamp(entry['createdAt']/1000)
    topic_dict = {
        'slug': entry['slug'],
        # 'createdBy': entry['createdBy'],
        # 'createdAt': ts,
        'title': entry['title'].lower(),
        'parents': [],
        'children': []
    }
    try:
        with local_session() as session:
            topic = session.query(Topic).filter(Topic.slug == entry['slug']).first()
            if not topic: topic = Topic.create(**topic_dict)
            topic_dict['id'] = topic.id
    except Exception as e:
        print(e)
        raise e
    
    topic_dict['tag_id'] = entry['_id']
    return topic_dict

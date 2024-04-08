from sqlalchemy import and_, distinct, func, select
from sqlalchemy.orm import aliased

from orm.author import Author
from orm.community import Community, CommunityAuthor
from orm.shout import ShoutCommunity
from services.db import local_session
from services.logger import root_logger as logger
from services.schema import query


def add_community_stat_columns(q):
    community_followers = aliased(CommunityAuthor)
    shout_community_aliased = aliased(ShoutCommunity)

    q = q.outerjoin(shout_community_aliased).add_columns(
        func.count(distinct(shout_community_aliased.shout)).label('shouts_stat')
    )
    q = q.outerjoin(
        community_followers, community_followers.author == Author.id
    ).add_columns(
        func.count(distinct(community_followers.follower)).label('followers_stat')
    )

    q = q.group_by(Author.id)

    return q


def get_communities_from_query(q):
    ccc = []
    with local_session() as session:
        for [c, shouts_stat, followers_stat] in session.execute(q):
            c.stat = {
                'shouts': shouts_stat,
                'followers': followers_stat,
                # "commented": commented_stat,
            }
            ccc.append(c)

    return ccc


# for mutation.field("follow")
def community_follow(follower_id, slug):
    try:
        with local_session() as session:
            community = session.query(Community).where(Community.slug == slug).first()
            if isinstance(community, Community):
                cf = CommunityAuthor(author=follower_id, community=community.id)
                session.add(cf)
                session.commit()
                return True
    except Exception as ex:
        logger.debug(ex)
    return False


# for mutation.field("unfollow")
def community_unfollow(follower_id, slug):
    with local_session() as session:
        flw = (
            session.query(CommunityAuthor)
            .join(Community, Community.id == CommunityAuthor.community)
            .filter(and_(CommunityAuthor.author == follower_id, Community.slug == slug))
            .first()
        )
        if flw:
            session.delete(flw)
            session.commit()
            return True
    return False


@query.field('get_communities_all')
async def get_communities_all(_, _info):
    q = select(Author)
    q = add_community_stat_columns(q)

    return get_communities_from_query(q)


@query.field('get_community')
async def get_community(_, _info, slug: str):
    q = select(Community).where(Community.slug == slug)
    q = add_community_stat_columns(q)

    communities = get_communities_from_query(q)
    return communities[0]

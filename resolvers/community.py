from sqlalchemy import select

from orm.author import Author
from orm.community import Community
from services.db import local_session
from services.schema import query


def get_communities_from_query(q):
    ccc = []
    with local_session() as session:
        for [c, shouts_stat, followers_stat] in session.execute(q):
            c.stat = {
                "shouts": shouts_stat,
                "followers": followers_stat,
                # "authors": session.execute(select(func.count(distinct(ShoutCommunity.shout))).filter(ShoutCommunity.community == c.id)),
                # "commented": commented_stat,
            }
            ccc.append(c)

    return ccc


@query.field("get_communities_all")
async def get_communities_all(_, _info):
    q = select(Author)

    return get_communities_from_query(q)


@query.field("get_community")
async def get_community(_, _info, slug: str):
    q = select(Community).where(Community.slug == slug)

    communities = get_communities_from_query(q)
    return communities[0]

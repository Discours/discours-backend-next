from orm.author import Author
from orm.community import Community, CommunityFollower
from services.db import local_session
from services.schema import query


@query.field("get_communities_all")
async def get_communities_all(_, _info):
    return local_session().query(Community).all()


@query.field("get_community")
async def get_community(_, _info, slug: str):
    q = local_session().query(Community).where(Community.slug == slug)
    return q.first()


@query.field("get_communities_by_author")
async def get_communities_by_author(_, _info, slug="", user="", author_id=0):
    with local_session() as session:
        q = session.query(Community).join(CommunityFollower)
        if slug:
            author_id = session.query(Author).where(Author.slug == slug).first().id
            q = q.where(CommunityFollower.author == author_id)
        if user:
            author_id = session.query(Author).where(Author.user == user).first().id
            q = q.where(CommunityFollower.author == author_id)
        if author_id:
            q = q.where(CommunityFollower.author == author_id)
        return q.all()
    return []

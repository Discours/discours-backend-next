from orm.author import Author
from orm.community import Community, CommunityFollower
from services.db import local_session
from services.schema import mutation, query


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


@mutation.field("join_community")
async def join_community(_, info, slug: str):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    with local_session() as session:
        community = session.query(Community).where(Community.slug == slug).first()
        if not community:
            return {"ok": False, "error": "Community not found"}
        session.add(CommunityFollower(community=community.id, author=author_id))
        session.commit()
        return {"ok": True}


@mutation.field("leave_community")
async def leave_community(_, info, slug: str):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    with local_session() as session:
        session.query(CommunityFollower).where(
            CommunityFollower.author == author_id, CommunityFollower.community == slug
        ).delete()
        session.commit()
        return {"ok": True}


@mutation.field("create_community")
async def create_community(_, info, community_data):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    with local_session() as session:
        session.add(Community(author=author_id, **community_data))
        session.commit()
        return {"ok": True}


@mutation.field("update_community")
async def update_community(_, info, community_data):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    slug = community_data.get("slug")
    if slug:
        with local_session() as session:
            try:
                session.query(Community).where(Community.created_by == author_id, Community.slug == slug).update(
                    community_data
                )
                session.commit()
            except Exception as e:
                return {"ok": False, "error": str(e)}
            return {"ok": True}
    return {"ok": False, "error": "Please, set community slug in input"}


@mutation.field("delete_community")
async def delete_community(_, info, slug: str):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    with local_session() as session:
        try:
            session.query(Community).where(Community.slug == slug, Community.created_by == author_id).delete()
            session.commit()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

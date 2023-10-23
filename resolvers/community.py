from base.orm import local_session
from base.resolvers import query
from orm.author import Author
from orm.community import Community, CommunityAuthor
from orm.shout import ShoutCommunity
from sqlalchemy import select, distinct, func, literal, and_
from sqlalchemy.orm import aliased


def add_community_stat_columns(q):
    community_followers = aliased(CommunityAuthor)
    shout_community_aliased = aliased(ShoutCommunity)

    q = q.outerjoin(shout_community_aliased).add_columns(
        func.count(distinct(shout_community_aliased.shout)).label("shouts_stat")
    )
    q = q.outerjoin(
        community_followers, community_followers.author == Author.id
    ).add_columns(
        func.count(distinct(community_followers.follower)).label("followers_stat")
    )

    q = q.add_columns(literal(0).label("rating_stat"))
    # FIXME
    # q = q.outerjoin(author_rating_aliased, author_rating_aliased.user == Author.id).add_columns(
    #     # TODO: check
    #     func.sum(author_rating_aliased.value).label('rating_stat')
    # )

    q = q.add_columns(literal(0).label("commented_stat"))
    # q = q.outerjoin(Reaction, and_(Reaction.createdBy == Author.id, Reaction.body.is_not(None))).add_columns(
    #     func.count(distinct(Reaction.id)).label('commented_stat')
    # )

    q = q.group_by(Author.id)

    return q


def get_communities_from_query(q):
    ccc = []
    with local_session() as session:
        for [c, *stat_columns] in session.execute(q):
            [shouts_stat, followers_stat, rating_stat, commented_stat] = stat_columns
            c.stat = {
                "shouts": shouts_stat,
                "followers": followers_stat,
                "rating": rating_stat,
                "commented": commented_stat,
            }
            ccc.append(c)

    return ccc


def followed_communities(follower_id):
    amount = select(Community).count()
    if amount < 2:
        # no need to run long query most of the cases
        return [
            select(Community).first(),
        ]
    else:
        q = select(Community)
        q = add_community_stat_columns(q)
        q = q.join(CommunityAuthor, CommunityAuthor.community == Community.id).where(
            CommunityAuthor.follower == follower_id
        )
        # 3. Pass the query to the get_authors_from_query function and return the results
        return get_communities_from_query(q)


# for mutation.field("follow")
def community_follow(follower_id, slug):
    try:
        with local_session() as session:
            community = session.query(Community).where(Community.slug == slug).one()
            cf = CommunityAuthor.create(author=follower_id, community=community.id)
            session.add(cf)
            session.commit()
        return True
    except Exception:
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


@query.field("communitiesAll")
async def get_communities_all(_, _info):
    q = select(Author)
    q = add_community_stat_columns(q)

    return get_communities_from_query(q)


@query.field("getCommunity")
async def get_community(_, _info, slug):
    q = select(Community).where(Community.slug == slug)
    q = add_community_stat_columns(q)

    authors = get_communities_from_query(q)
    return authors[0]

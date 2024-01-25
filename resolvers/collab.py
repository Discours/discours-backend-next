from orm.author import Author
from orm.invite import Invite, InviteStatus
from orm.shout import Shout
from services.auth import login_required
from services.db import local_session
from services.schema import mutation


@mutation.field('accept_invite')
@login_required
async def accept_invite(_, info, invite_id: int):
    user_id = info.context['user_id']

    # Check if the user exists
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            # Check if the invite exists
            invite = session.query(Invite).filter(Invite.id == invite_id).first()
            if invite and invite.author_id is author.id and invite.status is InviteStatus.PENDING.value:
                # Add the user to the shout authors
                shout = session.query(Shout).filter(Shout.id == invite.shout_id).first()
                if shout:
                    if author not in shout.authors:
                        shout.authors.append(author)
                        session.delete(invite)
                        session.add(shout)
                        session.commit()
                    return {'success': True, 'message': 'Invite accepted'}
                else:
                    return {'error': 'Shout not found'}
            else:
                return {'error': 'Invalid invite or already accepted/rejected'}
        else:
            return {'error': 'User not found'}


@mutation.field('reject_invite')
@login_required
async def reject_invite(_, info, invite_id: int):
    user_id = info.context['user_id']

    # Check if the user exists
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            # Check if the invite exists
            invite = session.query(Invite).filter(Invite.id == invite_id).first()
            if invite and invite.author_id is author.id and invite.status is InviteStatus.PENDING.value:
                # Delete the invite
                session.delete(invite)
                session.commit()
                return {'success': True, 'message': 'Invite rejected'}
            else:
                return {'error': 'Invalid invite or already accepted/rejected'}
        else:
            return {'error': 'User not found'}


@mutation.field('create_invite')
@login_required
async def create_invite(_, info, slug: str = '', author_id: int = 0):
    user_id = info.context['user_id']

    # Check if the inviter is the owner of the shout
    with local_session() as session:
        shout = session.query(Shout).filter(Shout.slug == slug).first()
        inviter = session.query(Author).filter(Author.user == user_id).first()
        if inviter and shout and shout.authors and inviter.id is shout.created_by:
            # Check if the author is a valid author
            author = session.query(Author).filter(Author.id == author_id).first()
            if author:
                # Check if an invite already exists
                existing_invite = (
                    session.query(Invite)
                    .filter(
                        Invite.inviter_id == inviter.id,
                        Invite.author_id == author_id,
                        Invite.shout_id == shout.id,
                        Invite.status == InviteStatus.PENDING.value,
                    )
                    .first()
                )
                if existing_invite:
                    return {'error': 'Invite already sent'}

                # Create a new invite
                new_invite = Invite(
                    inviter_id=user_id, author_id=author_id, shout_id=shout.id, status=InviteStatus.PENDING.value
                )
                session.add(new_invite)
                session.commit()

                return {'error': None, 'invite': new_invite}
            else:
                return {'error': 'Invalid author'}
        else:
            return {'error': 'Access denied'}


@mutation.field('remove_author')
@login_required
async def remove_author(_, info, slug: str = '', author_id: int = 0):
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            shout = session.query(Shout).filter(Shout.slug == slug).first()
            # NOTE: owner should be first in a list
            if shout and author.id is shout.created_by:
                shout.authors = [author for author in shout.authors if author.id != author_id]
                session.commit()
                return {}
    return {'error': 'Access denied'}


@mutation.field('remove_invite')
@login_required
async def remove_invite(_, info, invite_id: int):
    user_id = info.context['user_id']

    # Check if the user exists
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            # Check if the invite exists
            invite = session.query(Invite).filter(Invite.id == invite_id).first()
            if isinstance(invite, Invite):
                shout = session.query(Shout).filter(Shout.id == invite.shout_id).first()
                if shout and shout.deleted_at is None and invite:
                    if invite.inviter_id is author.id or author.id is shout.created_by:
                        if invite.status is InviteStatus.PENDING.value:
                            # Delete the invite
                            session.delete(invite)
                            session.commit()
                            return {}
            else:
                return {'error': 'Invalid invite or already accepted/rejected'}
        else:
            return {'error': 'Author not found'}

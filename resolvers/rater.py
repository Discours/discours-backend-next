
from orm.reaction import ReactionKind


RATING_REACTIONS = [
    ReactionKind.LIKE.value,
    ReactionKind.ACCEPT.value,
    ReactionKind.AGREE.value,
    ReactionKind.DISLIKE.value,
    ReactionKind.REJECT.value,
    ReactionKind.DISAGREE.value]



def is_negative(x):
    return x in [
        ReactionKind.ACCEPT.value,
        ReactionKind.LIKE.value,
        ReactionKind.PROOF.value,
    ]


def is_positive(x):
    return x in [
        ReactionKind.ACCEPT.value,
        ReactionKind.LIKE.value,
        ReactionKind.PROOF.value,
    ]

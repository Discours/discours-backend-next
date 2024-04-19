from orm.reaction import ReactionKind

PROPOSAL_REACTIONS = [
    ReactionKind.ACCEPT.value,
    ReactionKind.REJECT.value,
    ReactionKind.AGREE.value,
    ReactionKind.DISAGREE.value,
    ReactionKind.ASK.value,
    ReactionKind.PROPOSE.value,
]

PROOF_REACTIONS = [ReactionKind.PROOF.value, ReactionKind.DISPROOF.value]

RATING_REACTIONS = [ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]


def is_negative(x):
    return x in [
        ReactionKind.DISLIKE.value,
        ReactionKind.DISPROOF.value,
        ReactionKind.REJECT.value,
    ]


def is_positive(x):
    return x in [
        ReactionKind.ACCEPT.value,
        ReactionKind.LIKE.value,
        ReactionKind.PROOF.value,
    ]

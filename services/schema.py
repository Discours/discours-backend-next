from ariadne import ScalarType, QueryType, MutationType


datetime_scalar = ScalarType("DateTime")
query = QueryType()
mutation = MutationType()


@datetime_scalar.serializer
def serialize_datetime(value):
    return value.isoformat()


# NOTE: was used by studio
# @query.field("_service")
# def resolve_service(*_):
#    # Load the full SDL from your SDL file
#    with open("schemas/core.graphql", "r") as file:
#        full_sdl = file.read()
#
#    return {"sdl": full_sdl}


resolvers = [query, mutation, datetime_scalar]

from ariadne import MutationType, QueryType
from ariadne import load_schema_from_path, make_executable_schema

query = QueryType()
mutation = MutationType()
resolvers = [query, mutation]

schema = make_executable_schema(load_schema_from_path('schema/'), resolvers)

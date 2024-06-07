# FILE TO PRACTICE, TEST AND DEVELOP


from QueryBuilder import QueryBuilder       # import as it should normally be done
from tests.Config import PostgresConfig     # import of bank settings for testing only

# Example
# POSTGRES_CONFIG = {
#             'database': 'stolen_vehicles_db',
#             'host': 'localhost',
#             'port': 5432,
#             'user_name': 'test',
#             'password': 'test'
#         }

POSTGRES_CONFIG = PostgresConfig().get_config()

locations = QueryBuilder().Postgres(**POSTGRES_CONFIG)
query = QueryBuilder().Postgres(**POSTGRES_CONFIG)

results = (
    locations
    # .get_query()
    .select(['country AS pais', 'population AS populacao'])
    .from_table('locations')
    .where_subquery('population', 'IN',
        lambda: locations.select(['population']),
        lambda: locations.from_table('locations'),
        lambda: locations.where('country', '=', 'Brazil'),
        lambda: locations.subquery(
            lambda: locations.where('country', '=', 'Brazil'),
            lambda: locations.where_or('population', '>', 1000),
            combinator='AND'
        )
    )
    .get()
)
# SELECT country AS pais , population AS populacao FROM public.locations WHERE population IN ( SELECT population FROM public.locations WHERE country = 'Brazil' AND ( country = 'Brazil' OR population > 1000 ) );

# results = (
#     query
#     .get_query()
#     .select(['country'])
#     .from_table('locations')
#     .where('country', '=', 'Brazil')
#     .where_or('population', '>', 1000)
#     .get()
# )
#SELECT country FROM public.locations WHERE country = 'Brazil' OR population > 1000;

# print(results)

for result in results:
    print(result)

# results = (
#     qb1
#     .where('country', '=', 'USA')
#     .subquery(
#         lambda query = 'OR': qb1.where('country', '=', 'USA', query)
#         )
# )


# print(qb1.current_subquery)
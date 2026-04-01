# import dagster as dg
# from dagster_postgres import PostgresResource


# my_app_postgres_resource = PostgresResource(
#     host=dg.EnvVar("PG_HOST"),
#     port=dg.EnvVar.int("PG_PORT"), 
#     username=dg.EnvVar("PG_USERNAME"),
#     password=dg.EnvVar("PG_PASSWORD"),
#     database=dg.EnvVar("PG_DATABASE"),
# )
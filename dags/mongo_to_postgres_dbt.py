from airflow.decorators import dag, task
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import dlt
from pymongo import MongoClient
import os
from datetime import timedelta
import dlt

# Load environment variables

CONNECTION_ID = "db_conn"

# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/include/dbt"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": "public"}
    )
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)
MONGO_URI = os.getenv("MONGO_URI")

@dlt.source
def mongo_source():
    client = MongoClient(MONGO_URI)
    db = client["sample_mflix"]

    # Parent table: movies
    # - merge mode with stable primary key from Mongo _id
    @dlt.resource(
        name="movies",
        write_disposition="merge",
        primary_key="_id"
    )
    def movies():
        for doc in db["movies"].find():
            doc["_id"] = str(doc["_id"])  # ensure stable string ID
            yield doc

    # Independent table: comments
    @dlt.resource(
        name="comments",
        write_disposition="merge",
        primary_key="_id"
    )
    def comments():
        for doc in db["comments"].find():
            doc["_id"] = str(doc["_id"])
            doc["movie_id"] = str(doc["movie_id"])
            yield doc

    # Independent table: users
    @dlt.resource(
        name="users",
        write_disposition="merge",
        primary_key="_id"
    )
    def users():
        for doc in db["users"].find():
            doc["_id"] = str(doc["_id"])
            yield doc

    return movies(), comments(), users()

@dag(
    dag_id="mongo_to_postgres_dbt",
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["mongo", "dlt", "dbt", "cosmos"]
)
def pipeline_dag():
    
    @task
    def load_mongo_data():
        pipeline = dlt.pipeline(
            pipeline_name="mongo_pipeline",
            destination="postgres",
            dataset_name="mongo_data"
        )

        load_info = pipeline.run(mongo_source(), credentials={
                "database": "test_db",
                "username": "admin",
                "password": "admin",
                "host": "host.docker.internal",
                "port": 5433
            })
        print("DLT load result:", load_info)

    ingest = load_mongo_data()

    dbt_build = DbtTaskGroup(
        group_id="run_dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )

    ingest >> dbt_build

pipeline_dag = pipeline_dag()
# This DAG ingests data from MongoDB into PostgreSQL using DLT and then runs a DBT build.
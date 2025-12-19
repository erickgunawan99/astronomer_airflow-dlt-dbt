MongoDB to Postgres Data Pipeline: DLT, dbt, & Airflow
* This project implements an automated ETL pipeline that migrates raw data from MongoDB to a PostgreSQL warehouse, applies modular transformations using dbt, and orchestrates the entire flow via Airflow using the Astronomer Cosmos integration.
  
🛠️ Tech Stack
- Orchestration: Airflow (Astronomer)

- Ingestion: DLT (Data Load Tool)

- Transformation: dbt (Data Build Tool)

- Databases: MongoDB (Source), PostgreSQL (Warehouse)

🚀 Overview
The pipeline ingests "Movies" and "Comments" data from MongoDB, handles incremental loading, and builds analytical models to identify user-specific movie preferences based on sentiment-filtered comments.

+ Ingestion: Python dlt (Data Load Tool) for schema evolution and incremental loading.

+ Transformation: dbt for modular SQL modeling (Staging -> Marts).

+ Orchestration: Airflow with Cosmos to render dbt models as native Airflow DAG tasks.

🛠️ Technical Implementation
1. Ingestion (DLT)
We utilize dlt with the pymongo client to extract collections incrementally.

Incremental Logic: Using write_disposition="merge", we ensure that documents are updated or inserted based on their MongoDB _id to maintain a stable primary key.

Metadata Tracking: DLT automatically generates a _dlt_loads table. We join this in our staging layer to assign an inserted_at timestamp to each row, enabling downstream incremental processing even for source tables without native date columns.

2. Transformation (dbt)
The transformation layer is split into two primary phases:

Staging: Cleanses raw data and joins it with DLT metadata to establish a reliable loading timeline for the incremental models.

Marts (Sentiment & User Profiles): * Regex-based Filtering: Uses complex regex predicates to identify "good" comments while filtering out "not good" modifiers, ensuring only positive feedback is aggregated.

Incremental Aggregation: Implements an incremental strategy to update user preference arrays (e.g., fav_genres, fav_directors). While Postgres requires unnesting arrays for joins—adding complexity compared to modern warehouses—the logic ensures we only process new data.

3. Orchestration (Airflow + Cosmos)
By using Astronomer Cosmos, the dbt project is automatically parsed into an Airflow DbtTaskGroup.

<img width="1347" height="657" alt="Screenshot 2025-08-13 231529" src="https://github.com/user-attachments/assets/6b168b05-4c86-496e-aba9-5dc63da3b748" />

Visual Dependencies: Provides clear visibility of model relationships and dependencies directly within the Airflow UI.

Integrated Quality: Automatically runs dbt test and dbt snapshot as part of the DAG, ensuring data integrity at every step.

📈 Key Results
The pipeline successfully transforms raw JSON-like structures into flattened, analytical tables:

Refined User Profiles: Aggregates comment counts and distinct "favorite" lists (genres, directors, titles) per user based on sentiment.

Efficiency: The incremental logic reduces compute costs and execution time by focusing only on changed records from the MongoDB source.

<img width="1133" height="165" alt="Screenshot 2025-08-13 231431" src="https://github.com/user-attachments/assets/eae907b9-8b52-49cb-8665-ed4518a7fce6" />
<img width="1117" height="204" alt="Screenshot 2025-08-13 231448" src="https://github.com/user-attachments/assets/991b01ba-667f-437f-8905-17e2c98530e1" />




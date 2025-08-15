Astronomer to run airflow. Moving data from DLT into Postgres using DLT Python package. Then fiddle with them using DBT.
Also trying Astronomer's cosmos tool to break up a DBT project models into a visualized airflow-esque DAG with clear dependencies within each other and run them on airflow.


<img width="904" height="766" alt="Screenshot 2025-08-15 224027" src="https://github.com/user-attachments/assets/da1c95b9-2c9a-4ea1-94e3-a1ba480f8727" />

Utilizing DLT and Pymongo client to read movies and comments collections in mongo and return them, then insert them incrementally to Postgres.

<img width="905" height="872" alt="Screenshot 2025-08-15 224101" src="https://github.com/user-attachments/assets/cd496038-db54-454e-adee-872c4292f597" />

Cosmos provides DbtTaskGroup a method to run DBT build command (tests, snapshots, and run) on airflow.

<img width="1347" height="657" alt="Screenshot 2025-08-13 231529" src="https://github.com/user-attachments/assets/6b168b05-4c86-496e-aba9-5dc63da3b748" />

Moving data using DLT table generates a dlt_load table that contains a insertion date column. 
It also has a load id column which also present in the each raw table

<img width="1147" height="793" alt="Screenshot 2025-08-14 003928" src="https://github.com/user-attachments/assets/33629293-1311-4276-a9f9-cbf496d3c77b" />

Join the raw table and dlt load table as a staging view to assign date (insertion date column) to each row 
in order to run an incremental query in the marts model in case there's no date column present in the initial table.

<img width="1249" height="239" alt="Screenshot 2025-08-14 003739" src="https://github.com/user-attachments/assets/6e57f465-1007-4b37-9e2f-6497795b23e8" />

Experimenting by doing an unnessecarily long incremental query doing the count and concatanation which almost has no permorfance benefit over rebuilding the table with simple query everytime. 
Would be better on modern warehouses since they have rich ways to manipulate arrays, while Postgres needs to unnest the arrays in order to have a flat array when joining two arrays that added the complexity.

<img width="515" height="804" alt="Screenshot 2025-08-13 232906" src="https://github.com/user-attachments/assets/c73fe180-2f50-4f15-b454-333cf9ebca1e" />

<img width="775" height="325" alt="Screenshot 2025-08-13 234248" src="https://github.com/user-attachments/assets/b25db3ff-2764-4a75-b2c0-ae70b7681509" />

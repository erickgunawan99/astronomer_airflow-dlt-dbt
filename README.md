Astronomer to run airflow. Moving data from DLT into Postgres using DLT Python package. Then fiddle with them using DBT.
Also trying Astronomer's cosmos tool to break up a DBT project models into a visualized airflow-esque DAG with clear dependencies within each other and then run them on airflow.


<img width="904" height="766" alt="Screenshot 2025-08-15 224027" src="https://github.com/user-attachments/assets/da1c95b9-2c9a-4ea1-94e3-a1ba480f8727" />

Utilizing DLT and Pymongo client to read movies and comments collections in mongo and return them, then insert them incrementally to Postgres.

<img width="905" height="872" alt="Screenshot 2025-08-15 224101" src="https://github.com/user-attachments/assets/cd496038-db54-454e-adee-872c4292f597" />

Cosmos provides DbtTaskGroup, a method to run DBT build command (tests, snapshots, and run) on airflow.

<img width="1347" height="657" alt="Screenshot 2025-08-13 231529" src="https://github.com/user-attachments/assets/6b168b05-4c86-496e-aba9-5dc63da3b748" />

Moving data using DLT table generates a dlt_load table that contains a insertion date column. 
It also has a load id column, which also present in each raw table

<img width="1147" height="793" alt="Screenshot 2025-08-14 003928" src="https://github.com/user-attachments/assets/33629293-1311-4276-a9f9-cbf496d3c77b" />

Join the raw table and dlt load table as a staging view to assign date (insertion date column) to each row 
in order to run an incremental query in the marts model in case there's no date column present in the initial table.

<img width="1249" height="239" alt="Screenshot 2025-08-14 003739" src="https://github.com/user-attachments/assets/6e57f465-1007-4b37-9e2f-6497795b23e8" />

Experimenting by doing an unnessecarily long incremental query doing the count and concatanation which almost has no permorfance benefit over rebuilding the table with simple query everytime. 
Would be better on modern warehouses since they have rich ways to manipulate arrays, while Postgres needs to unnest the arrays in order to have a flat array when joining two arrays that added the complexity.

<img width="515" height="804" alt="Screenshot 2025-08-13 232906" src="https://github.com/user-attachments/assets/c73fe180-2f50-4f15-b454-333cf9ebca1e" />

<img width="775" height="325" alt="Screenshot 2025-08-13 234248" src="https://github.com/user-attachments/assets/b25db3ff-2764-4a75-b2c0-ae70b7681509" />

Testing the pipeline by having a regex table that grabs the elements of movies for each user with a "good" comment. THe first run yields:

<img width="434" height="117" alt="Screenshot 2025-08-13 213700" src="https://github.com/user-attachments/assets/772b8079-030b-4c41-8895-0f666a26df70" />
<img width="1110" height="202" alt="Screenshot 2025-08-13 213648" src="https://github.com/user-attachments/assets/3cb1c7e7-95f0-4421-8a70-2bd7008cb88a" />

Then insert some data to test the DBT model that filters only movies, genres, and directors that have "good" comments and the "good" doesnt get prefixed by "not" for each user. A very customizable of course with extended regex conditions. The model query:

<img width="999" height="469" alt="Screenshot 2025-08-13 232717" src="https://github.com/user-attachments/assets/f78daeaa-4171-4724-a9b7-d516c79c4b1b" />

Inserting one "good" and one "not good". Should only 1 that get picked up and added to the collections if it works correctly 

<img width="1201" height="896" alt="Screenshot 2025-08-13 231712" src="https://github.com/user-attachments/assets/f6415024-133b-4727-bfe0-65a433fdecaf" />

Run the airflow. Cosmos also run the DBT tests too as explained. The simple tests script: 
<img width="883" height="903" alt="Screenshot 2025-08-13 234224" src="https://github.com/user-attachments/assets/0befd2a7-df24-45ea-8d86-01645258f5f8" />

Two comments count added for the certain user.

<img width="896" height="152" alt="Screenshot 2025-08-13 213934" src="https://github.com/user-attachments/assets/6b790c81-bd49-48fc-863a-143c9017c302" />
<img width="984" height="178" alt="Screenshot 2025-08-13 231502" src="https://github.com/user-attachments/assets/8f3f1278-5cd1-4e74-825f-d8140354a9cf" />


But only one movie title is added to the fav_movies array column of the only_good_comments table for the same user. Also only related genres and director related to that movie with "good" comment added to the fav_genres and fav_director respectively

<img width="1133" height="165" alt="Screenshot 2025-08-13 231431" src="https://github.com/user-attachments/assets/eae907b9-8b52-49cb-8665-ed4518a7fce6" />
<img width="1117" height="204" alt="Screenshot 2025-08-13 231448" src="https://github.com/user-attachments/assets/991b01ba-667f-437f-8905-17e2c98530e1" />




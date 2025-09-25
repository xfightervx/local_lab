## Rides Analytics — Mini Local Lab

This is a mini local lab to test and get familier with (POSTGRES,SPARK,KAFKA,DBT,AIRFLOW) for a test data
from a simulating LA taxi traffic using a time compression of 1 day = 1 min for faster testing and execution (this scale my change along the way and can be changed using env (check .env-template for more infos) but the idea is to minimize the system idle time)

the success criterias are simple :
    - get a end to end pipeline with working and resilient processes 
    - get a working ci/cd pipeline
    - create tests with a dicent couverage


might be added later :
    - transform the architecure to a hybride setting
    - add other sources and optimize spark + dbt excution
    - add a reporting layer
    - add ML layer
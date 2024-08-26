# Summary

This repo contains a basic DAG that more-or-less does the following:

1. Ingests JSON data from S3 and loads it to staging tables in Redshift
2. Uses the staging data to create / update one fact table and four dimensions
3. Performs some data quality checks after-the-fact to ensure that everything executed correctly

However, despite the commonplace nature of these tasks, there are some setup steps that need to be configured correctly, so be use to read through the setup steps to make sure that you adapt this code base to your use case as needed.

# Setup

1. Run the [`create_tables.sql`](create_tables.sql) script in Redshift to create the fact, dimension, and staging tables.
2. Fill in your AWS and Redshift info as appropriate in [`set_connections_and_variables.sh`](set_connections_and_variables.sh).
3. In your Airflow environment (presuming that Airflow and its affiliated services are already running), run the shell script (or fill in the same information using Airflow's UI).
4. Load the DAG and operators to your Airflow directory and wait for the scheduler to pick them up.
5. Activate the DAG and wait for it to run, or manually trigger it.

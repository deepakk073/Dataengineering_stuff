#Before execute the code set the Google_Application_credetial and set the key
from google.cloud import bigquery
client = bigquery.Client()
dataset_id="pip"
table = "dm_time_taken_to_close_a_campaign"
dataset_ref = client.dataset(dataset_id)
print(dataset_ref.table)
job_config = bigquery.QueryJobConfig(destination=dataset_ref.table(table))
job_config.allow_quoted_newlines = True
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
query = """
SELECT * FROM `ma-c4m-prod.pip.vw_time_taken_to_close_a_campaign`
"""
print("deep")
query_job = client.query(query, job_config=job_config)
print(query_job)
query_job.result()
print("Query results loaded to the table {}".format(table_id))

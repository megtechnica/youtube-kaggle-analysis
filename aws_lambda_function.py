import awswrangler as wr
import pandas as pd
import urllib.parse
import os

s3_clean_room = os.environ['s3_clean_room']
glue_catalog_db = os.environ['glue_catalog_db']
glue_catalog_table = os.environ['glue_catalog_table']
write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extract required columns:
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=s3_clean_room,
            dataset=True,
            database=glue_catalog_db,
            table=glue_catalog_table,
            mode=write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

import boto3
import pandas as pd
import os
import json

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    # Get the bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(key)
    print(event)
    print(bucket)
    # Download the CSV file from S3
    download_path = '/tmp/{}'.format(key)
    s3.download_file(bucket, key, download_path)
    print(download_path)
    # Process the CSV file using pandas
    
    with open(download_path) as f:
       df = pd.DataFrame([json.loads(l) for l in f.readlines()])

       df_filtered=df.loc[lambda df: df['status'] == 'delivered']
    
       print(df_filtered)
    # Perform processing, for example:
    # df_processed = df.dropna()  # Example processing
    # Save the filtered data to a new JSON file
    #filtered_df = df[df['status'] == 'delivered']
    # Save the processed data to a new CSV file
    
    json_data = df_filtered.to_json(orient='records')
    # Save the filtered data to a new JSON file
    destination_bucket = 'doordash-target-znn'
    destination_key = 'json_data'
    s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=json_data)

    # Delete the temporary file
    

    message ='File saved to S3'

    
    
    # Publish a success message to the SNS topic
    #sns_topic_arn = 'arn:aws:sns:us-east-1:058264439636:s3-notification'
    sns.publish(TopicArn='arn:aws:sns:ap-south-1:058264439636:s3-notification',Message='File uploaded',Subject='S3 data uploaded')
    # Log the response
    #print(response)
    
    return {
        'statusCode': 200,
        'body': 'File processed and saved successfully'
    }
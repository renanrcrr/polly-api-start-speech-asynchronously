"""
The code is developed using reference from
https://docs.aws.amazon.com/polly/latest/dg/python-samples-overall.html
"""

import json
import boto3
import logging
from botocore.exceptions import ClientError

# It is good practice to use proper logging.
# Here we are using the logging module of python.
# https://docs.python.org/3/library/logging.html

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Using boto3 S3 Client
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

s3 = boto3.client('s3')


def lambda_handler(event, context):
    
    # log the event 
    logger.info(event)

    # Define default polly response and s3 path
    output_key = 'output/polly_response.json'
    response = {}
    
    for record in event['Records']:
        
        # Get the bucket name and key of the file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(key)

        # Split the key to extract only filename
        filename = key.split("/")[-1]
        print(filename)
        
        
        # Download the file to tmp directory of the Lambda.
        
        try:
            local_file_name = '/tmp/'+ filename
            with open(local_file_name, 'wb') as data:
                s3.download_fileobj(bucket, key, data)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                continue
            else:
                raise
            
        # Amazon Polly Client
        # More Info: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/polly.html
       
        polly_client = boto3.client('polly')
        
        # Define default Polly Task Status
        
        task_status = "null"
        line = ""

        # Define the Voice_ID.
        # For list of supported voice and engine, refer to
        # https://docs.aws.amazon.com/polly/latest/dg/voicelist.html

        voice_id='<Enter_Your_Voice_ID>'

        # Define the language code
        # For the list of supported language codes, refer to
        # https://docs.aws.amazon.com/polly/latest/dg/SupportedLanguage.html

        language_code = '<Enter_Your_Language_Code>'
        
        # Read the file and convert to string
        # Start speech synthesis task to convert the string to speech.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/polly.html#Polly.Client.start_speech_synthesis_task
        
        try:
            with open(local_file_name, 'r') as file:
                line = file.read().replace("\n", " ")
                print(line)
                response = polly_client.start_speech_synthesis_task( # You are using start_speech_synthesis_task API
                        Engine='neural',
                        LanguageCode=language_code,
                        OutputFormat='mp3',
                        OutputS3BucketName=bucket,
                        OutputS3KeyPrefix="output/"+filename,
                        Text=line,
                        TextType='text',
                        VoiceId=voice_id
                        )
            
                taskid = response['SynthesisTask']['TaskId']
                task_status = response['SynthesisTask']['TaskStatus']
                output_filename = filename + "." + taskid + ".mp3"

            return_result = {"FileName":output_filename,"TaskStatus":task_status}
        except Exception as error:
            print(error)
            return_result = {"Status":"Failed", "Reason":error}
        
    # Save response is S3 bucket
        s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=json.dumps(response, default=str, indent=4)
        )

        return return_result

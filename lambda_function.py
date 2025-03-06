import json
import boto3
import uuid
import os
import time
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
lambda_client = boto3.client('lambda', region_name='us-east-2')

def lambda_handler(event, context):
    try:
        print(f"Received event: {json.dumps(event)}")
        
        # Check if the event is from API Gateway
        if 'body' in event and 'requestContext' in event and 'http' in event['requestContext']:
            print("Processing API Gateway event")
            # Parse the body from API Gateway event
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']
                
            # Extract parameters from the API Gateway event
            if 'args' in body and 'company' in body['args']:
                company_name = body['args']['company']
                address = body['args'].get('address')
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing required parameters in API Gateway event')
                }
        else:
            print("Processing direct Lambda invocation")
            # Extract parameters from direct Lambda invocation
            if 'company' not in event:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing required parameter: company')
                }
            company_name = event['company']
            address = event.get('address')
        
        print(f"Processing request for company: {company_name}, address: {address}")
        
        # Get data from NextAvailableCache table
        cache_table = dynamodb.Table('NextAvailableCache')
        cache_response = cache_table.get_item(
            Key={
                'companyName': company_name
            }
        )
        
        # If cache is empty, call get-locksmith-eta Lambda
        if 'Item' not in cache_response:
            print(f"Cache miss for company: {company_name}. Calling get-locksmith-eta Lambda...")
            
            # Prepare payload for get-locksmith-eta Lambda
            payload = {
                "address": address,
                "company": company_name
            }
            
            # Invoke get-locksmith-eta Lambda
            response = lambda_client.invoke(
                FunctionName='get-locksmith-eta',
                InvocationType='RequestResponse',  # Synchronous invocation
                Payload=json.dumps(payload)
            )
            
            # Parse the response
            response_payload = json.loads(response['Payload'].read().decode())
            print(f"get-locksmith-eta response: {response_payload}")
            
            # Check the cache again after get-locksmith-eta has run
            print("Checking cache again after get-locksmith-eta execution...")
            cache_response = cache_table.get_item(
                Key={
                    'companyName': company_name
                }
            )
            
            # If cache is still empty after calling get-locksmith-eta
            if 'Item' not in cache_response:
                return {
                    'statusCode': 404,
                    'body': json.dumps(f'No available locksmith found for company: {company_name} even after calling get-locksmith-eta')
                }
        
        cache_item = cache_response['Item']
        locksmith_id = cache_item.get('locksmithId')
        travel_time = cache_item.get('travelTime', 0)
        job_address = cache_item.get('jobAddress', '')
        
        # Use 'latitude' instead of 'jlatitude'
        job_latitude = cache_item.get('latitude')
        job_longitude = cache_item.get('longitude')
        
        print(f"Cache item: {cache_item}")
        print(f"Extracted values - locksmithId: {locksmith_id}, travelTime: {travel_time}, jobAddress: {job_address}, latitude: {job_latitude}, longitude: {job_longitude}")
        
        # Access the company-specific locksmith jobs table
        jobs_table_name = f"{company_name}LocksmithJobs"
        jobs_table = dynamodb.Table(jobs_table_name)
        
        # Get the locksmith record
        locksmith_response = jobs_table.get_item(
            Key={
                'locksmithId': locksmith_id
            }
        )
        
        if 'Item' not in locksmith_response:
            return {
                'statusCode': 404,
                'body': json.dumps(f'Locksmith with ID {locksmith_id} not found')
            }
        
        locksmith_item = locksmith_response['Item']
        
        # Create a new job entry
        new_job = {
            'jobId': f"JOB{uuid.uuid4().hex[:7].upper()}",
            'address': job_address,
            'latitude': job_latitude,
            'longitude': job_longitude,
            'estimatedTime': 30,  # Default estimated time is 30 minutes
            'travelTime': travel_time,
            'arrived': False
        }
        
        # Append the new job to the job queue
        job_queue = locksmith_item.get('jobQueue', [])
        job_queue.append(new_job)
        
        # Update the locksmith record
        jobs_table.update_item(
            Key={
                'locksmithId': locksmith_id
            },
            UpdateExpression='SET jobQueue = :jobQueue',
            ExpressionAttributeValues={
                ':jobQueue': job_queue
            }
        )
        
        # Delete the cache entry after successful booking
        cache_table.delete_item(
            Key={
                'companyName': company_name
            }
        )
        
        result = {
            'message': 'Locksmith appointment scheduled successfully',
            'jobId': new_job['jobId'],
            'locksmithId': locksmith_id,
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error scheduling locksmith appointment: {str(e)}')
        } 
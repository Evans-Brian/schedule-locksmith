import boto3
import os
import zipfile
import io
import time

def create_deployment_package():
    """Create a deployment package for the Lambda function"""
    print("Creating deployment package...")
    
    # Create a zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Add lambda_function.py to the zip
        zip_file.write('lambda_function.py')
    
    zip_buffer.seek(0)
    return zip_buffer.read()

def deploy_lambda(zip_content, role_arn=None):
    """Deploy the Lambda function"""
    print("Deploying Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    
    # Check if the function already exists
    try:
        lambda_client.get_function(FunctionName='schedule-locksmith')
        print("Lambda function already exists. Updating code...")
        
        # Update the existing function
        response = lambda_client.update_function_code(
            FunctionName='schedule-locksmith',
            ZipFile=zip_content,
            Publish=True
        )
        
        # Update the role if provided
        if role_arn:
            max_retries = 5
            retry_count = 0
            while retry_count < max_retries:
                try:
                    print(f"Updating function configuration with role: {role_arn}")
                    lambda_client.update_function_configuration(
                        FunctionName='schedule-locksmith',
                        Role=role_arn
                    )
                    print("Function configuration updated successfully")
                    break
                except lambda_client.exceptions.ResourceConflictException:
                    retry_count += 1
                    wait_time = 5 * retry_count
                    print(f"Update in progress, retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
            
            if retry_count == max_retries:
                print("Failed to update function configuration after maximum retries")
        
    except lambda_client.exceptions.ResourceNotFoundException:
        print("Creating new Lambda function...")
        
        if not role_arn:
            print("Error: Role ARN is required to create a new Lambda function")
            return None
        
        # Create the function
        response = lambda_client.create_function(
            FunctionName='schedule-locksmith',
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={
                'ZipFile': zip_content
            },
            Description='Lambda function to schedule locksmith appointments',
            Timeout=30,
            MemorySize=128,
            Publish=True
        )
    
    print(f"Lambda function deployed successfully. ARN: {response['FunctionArn']}")
    return response['FunctionArn']

def main():
    """Main function to deploy the Lambda"""
    # Create the deployment package
    zip_content = create_deployment_package()
    
    # Get role ARN from environment variable or command line argument
    role_arn = os.environ.get('LAMBDA_ROLE_ARN')
    
    # Deploy the Lambda function
    function_arn = deploy_lambda(zip_content, role_arn)
    
    if function_arn:
        print("\nDeployment completed successfully!")
        print(f"Function ARN: {function_arn}")
        print("You can test the function using the AWS console or CLI with the test-event.json file.")
    else:
        print("\nDeployment failed or incomplete.")

if __name__ == "__main__":
    main() 
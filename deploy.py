"""
AWS Lambda Deployment Script

Deploys the Search Keyword Performance Analyzer to AWS Lambda.
Creates all required resources: S3 bucket, IAM role, Lambda function, S3 trigger.

Usage:
    python deploy.py --create    # Create all resources
    python deploy.py --update    # Update Lambda code only
    python deploy.py --delete    # Delete all resources
"""

import argparse
import boto3
import json
import zipfile
import time
import os

# Configuration
FUNCTION_NAME = 'search-keyword-processor'
ROLE_NAME = 'search-keyword-lambda-role'
BUCKET_PREFIX = 'adobe-search-keyword-data'
REGION = 'us-east-1'


class LambdaDeployer:
    """Handles AWS Lambda deployment for Search Keyword Processor."""

    def __init__(self):
        self.sts = boto3.client('sts', region_name=REGION)
        self.s3 = boto3.client('s3', region_name=REGION)
        self.iam = boto3.client('iam', region_name=REGION)
        self.lambda_client = boto3.client('lambda', region_name=REGION)
        self.account_id = self.sts.get_caller_identity()['Account']
        self.bucket_name = f"{BUCKET_PREFIX}-{self.account_id}"

    def create_deployment_package(self):
        """Create ZIP file with Lambda code."""
        zip_path = 'lambda_deployment.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write('lambda_handler.py')
        print(f"Created deployment package: {zip_path}")
        return zip_path

    def create_s3_bucket(self):
        """Create S3 bucket for input/output files."""
        try:
            if REGION == 'us-east-1':
                self.s3.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': REGION}
                )
            print(f"Created S3 bucket: {self.bucket_name}")
        except self.s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"S3 bucket already exists: {self.bucket_name}")

    def create_iam_role(self):
        """Create IAM role for Lambda."""
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }

        try:
            self.iam.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(trust_policy)
            )
            print(f"Created IAM role: {ROLE_NAME}")
        except self.iam.exceptions.EntityAlreadyExistsException:
            print(f"IAM role already exists: {ROLE_NAME}")

        # Attach policies
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": "arn:aws:logs:*:*:*"
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}",
                        f"arn:aws:s3:::{self.bucket_name}/*"
                    ]
                }
            ]
        }

        self.iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName='lambda-s3-policy',
            PolicyDocument=json.dumps(policy)
        )
        print("Attached S3 and CloudWatch policies")
        time.sleep(10)  # Wait for IAM propagation

    def create_lambda_function(self, zip_path):
        """Create Lambda function."""
        role_arn = f"arn:aws:iam::{self.account_id}:role/{ROLE_NAME}"

        with open(zip_path, 'rb') as f:
            zip_content = f.read()

        try:
            self.lambda_client.create_function(
                FunctionName=FUNCTION_NAME,
                Runtime='python3.11',
                Role=role_arn,
                Handler='lambda_handler.lambda_handler',
                Code={'ZipFile': zip_content},
                Timeout=300,
                MemorySize=512,
                Layers=[
                    f'arn:aws:lambda:{REGION}:336392948345:layer:AWSSDKPandas-Python311:26'
                ]
            )
            print(f"Created Lambda function: {FUNCTION_NAME}")
            self._wait_for_function_active()
        except self.lambda_client.exceptions.ResourceConflictException:
            print(f"Lambda function already exists, updating code...")
            self.update_lambda_code(zip_path)

    def update_lambda_code(self, zip_path=None):
        """Update Lambda function code."""
        if zip_path is None:
            zip_path = self.create_deployment_package()

        with open(zip_path, 'rb') as f:
            zip_content = f.read()

        self.lambda_client.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=zip_content
        )
        print(f"Updated Lambda function code: {FUNCTION_NAME}")
        self._wait_for_function_active()

    def _wait_for_function_active(self):
        """Wait for Lambda function to be active."""
        print("Waiting for Lambda to be active...", end='')
        while True:
            response = self.lambda_client.get_function(FunctionName=FUNCTION_NAME)
            state = response['Configuration']['State']
            if state == 'Active':
                print(" Ready!")
                break
            print(".", end='', flush=True)
            time.sleep(2)

    def add_s3_trigger(self):
        """Add S3 trigger to Lambda."""
        # Add permission for S3 to invoke Lambda
        try:
            self.lambda_client.add_permission(
                FunctionName=FUNCTION_NAME,
                StatementId='s3-trigger',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{self.bucket_name}',
                SourceAccount=self.account_id
            )
            print("Added S3 invoke permission")
        except self.lambda_client.exceptions.ResourceConflictException:
            print("S3 permission already exists")

        # Configure S3 bucket notification
        notification_config = {
            'LambdaFunctionConfigurations': [{
                'LambdaFunctionArn': f'arn:aws:lambda:{REGION}:{self.account_id}:function:{FUNCTION_NAME}',
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': 'input/'},
                            {'Name': 'suffix', 'Value': '.tsv'}
                        ]
                    }
                }
            }]
        }

        self.s3.put_bucket_notification_configuration(
            Bucket=self.bucket_name,
            NotificationConfiguration=notification_config
        )
        print(f"Configured S3 trigger: s3://{self.bucket_name}/input/*.tsv")

    def create(self):
        """Create all AWS resources."""
        print("\n=== Creating AWS Resources ===\n")
        zip_path = self.create_deployment_package()
        self.create_s3_bucket()
        self.create_iam_role()
        self.create_lambda_function(zip_path)
        self.add_s3_trigger()
        os.remove(zip_path)
        self._print_summary()

    def update(self):
        """Update Lambda code only."""
        print("\n=== Updating Lambda Code ===\n")
        zip_path = self.create_deployment_package()
        self.update_lambda_code(zip_path)
        os.remove(zip_path)
        print("\nLambda updated successfully!")

    def delete(self):
        """Delete all AWS resources."""
        print("\n=== Deleting AWS Resources ===\n")

        # Delete Lambda
        try:
            self.lambda_client.delete_function(FunctionName=FUNCTION_NAME)
            print(f"Deleted Lambda function: {FUNCTION_NAME}")
        except:
            print(f"Lambda function not found: {FUNCTION_NAME}")

        # Delete IAM role policy and role
        try:
            self.iam.delete_role_policy(RoleName=ROLE_NAME, PolicyName='lambda-s3-policy')
            self.iam.delete_role(RoleName=ROLE_NAME)
            print(f"Deleted IAM role: {ROLE_NAME}")
        except:
            print(f"IAM role not found: {ROLE_NAME}")

        # Empty and delete S3 bucket
        try:
            objects = self.s3.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    self.s3.delete_object(Bucket=self.bucket_name, Key=obj['Key'])
            self.s3.delete_bucket(Bucket=self.bucket_name)
            print(f"Deleted S3 bucket: {self.bucket_name}")
        except:
            print(f"S3 bucket not found: {self.bucket_name}")

        print("\nAll resources deleted!")

    def _print_summary(self):
        """Print deployment summary."""
        print(f"""
{'='*50}
DEPLOYMENT COMPLETE
{'='*50}

S3 Bucket:  {self.bucket_name}
Lambda:     {FUNCTION_NAME}
IAM Role:   {ROLE_NAME}

Usage:
  Upload:   aws s3 cp data.tsv s3://{self.bucket_name}/input/
  Output:   s3://{self.bucket_name}/output/

  Invoke:   aws lambda invoke --function-name {FUNCTION_NAME} \\
              --payload '{{"input_bucket":"{self.bucket_name}","input_key":"input/data.tsv"}}' \\
              response.json
""")


def main():
    parser = argparse.ArgumentParser(description='Deploy Search Keyword Processor to AWS Lambda')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--create', action='store_true', help='Create all AWS resources')
    group.add_argument('--update', action='store_true', help='Update Lambda code only')
    group.add_argument('--delete', action='store_true', help='Delete all AWS resources')

    args = parser.parse_args()
    deployer = LambdaDeployer()

    if args.create:
        deployer.create()
    elif args.update:
        deployer.update()
    elif args.delete:
        deployer.delete()


if __name__ == '__main__':
    main()

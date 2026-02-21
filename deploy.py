"""
Amazon Connect Agent State Tracker — Deployment Script

Automates the full lifecycle of the Agent State Tracker stack:

  python deploy.py          Deploy (create or update) the stack + Lambda code
  python deploy.py deploy   Same as above
  python deploy.py destroy  Empty S3 buckets and delete the stack

Uses only boto3 and the standard library. All parameters are collected
via interactive prompts with sensible defaults.

Prerequisites:
  - AWS credentials configured (aws configure, env vars, or IAM role)
  - boto3 installed (pip install boto3)
"""

import io
import logging
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, WaiterError

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
CFN_TEMPLATE_FILE = 'agent-state-cfn.yaml'
LAMBDA_FILES = [
    'handler.py',
    'enrichment.py',
    'parquet_writer.py',
    'service_page.py',
    'connect-sdk-service.bundle.js',
]


# ──────────────────────────────────────────────
# INPUT HELPERS
# ──────────────────────────────────────────────
def prompt(label, default=None, required=True):
    """Prompt the user for a value with an optional default."""
    if default is not None and default != '':
        display = f"  {label} [{default}]: "
    elif not required:
        display = f"  {label} (optional, press Enter to skip): "
    else:
        display = f"  {label}: "

    while True:
        value = input(display).strip()
        if not value and default is not None:
            return default
        if not value and not required:
            return ''
        if not value and required:
            print(f"    ** {label} is required.")
            continue
        return value


def confirm(message):
    """Ask the user to confirm an action. Returns True if confirmed."""
    response = input(f"  {message} (yes/no): ").strip().lower()
    return response in ('yes', 'y')


# ──────────────────────────────────────────────
# STACK STATUS HELPERS
# ──────────────────────────────────────────────
def get_stack_status(cfn_client, stack_name):
    """
    Return the stack status string, or None if the stack does not exist.
    """
    try:
        resp = cfn_client.describe_stacks(StackName=stack_name)
        return resp['Stacks'][0]['StackStatus']
    except ClientError as e:
        if 'does not exist' in str(e):
            return None
        raise


def print_stack_events(cfn_client, stack_name, limit=10):
    """Print recent stack events to help debug failures."""
    try:
        resp = cfn_client.describe_stack_events(StackName=stack_name)
        events = resp['StackEvents'][:limit]
        logger.info("Recent stack events:")
        for event in events:
            status = event.get('ResourceStatus', '')
            reason = event.get('ResourceStatusReason', '')
            logical_id = event.get('LogicalResourceId', '')
            timestamp = event.get('Timestamp', '')
            line = f"  {timestamp}  {logical_id:40s}  {status}"
            if reason:
                line += f"  — {reason}"
            print(line)
    except ClientError:
        logger.warning("Could not retrieve stack events.")


def get_stack_outputs(cfn_client, stack_name):
    """Return stack outputs as a dict of {OutputKey: OutputValue}."""
    resp = cfn_client.describe_stacks(StackName=stack_name)
    outputs = resp['Stacks'][0].get('Outputs', [])
    return {o['OutputKey']: o['OutputValue'] for o in outputs}


def get_stack_s3_buckets(cfn_client, stack_name):
    """Return physical bucket names for all S3 bucket resources in the stack."""
    buckets = []
    try:
        paginator = cfn_client.get_paginator('list_stack_resources')
        for page in paginator.paginate(StackName=stack_name):
            for resource in page['StackResourceSummaries']:
                if resource['ResourceType'] == 'AWS::S3::Bucket':
                    physical_id = resource.get('PhysicalResourceId')
                    if physical_id:
                        buckets.append(physical_id)
    except ClientError as e:
        logger.error("Failed to list stack resources: %s", e)
    return buckets


# ──────────────────────────────────────────────
# S3 BUCKET EMPTYING
# ──────────────────────────────────────────────
def empty_s3_bucket(s3_client, bucket_name):
    """
    Delete all objects, versions, and delete markers from an S3 bucket.
    Handles both versioned and non-versioned buckets.
    """
    logger.info("Emptying bucket: %s", bucket_name)
    total_deleted = 0

    try:
        # Try versioned delete first (works for both versioned and non-versioned)
        paginator = s3_client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=bucket_name):
            objects_to_delete = []

            # Collect versions
            for version in page.get('Versions', []):
                objects_to_delete.append({
                    'Key': version['Key'],
                    'VersionId': version['VersionId'],
                })

            # Collect delete markers
            for marker in page.get('DeleteMarkers', []):
                objects_to_delete.append({
                    'Key': marker['Key'],
                    'VersionId': marker['VersionId'],
                })

            if objects_to_delete:
                # Delete in batches of 1000 (S3 API limit)
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch, 'Quiet': True},
                    )
                    total_deleted += len(batch)

        logger.info(
            "Deleted %d objects/versions from %s", total_deleted, bucket_name
        )

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.info("Bucket %s does not exist, skipping.", bucket_name)
        else:
            raise


# ──────────────────────────────────────────────
# LAMBDA CODE PACKAGING
# ──────────────────────────────────────────────
def package_lambda_code():
    """
    Zip the Lambda source files into an in-memory archive.
    Returns the zip bytes.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in LAMBDA_FILES:
            filepath = SCRIPT_DIR / filename
            if not filepath.exists():
                raise FileNotFoundError(
                    f"Required Lambda file not found: {filepath}"
                )
            zf.write(filepath, filename)
            logger.info("  Packed: %s", filename)

    buf.seek(0)
    zip_bytes = buf.getvalue()
    size_kb = len(zip_bytes) / 1024
    logger.info("Lambda package size: %.1f KB", size_kb)
    return zip_bytes


# ──────────────────────────────────────────────
# LAMBDA CODE UPLOAD
# ──────────────────────────────────────────────
def upload_lambda_code(lambda_client, function_name, zip_bytes):
    """Upload the zip package to the Lambda function and wait for readiness."""
    logger.info("Uploading code to Lambda function: %s", function_name)
    lambda_client.update_function_code(
        FunctionName=function_name,
        ZipFile=zip_bytes,
    )

    # Wait for the function to become Active
    logger.info("Waiting for Lambda function to become active...")
    for _ in range(60):
        resp = lambda_client.get_function(FunctionName=function_name)
        state = resp['Configuration']['State']
        update_status = resp['Configuration'].get('LastUpdateStatus', '')
        if state == 'Active' and update_status in ('Successful', ''):
            logger.info("Lambda function is active.")
            return
        if state == 'Failed' or update_status == 'Failed':
            raise RuntimeError(
                f"Lambda update failed: State={state}, "
                f"LastUpdateStatus={update_status}"
            )
        time.sleep(2)

    raise TimeoutError("Timed out waiting for Lambda function to become active.")


# ──────────────────────────────────────────────
# SECURITY PROFILE ACCESS
# ──────────────────────────────────────────────
def grant_security_profile_access(connect_client, instance_arn, app_namespace):
    """
    List security profiles and let the user choose which ones
    should have access to the 3P service.
    """
    instance_id = instance_arn.split('/')[-1]

    # List available security profiles
    try:
        profiles = []
        paginator = connect_client.get_paginator('list_security_profiles')
        for page in paginator.paginate(InstanceId=instance_id):
            profiles.extend(page['SecurityProfileSummaryList'])
    except ClientError as e:
        logger.error("Failed to list security profiles: %s", e)
        return

    if not profiles:
        logger.warning("No security profiles found.")
        return

    print("\n  Available security profiles:")
    for i, profile in enumerate(profiles, 1):
        print(f"    {i}. {profile['Name']} ({profile['Id']})")

    selection = prompt(
        "Enter profile numbers to grant access (comma-separated, e.g., 1,3)",
    )

    selected_indices = []
    for part in selection.split(','):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(profiles):
                selected_indices.append(idx)

    if not selected_indices:
        logger.info("No valid profiles selected, skipping.")
        return

    app_config = [{
        'Namespace': app_namespace,
        'ApplicationPermissions': ['ACCESS'],
    }]

    for idx in selected_indices:
        profile = profiles[idx]
        try:
            connect_client.update_security_profile(
                SecurityProfileId=profile['Id'],
                InstanceId=instance_id,
                Applications=app_config,
            )
            logger.info(
                "Granted access to profile: %s", profile['Name']
            )
        except ClientError as e:
            logger.error(
                "Failed to update profile '%s': %s", profile['Name'], e
            )


# ──────────────────────────────────────────────
# DEPLOY COMMAND
# ──────────────────────────────────────────────
def cmd_deploy():
    """Deploy or update the Agent State Tracker stack and Lambda code."""
    print("\n=== Agent State Tracker — Deploy ===\n")

    # ── Collect parameters ────────────────────
    region = prompt("AWS Region", default=boto3.Session().region_name or 'us-east-1')
    stack_name = prompt("Stack name (without env suffix)", default='agent-state-tracker')
    environment = prompt("Environment suffix (e.g., dev, prod, staging)", default='dev')
    stack_name = f"{stack_name}-{environment}"
    logger.info("Full stack name: %s", stack_name)
    connect_domain = prompt("Connect instance domain (e.g., myinstance.my.connect.aws)")
    bucket_name = prompt("S3 bucket name for Parquet data", default='softwareone-agent-state-551642657889')
    prefix = prompt("S3 key prefix", default='agent-state-events')
    athena_bucket = prompt(
        "Athena results bucket name",
        default='softwareone-athena-results-551642657889',
    )
    bytes_limit = prompt("Athena bytes scan limit", default='2199023255552')
    retention_days = prompt("Data retention days", default='90')
    layer_arn = prompt(
        "AWSSDKPandas Lambda Layer ARN",
        default='arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16',
    )
    connect_instance_arn = prompt(
        "Connect instance ARN"
    )

    print()

    # ── Build clients ─────────────────────────
    session = boto3.Session(region_name=region)
    cfn = session.client('cloudformation')
    lam = session.client('lambda')
    connect = session.client('connect')

    # ── Read template ─────────────────────────
    template_path = SCRIPT_DIR / CFN_TEMPLATE_FILE
    if not template_path.exists():
        logger.error("Template not found: %s", template_path)
        sys.exit(1)
    template_body = template_path.read_text(encoding='utf-8')
    logger.info("Loaded template: %s", CFN_TEMPLATE_FILE)

    # ── Build CFN parameters ──────────────────
    cfn_params = [
        {'ParameterKey': 'ConnectInstanceDomain', 'ParameterValue': connect_domain},
        {'ParameterKey': 'AgentStateBucketName', 'ParameterValue': bucket_name},
        {'ParameterKey': 'AgentStatePrefix', 'ParameterValue': prefix},
        {'ParameterKey': 'AthenaResultsBucketName', 'ParameterValue': athena_bucket},
        {'ParameterKey': 'BytesScanLimit', 'ParameterValue': bytes_limit},
        {'ParameterKey': 'DataRetentionDays', 'ParameterValue': retention_days},
        {'ParameterKey': 'LambdaLayerArn', 'ParameterValue': layer_arn},
        {'ParameterKey': 'ConnectInstanceArn', 'ParameterValue': connect_instance_arn},
        {'ParameterKey': 'Environment', 'ParameterValue': environment},
    ]

    # ── Create or update stack ────────────────
    status = get_stack_status(cfn, stack_name)

    cfn_kwargs = dict(
        StackName=stack_name,
        TemplateBody=template_body,
        Parameters=cfn_params,
        Capabilities=['CAPABILITY_NAMED_IAM'],
        Tags=[{'Key': 'Project', 'Value': 'ConnectAgentStateTracker'}],
    )

    if status is None:
        logger.info("Creating stack: %s", stack_name)
        cfn.create_stack(**cfn_kwargs)
        waiter_name = 'stack_create_complete'
    elif status in (
        'CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE',
    ):
        logger.info("Updating stack: %s (current status: %s)", stack_name, status)
        try:
            cfn.update_stack(**cfn_kwargs)
        except ClientError as e:
            if 'No updates are to be performed' in str(e):
                logger.info("No stack changes detected — skipping update.")
                waiter_name = None
            else:
                raise
        else:
            waiter_name = 'stack_update_complete'
    else:
        logger.error(
            "Stack is in state '%s' and cannot be updated. "
            "Resolve the stack state in the AWS Console first.",
            status,
        )
        sys.exit(1)

    # ── Wait for stack operation ──────────────
    if waiter_name:
        logger.info("Waiting for stack operation to complete...")
        try:
            waiter = cfn.get_waiter(waiter_name)
            waiter.wait(
                StackName=stack_name,
                WaiterConfig={'Delay': 10, 'MaxAttempts': 120},
            )
            logger.info("Stack operation completed successfully.")
        except WaiterError:
            logger.error("Stack operation failed.")
            print_stack_events(cfn, stack_name)
            sys.exit(1)

    # ── Package Lambda code ───────────────────
    logger.info("Packaging Lambda code...")
    zip_bytes = package_lambda_code()

    # ── Upload Lambda code ────────────────────
    function_name = f"{stack_name}-agent-state"
    upload_lambda_code(lam, function_name, zip_bytes)

    # ── Grant security profile access ─────────
    outputs = get_stack_outputs(cfn, stack_name)
    app_namespace = outputs.get('ApplicationNamespace', f"{stack_name}.state")

    print()
    logger.info("Granting security profile access to the service...")
    grant_security_profile_access(
        connect, connect_instance_arn, app_namespace
    )

    # ── Print summary ─────────────────────────
    print("\n" + "=" * 60)
    print("  DEPLOYMENT COMPLETE")
    print("=" * 60)
    print(f"  Stack:              {stack_name}")
    print(f"  Region:             {region}")
    print(f"  Function URL:       {outputs.get('FunctionUrl', 'N/A')}")
    print(f"  Lambda ARN:         {outputs.get('LambdaFunctionArn', 'N/A')}")
    print(f"  Application ARN:    {outputs.get('ApplicationArn', 'N/A')}")
    print(f"  App Namespace:      {app_namespace}")
    print(f"  Glue Database:      {outputs.get('GlueDatabaseName', 'N/A')}")
    print(f"  Glue Table:         {outputs.get('GlueTableName', 'N/A')}")
    print(f"  Athena Workgroup:   {outputs.get('AthenaWorkGroupName', 'N/A')}")
    print(f"  S3 Location:        {outputs.get('S3ParquetLocation', 'N/A')}")
    print("=" * 60)

    print("\n  The 3P service is registered and associated with your instance.")
    print("  If you skipped the security profile step, you can grant access")
    print("  manually in the Connect Console > Security Profiles > Applications.")
    print("  Once granted, agents will start tracking on their next login.\n")


# ──────────────────────────────────────────────
# SECURITY PROFILE REVOCATION
# ──────────────────────────────────────────────
def revoke_security_profile_access(connect_client, instance_arn, app_namespace):
    """
    Force-clear the 3P service from ALL security profiles.

    Connect caches application associations, so a targeted removal
    (checking describe then filtering) is unreliable.  Instead we
    clear the Applications list on every profile that contains our
    namespace, then sleep to let the change propagate before CFN
    tries to delete the integration association.
    """
    instance_id = instance_arn.split('/')[-1]
    revoked = 0

    try:
        profiles = []
        paginator = connect_client.get_paginator('list_security_profiles')
        for page in paginator.paginate(InstanceId=instance_id):
            profiles.extend(page['SecurityProfileSummaryList'])
    except ClientError as e:
        logger.warning("Could not list security profiles: %s", e)
        return

    for profile in profiles:
        try:
            resp = connect_client.describe_security_profile(
                SecurityProfileId=profile['Id'],
                InstanceId=instance_id,
            )
            apps = resp.get('SecurityProfile', {}).get('Applications', [])
            remaining = [a for a in apps if a.get('Namespace') != app_namespace]

            # Update if our app was found, or force-clear if apps
            # list is empty but Connect might still cache the grant
            if len(remaining) != len(apps) or not apps:
                connect_client.update_security_profile(
                    SecurityProfileId=profile['Id'],
                    InstanceId=instance_id,
                    Applications=remaining,
                )
                if len(remaining) != len(apps):
                    logger.info(
                        "Revoked access from profile: %s", profile['Name']
                    )
                    revoked += 1
        except ClientError as e:
            logger.warning(
                "Could not update profile '%s': %s", profile['Name'], e
            )

    if revoked > 0:
        logger.info(
            "Waiting 30 seconds for security profile changes to propagate..."
        )
        time.sleep(30)


# ──────────────────────────────────────────────
# DESTROY COMMAND
# ──────────────────────────────────────────────
def cmd_destroy():
    """Empty S3 buckets, revoke access, and delete the Agent State Tracker stack."""
    print("\n=== Agent State Tracker — Destroy ===\n")

    # ── Collect parameters ────────────────────
    region = prompt("AWS Region", default=boto3.Session().region_name or 'us-east-1')
    stack_name = prompt("Stack name (without env suffix)", default='agent-state-tracker')
    environment = prompt("Environment suffix (e.g., dev, prod, staging)", default='dev')
    stack_name = f"{stack_name}-{environment}"
    logger.info("Full stack name: %s", stack_name)
    connect_instance_arn = prompt(
        "Connect instance ARN"
    )
    print()

    # ── Build clients ─────────────────────────
    session = boto3.Session(region_name=region)
    cfn = session.client('cloudformation')
    s3 = session.client('s3')
    connect = session.client('connect')

    # ── Verify stack exists ───────────────────
    status = get_stack_status(cfn, stack_name)
    if status is None:
        logger.error("Stack '%s' does not exist in %s.", stack_name, region)
        sys.exit(1)

    logger.info("Found stack: %s (status: %s)", stack_name, status)

    # ── Find S3 buckets in the stack ──────────
    buckets = get_stack_s3_buckets(cfn, stack_name)
    if buckets:
        logger.info("S3 buckets owned by stack: %s", ', '.join(buckets))
    else:
        logger.info("No S3 buckets found in stack resources.")

    # ── Resolve app namespace ─────────────────
    app_namespace = f"{stack_name}.state"
    try:
        outputs = get_stack_outputs(cfn, stack_name)
        app_namespace = outputs.get('ApplicationNamespace', app_namespace)
    except ClientError:
        pass

    # ── Confirmation ──────────────────────────
    print()
    print("  WARNING: This will permanently delete:")
    print(f"    - CloudFormation stack: {stack_name}")
    print(f"    - Security profile grants for: {app_namespace}")
    for b in buckets:
        print(f"    - S3 bucket and all contents: {b}")
    print()

    confirmation = input(
        f"  Type the stack name to confirm destruction [{stack_name}]: "
    ).strip()

    if confirmation != stack_name:
        logger.info("Confirmation did not match. Aborting.")
        sys.exit(0)

    print()

    # ── Revoke security profile access ────────
    logger.info("Revoking security profile access...")
    revoke_security_profile_access(connect, connect_instance_arn, app_namespace)

    # ── Empty S3 buckets ──────────────────────
    for bucket_name in buckets:
        empty_s3_bucket(s3, bucket_name)

    # ── Clean up Athena workgroup ─────────────
    athena = session.client('athena')
    wg_name = f"{stack_name}-workgroup"
    try:
        athena.delete_work_group(WorkGroup=wg_name, RecursiveDeleteOption=True)
        logger.info("Deleted Athena workgroup: %s", wg_name)
    except ClientError as e:
        if 'WorkGroup is not found' not in str(e):
            logger.warning("Could not delete Athena workgroup: %s", e)

    # ── Delete stack ──────────────────────────
    logger.info("Deleting stack: %s", stack_name)
    cfn.delete_stack(StackName=stack_name)

    logger.info("Waiting for stack deletion to complete...")
    try:
        waiter = cfn.get_waiter('stack_delete_complete')
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120},
        )
    except WaiterError:
        logger.error("Stack deletion failed.")
        print_stack_events(cfn, stack_name)
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  STACK DESTROYED SUCCESSFULLY")
    print("=" * 60)
    print(f"  Stack:  {stack_name}")
    print(f"  Region: {region}")
    print(f"  Security profile access revoked for: {app_namespace}")
    for b in buckets:
        print(f"  Bucket deleted: {b}")
    print("=" * 60 + "\n")


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────
def main():
    command = sys.argv[1] if len(sys.argv) > 1 else 'deploy'
    command = command.lower().strip()

    if command == 'deploy':
        cmd_deploy()
    elif command == 'destroy':
        cmd_destroy()
    else:
        print(f"Unknown command: {command}")
        print("Usage: python deploy.py [deploy|destroy]")
        sys.exit(1)


if __name__ == '__main__':
    main()

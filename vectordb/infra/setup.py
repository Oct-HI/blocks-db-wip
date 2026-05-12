import configparser
import json
import os
import re
import subprocess
import time
from pathlib import Path

import boto3
from ..config import DEFAULT_INFRA_CONFIG, get_infra_config as _get_infra_config


LITHOPS_CONFIG_DIR = Path.home() / ".lithops"

_INFRA_CONFIG_CACHE = None

def get_infra_config(overrides: dict = None, use_cache: bool = True):
    """Get infrastructure config with optional CLI overrides.
    
    Priority: overrides > Python defaults
    """
    global _INFRA_CONFIG_CACHE
    if use_cache and _INFRA_CONFIG_CACHE is not None and overrides is None:
        config = _INFRA_CONFIG_CACHE
    else:
        config = DEFAULT_INFRA_CONFIG.__dict__.copy()
        if use_cache:
            _INFRA_CONFIG_CACHE = config
    if overrides:
        config.update(overrides)
    return config


def get_aws_credentials():
    creds_path = Path.home() / ".aws" / "credentials"
    config_path = Path.home() / ".aws" / "config"

    creds = {}
    profile = os.getenv("AWS_PROFILE") or "default"
    if creds_path.exists():
        cp = configparser.ConfigParser()
        cp.read(creds_path)
        if cp.has_section(profile):
            section = dict(cp.items(profile))
            creds["access_key_id"] = section.get("aws_access_key_id", "")
            creds["secret_access_key"] = section.get("aws_secret_access_key", "")
            creds["session_token"] = section.get("aws_session_token", "")

    region = ""
    if config_path.exists():
        cp = configparser.ConfigParser()
        cp.read(config_path)
        section_name = f"profile {profile}" if profile != "default" else "default"
        if cp.has_section(section_name):
            region = cp.get(section_name, "region", fallback="") or os.getenv("AWS_DEFAULT_REGION", "")

    return creds, region


def refresh_lithops_credentials():
    config_path = LITHOPS_CONFIG_DIR / "config"
    if not config_path.exists():
        print(f"No Lithops config found at {config_path}. Run 'blocks-db setup' first.")
        return False

    creds, region = get_aws_credentials()
    if not creds.get("access_key_id"):
        print("No credentials found in ~/.aws/credentials")
        return False

    with open(config_path) as f:
        content = f.read()

    def update_cred(match):
        key = match.group(1)
        new_val = creds.get(key, "")
        return f"  {key}: {new_val}"

    patterns = {
        r"  (access_key_id): .+": lambda m: update_cred(m),
        r"  (secret_access_key): .+": lambda m: update_cred(m),
        r"  (session_token): .+": lambda m: update_cred(m),
        r"  (region): .+": lambda m: f"  region: {region}" if region else m.group(0),
    }
    for pattern, repl in patterns.items():
        content = re.sub(pattern, repl, content, flags=re.MULTILINE)

    with open(config_path, "w") as f:
        f.write(content)

    print(f"Credentials refreshed in {config_path}")
    return True


def get_lambda_code(threshold_bytes=None):
    """Read Lambda code from file and substitute config placeholders."""
    config = get_infra_config()
    
    lambda_file = Path(__file__).parent / "lambda" / "lambda_code.py"
    
    if not lambda_file.exists():
        raise FileNotFoundError(f"Lambda code file not found: {lambda_file}")
    
    code = lambda_file.read_text()
    
    dynamodb_table = config.get("dynamodb_table_name", "BlocksDB-default")
    if threshold_bytes is None:
        threshold_bytes = config.get("threshold_size_bytes", 5242880)
    
    code = code.replace("__DYNAMODB_TABLE__", dynamodb_table)
    code = code.replace("__THRESHOLD_SIZE__", str(threshold_bytes))
    
    return code


def create_faiss_lambda_layer(layer_name=None, layer_version=None):
    """Build Lambda layer via Docker and publish to AWS."""
    import zipfile
    import tempfile
    import shutil
    
    config = get_infra_config()
    if layer_name is None:
        layer_name = config.get("layer_name", "blocksdb-layer-faiss-default")
    
    lambda_client = boto3.client("lambda")
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    region = boto3.Session().region_name or "us-east-1"
    
    ecr_repo = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{layer_name}"
    image_tag = f"{ecr_repo}:layer-build"
    
    dockerfile_content = '''FROM python:3.12-slim

RUN apt-get update && apt-get install -y \\
    g++ \\
    make \\
    cmake \\
    zip \\
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --target /layer/python/lib/python3.12/site-packages \\
    --platform manylinux2014_x86_64 \\
    --only-binary=:all: \\
    faiss-cpu==1.9.0.post1 \\
    "numpy<2" \\
    orjson
'''
    
    dockerfile = Path("/tmp") / f"Dockerfile.{layer_name}"
    dockerfile.write_text(dockerfile_content)
    
    print(f"  Building Docker image for layer...")
    build_result = subprocess.run(
        ["docker", "build", "-t", image_tag, "-f", str(dockerfile), "/tmp"],
        capture_output=True,
        text=True,
    )
    dockerfile.unlink()
    
    if build_result.returncode != 0:
        print(f"  Docker build failed: {build_result.stderr}")
        return None
    
    print(f"  Extracting layer from container...")
    
    container_id = subprocess.run(
        ["docker", "create", "--quiet", image_tag],
        capture_output=True,
        text=True,
    ).stdout.strip()
    
    try:
        subprocess.run(["docker", "cp", f"{container_id}:/layer", "/tmp/layer-extract"], check=True)
        
        layer_zip_path = Path("/tmp") / f"{layer_name}.zip"
        
        with zipfile.ZipFile(layer_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for path in Path("/tmp/layer-extract").rglob("*"):
                arcname = str(path.relative_to("/tmp/layer-extract"))
                if path.is_file():
                    zf.write(path, arcname)
                else:
                    zf.writestr(arcname + "/", "")
        
        shutil.rmtree("/tmp/layer-extract")
        subprocess.run(["docker", "rm", "-v", container_id], capture_output=True)
        
        with open(layer_zip_path, 'rb') as f:
            layer_content = f.read()
        
        layer_zip_path.unlink()
        
        print(f"  Layer zip size: {len(layer_content) / 1024 / 1024:.1f} MB")
        
        existing_version = None
        try:
            paginator = lambda_client.get_paginator('list_layer_versions')
            for page in paginator.paginate(LayerName=layer_name):
                versions = page.get('LayerVersions', [])
                if versions:
                    existing_version = versions[0]['Version']
                    print(f"  Found existing layer version: {existing_version}")
                    break
        except Exception as e:
            print(f"  No existing layer: {e}")
        
        try:
            response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Description="Blocks-DB FAISS layer with numpy for Python 3.12",
                Content={'ZipFile': layer_content},
                CompatibleRuntimes=['python3.12'],
                LicenseInfo='MIT'
            )
            new_version = response['Version']
            print(f"  ✓ Layer version {new_version} published")
        except Exception as e:
            print(f"  Error publishing layer: {e}")
            return None
        
        layer_arn = f"arn:aws:lambda:{region}:{account_id}:layer:{layer_name}:{new_version}"
        print(f"  Layer ARN: {layer_arn}")
        return layer_arn
        
    except Exception as e:
        print(f"  Error creating layer: {e}")
        return None


def get_or_create_lithops_execution_role():
    """Get or create the Lithops Lambda execution role."""
    iam = boto3.client("iam")
    role_name = "lambdaLithopsExecutionRole"
    
    try:
        response = iam.get_role(RoleName=role_name)
        return response["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        pass
    
    print(f"  Creating IAM role: {role_name}")
    
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": ["lambda.amazonaws.com", "events.amazonaws.com"]},
            "Action": "sts:AssumeRole"
        }]
    }
    
    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy),
        Description="Execution role for Lithops Lambda functions"
    )
    role_arn = role["Role"]["Arn"]
    
    import time
    time.sleep(5)
    
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    )
    
    return role_arn


def generate_lithops_config(region, bucket, runtime, execution_role=None):
    if execution_role is None or execution_role == "execution-role":
        execution_role = get_or_create_lithops_execution_role()
    
    creds, _ = get_aws_credentials()
    config = f"""lithops:
  backend: aws_lambda
  storage: aws_s3
  log_level: INFO

aws:
  region: {region}
  access_key_id: {creds.get('access_key_id', '')}
  secret_access_key: {creds.get('secret_access_key', '')}
  session_token: {creds.get('session_token', '')}

aws_lambda:
  runtime: {runtime}
  execution_role: {execution_role}

aws_s3:
  bucket_name: {bucket}
"""
    LITHOPS_CONFIG_PATH = LITHOPS_CONFIG_DIR / "config"
    LITHOPS_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LITHOPS_CONFIG_PATH, "w") as f:
        f.write(config)
    print(f"  Lithops config written to {LITHOPS_CONFIG_PATH}")
    print(f"  Execution role: {execution_role}")


def build_and_push_lambda_image(image_name=None):
    """Build and push Lambda Docker image to ECR."""
    config = get_infra_config()
    if image_name is None:
        image_name = config.get("lambda_function_name", "blocksdb-autoindexer-default")
    
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    region = boto3.Session().region_name or "us-east-1"
    
    ecr_client = boto3.client("ecr")
    ecr_repo_name = image_name
    ecr_repo_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{ecr_repo_name}"
    image_tag = f"{ecr_repo_uri}:latest"
    
    try:
        ecr_client.describe_repositories(repositoryNames=[ecr_repo_name])
        print(f"  ECR repository '{ecr_repo_name}' already exists")
    except ecr_client.exceptions.RepositoryNotFoundException:
        print(f"  Creating ECR repository '{ecr_repo_name}'...")
        ecr_client.create_repository(repositoryName=ecr_repo_name)
        print(f"  ✓ ECR repository created")
    
    dockerfile = Path(__file__).parent / "Dockerfile.autoindexer"
    
    if not dockerfile.exists():
        print(f"  Dockerfile not found: {dockerfile}")
        return None
    
    print(f"  Building Docker image for Lambda...")
    build_result = subprocess.run(
        ["docker", "build", "-t", image_name, "-f", str(dockerfile), str(dockerfile.parent)],
        capture_output=True,
        text=True,
    )
    
    if build_result.returncode != 0:
        print(f"  Docker build failed: {build_result.stderr}")
        return None
    
    print(f"  Tagging image as {image_tag}...")
    subprocess.run(["docker", "tag", image_name, image_tag], check=True)
    
    print(f"  Pushing image to ECR...")
    push_result = subprocess.run(["docker", "push", image_tag], capture_output=True, text=True)
    if push_result.returncode != 0:
        print(f"  Push failed: {push_result.stderr}")
        return None
    
    print(f"  ✓ Lambda image pushed: {image_tag}")
    return image_tag


def build_and_push_runtime(dockerfile, runtime_name):
    print(f"Building and pushing Lithops runtime: {runtime_name}")

    result = subprocess.run(
        ["lithops", "runtime", "build", "-f", dockerfile, "-b", "aws_lambda", runtime_name],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error building runtime:\n{result.stderr}")
        raise RuntimeError(f"Failed to build runtime: {result.stderr}")

    print(f"Runtime {runtime_name} built and pushed successfully.")
    return runtime_name


def ecr_login(region):
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]

    login_cmd = f"aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {account_id}.dkr.ecr.{region}.amazonaws.com"
    result = subprocess.run(login_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"ECR login warning (may already be logged in): {result.stderr}")
    else:
        print("Logged into ECR successfully.")


def run_setup(s3_bucket, config_overrides=None, create_vector_table=True, build_runtime=True):
    """
    Setup Blocks-DB infrastructure.
    
    Args:
        s3_bucket: S3 bucket for storage
        config_overrides: Optional dict to override config values
        create_vector_table: Create DynamoDB table if not exists
        build_runtime: Build and push Lithops runtime (default: True)
    """
    config = get_infra_config(overrides=config_overrides)

    lambda_function_name = config.get("lambda_function_name")
    runtime_name = config.get("runtime_name")
    threshold_bytes = config.get("threshold_size_bytes")
    
    # template_path not used - setup creates resources manually
    
    region = boto3.Session().region_name or "us-east-1"
    
    steps = [
        ("Setting up infrastructure", False),
        ("Building Lambda layer with dependencies", False),
        ("Creating Lambda with code + layer", False),
        ("Configuring S3 trigger", False),
        ("Creating DynamoDB table", False),
        ("Logging into ECR", False),
        ("Generating Lithops config", False),
        ("Building runtime", False),
    ]
    
    def show_progress(current_step_idx):
        print("\033[2K\033[G\033[J", end="")
        print("\n" + "=" * 50)
        for i, (name, done) in enumerate(steps):
            status = "✓" if done else ("▶" if i == current_step_idx else " ")
            print(f"  [{status}] {name}")
        print("=" * 50)
    
    print(f"\nSetting up Blocks-DB infrastructure...")
    print(f"  S3 Bucket: {s3_bucket}")
    print(f"  Region: {region}")

    lambda_arn = None
    execution_role = None
    layer_arn = None
    
    steps[0] = (steps[0][0], True)
    show_progress(1)
    
    print("  Building Lambda layer with dependencies...")
    try:
        layer_arn = create_faiss_lambda_layer()
        if layer_arn:
            print(f"  ✓ Lambda layer created: {layer_arn}")
        else:
            print("  ⚠ Could not create Lambda layer")
    except Exception as e:
        print(f"  Warning building layer: {e}")
    
    steps[1] = (steps[1][0], True)
    show_progress(2)
    
    print("  Creating Lambda with code + layer...")
    try:
        lambda_arn = create_lambda_with_code_and_layer(s3_bucket, layer_arn, lambda_function_name)
        if lambda_arn:
            print(f"  ✓ Lambda created: {lambda_arn}")
    except Exception as e:
        print(f"  Warning creating Lambda: {e}")
    
    steps[2] = (steps[2][0], True)
    show_progress(3)
    
    if lambda_arn:
        print("  Configuring S3 bucket notification...")
        try:
            configure_s3_notification(s3_bucket, lambda_arn, lambda_function_name)
            print("  ✓ S3 trigger configured")
        except Exception as e:
            print(f"  Warning: {e}")
        steps[3] = (steps[3][0], True)
    show_progress(4)
    
    if create_vector_table:
        print("  Ensuring DynamoDB table exists...")
        create_vector_index_table(s3_bucket)
        steps[4] = (steps[4][0], True)
    show_progress(5)
    
    print("  Logging into ECR...")
    try:
        ecr_login(region)
        print("  ✓ Logged into ECR")
    except Exception as e:
        print(f"  Warning: {e}")
    steps[5] = (steps[5][0], True)
    show_progress(6)
    
    print("  Generating Lithops config...")
    generate_lithops_config(region, s3_bucket, runtime_name, execution_role)
    steps[6] = (steps[6][0], True)
    show_progress(7)
    
    if build_runtime:
        dockerfile = Path(__file__).parent / "Dockerfile.lambda"
        if dockerfile.exists():
            print("  Building Lithops runtime...")
            try:
                build_and_push_runtime(str(dockerfile), runtime_name)
                print("  ✓ Runtime built and pushed")
            except Exception as e:
                print(f"  Runtime build failed: {e}")
    
    steps[7] = (steps[7][0], True)
    show_progress(len(steps) - 1)
    
    print(f"\n✓ Setup complete!")
    print(f"\nNext steps:")
    print(f"  1. blocks-db configure --bucket {s3_bucket} --region {region}")
    print(f"  2. blocks-db initialize-database mydata vectors.csv --config config.json")
    print(f"  3. blocks-db put mydata new_vectors.csv  (add more vectors)")
    print(f"  4. blocks-db query mydata --file queries.csv  (search)")


def create_lambda_manually(s3_bucket, layer_arn=None, function_name=None):
    """Create the auto-indexer Lambda function manually."""
    import base64
    import zipfile
    from io import BytesIO
    
    config = get_infra_config()
    if function_name is None:
        function_name = config.get("lambda_function_name", "blocks-db-auto-indexer")
    
    lambda_client = boto3.client("lambda")
    iam_client = boto3.client("iam")
    
    try:
        lambda_client.get_function(FunctionName=function_name)
        print(f"  Lambda '{function_name}' already exists")
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        return response["FunctionArn"]
    except lambda_client.exceptions.ResourceNotFoundException:
        pass
    
    print(f"  Creating Lambda function '{function_name}'...")
    
    role_name = config.get("lambda_role_name", f"{function_name}-role")
    try:
        role = iam_client.get_role(RoleName=role_name)
        role_arn = role["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        print(f"  Creating IAM role...")
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        role_arn = role["Role"]["Arn"]
        
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

        print("  Waiting for IAM role propagation...")
        time.sleep(10)

    print(f"  Reading Lambda code from file...")
    threshold_bytes = config.get("threshold_size_bytes", 5242880)
    lambda_code = get_lambda_code(threshold_bytes)
    
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('index.py', lambda_code)
    zip_data = zip_buffer.getvalue()
    
    dynamodb_table = config.get("dynamodb_table_name", "BlocksDB-default")
    
    kwargs = {
        'FunctionName': function_name,
        'Runtime': config.get("lambda_runtime", "python3.13"),
        'Role': role_arn,
        'Handler': 'index.lambda_handler',
        'Code': {'ZipFile': zip_data},
        'Description': 'Auto-indexer for Blocks-DB vector database',
        'Timeout': config.get("lambda_timeout_seconds", 900),
        'MemorySize': config.get("lambda_memory_mb", 10240),
        'Environment': {
            'Variables': {
                'DYNAMODB_TABLE': dynamodb_table,
                'THRESHOLD_SIZE_BYTES': str(threshold_bytes)
            }
        }
    }
    
    if layer_arn:
        kwargs['Layers'] = [layer_arn]
        print(f"  Attaching layer: {layer_arn}")
    
    try:
        response = lambda_client.create_function(**kwargs)
        print(f"  Lambda created: {response['FunctionName']}")
        return response['FunctionArn']
    except lambda_client.exceptions.ResourceConflictException:
        print(f"  Lambda already exists")
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        return response['FunctionArn']


def create_lambda_from_image(s3_bucket, image_uri, function_name=None):
    """Create Lambda function from Docker image."""
    config = get_infra_config()
    if function_name is None:
        function_name = config.get("lambda_function_name", "blocksdb-autoindexer-default")
    
    if not image_uri:
        print("  No image URI provided, cannot create Lambda from image")
        return None
    
    lambda_client = boto3.client("lambda")
    iam_client = boto3.client("iam")
    
    try:
        lambda_client.get_function(FunctionName=function_name)
        print(f"  Lambda '{function_name}' already exists")
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        return response["FunctionArn"]
    except lambda_client.exceptions.ResourceNotFoundException:
        pass
    
    print(f"  Creating Lambda function '{function_name}' from image...")
    
    role_name = config.get("lambda_role_name", f"{function_name}-role")
    try:
        role = iam_client.get_role(RoleName=role_name)
        role_arn = role["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        print(f"  Creating IAM role...")
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        role_arn = role["Role"]["Arn"]
        
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
    
    dynamodb_table = config.get("dynamodb_table_name", "BlocksDB-default")
    threshold_bytes = config.get("threshold_size_bytes", 5242880)
    
    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            PackageType='Image',
            Code={'ImageUri': image_uri},
            Role=role_arn,
            Timeout=config.get("lambda_timeout_seconds", 900),
            MemorySize=config.get("lambda_memory_mb", 10240),
            ImageConfig={'Command': ['lambda_handler.lambda_handler']},
            Environment={
                'Variables': {
                    'DYNAMODB_TABLE': dynamodb_table,
                    'THRESHOLD_SIZE_BYTES': str(threshold_bytes)
                }
            }
        )
        print(f"  Lambda created from image: {response['FunctionName']}")
        return response['FunctionArn']
    except lambda_client.exceptions.ResourceConflictException:
        print(f"  Lambda already exists")
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        return response['FunctionArn']


def create_lambda_with_code_and_layer(s3_bucket, layer_arn=None, function_name=None, threshold_bytes=None):
    """Create Lambda function with code zip file and attach a layer."""
    import zipfile
    from io import BytesIO
    
    config = get_infra_config()
    if function_name is None:
        function_name = config.get("lambda_function_name", "blocksdb-autoindexer-default")
    if threshold_bytes is None:
        threshold_bytes = config.get("threshold_size_bytes", 5242880)
    
    lambda_client = boto3.client("lambda")
    iam_client = boto3.client("iam")
    
    try:
        lambda_client.get_function(FunctionName=function_name)
        print(f"  Lambda '{function_name}' already exists, updating code...")
        return update_lambda_code(function_name, layer_arn)
    except lambda_client.exceptions.ResourceNotFoundException:
        pass
    
    print(f"  Creating Lambda function '{function_name}' with code + layer...")
    
    role_name = config.get("lambda_role_name", f"{function_name}-role")
    try:
        role = iam_client.get_role(RoleName=role_name)
        role_arn = role["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        print(f"  Creating IAM role...")
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        role_arn = role["Role"]["Arn"]
        
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
        )
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

        print("  Waiting for IAM role propagation...")
        time.sleep(10)

    print(f"  Reading Lambda code from file...")
    lambda_code = get_lambda_code(threshold_bytes)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('index.py', lambda_code)
    zip_data = zip_buffer.getvalue()

    dynamodb_table = config.get("dynamodb_table_name", "BlocksDB-default")
    
    kwargs = {
        'FunctionName': function_name,
        'Runtime': config.get("lambda_runtime", "python3.13"),
        'Role': role_arn,
        'Handler': 'index.lambda_handler',
        'Code': {'ZipFile': zip_data},
        'Description': 'Auto-indexer for Blocks-DB vector database',
        'Timeout': config.get("lambda_timeout_seconds", 900),
        'MemorySize': config.get("lambda_memory_mb", 10240),
        'Environment': {
            'Variables': {
                'DYNAMODB_TABLE': dynamodb_table,
                'THRESHOLD_SIZE_BYTES': str(threshold_bytes),
                'INDEX_IMPLEMENTATION': 'blocks'
            }
        }
    }
    
    if layer_arn:
        kwargs['Layers'] = [layer_arn]
        print(f"  Attaching layer: {layer_arn}")
    
    try:
        response = lambda_client.create_function(**kwargs)
        print(f"  Lambda created with code + layer: {response['FunctionName']}")
        return response['FunctionArn']
    except lambda_client.exceptions.ResourceConflictException:
        print(f"  Lambda already exists, updating code...")
        return update_lambda_code(function_name, layer_arn, threshold_bytes)


def update_lambda_code(function_name, layer_arn=None, threshold_bytes=None):
    """Update Lambda function code (and optionally layer)."""
    import zipfile
    from io import BytesIO
    
    config = get_infra_config()
    runtime = config.get("lambda_runtime", "python3.12")
    
    lambda_client = boto3.client("lambda")
    
    print(f"  Updating Lambda code for '{function_name}'...")
    
    try:
        current = lambda_client.get_function_configuration(FunctionName=function_name)
        if current.get("Runtime") != runtime:
            print(f"  Updating runtime from {current.get('Runtime')} to {runtime}")
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Runtime=runtime
            )
    except Exception as e:
        print(f"  Warning checking runtime: {e}")
    
    lambda_code = get_lambda_code(threshold_bytes)
    
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('index.py', lambda_code)
    zip_data = zip_buffer.getvalue()
    
    try:
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_data
        )
        print(f"  Lambda code updated")
    except Exception as e:
        print(f"  Warning updating code: {e}")
    
    if layer_arn:
        try:
            response = lambda_client.get_function_configuration(FunctionName=function_name)
            current_layers = response.get("Layers", [])
            current_layer_arns = [l.split(":")[6] for l in current_layers if l.startswith("arn:aws:lambda")]
            layer_name = layer_arn.split(":")[6]
            
            if layer_name not in current_layer_arns:
                new_layers = current_layers + [layer_arn]
                lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Layers=new_layers
                )
                print(f"  Layer attached: {layer_arn}")
            else:
                print(f"  Layer already attached")
        except Exception as e:
            print(f"  Warning updating layer: {e}")
    
    response = lambda_client.get_function_configuration(FunctionName=function_name)
    return response['FunctionArn']


def deploy_lambda_code(function_name=None):
    """Deploy only the Lambda code (update the function)."""
    config = get_infra_config()
    if function_name is None:
        function_name = config.get("lambda_function_name", "blocksdb-autoindexer-default")
    
    print(f"Deploying Lambda code: {function_name}")
    
    lambda_client = boto3.client("lambda")
    
    try:
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        layers = response.get("Layers", [])
        
        layer_arn = layers[0] if layers else None
        
        return update_lambda_code(function_name, layer_arn)
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"  Lambda '{function_name}' not found. Run setup first.")
        return None




def configure_s3_notification(s3_bucket, lambda_arn, function_name=None):
    """Configure S3 bucket to trigger Lambda on CSV uploads to pending/ folder only."""
    config = get_infra_config()
    if function_name is None:
        function_name = config.get("lambda_function_name", "blocksdb-autoindexer-default")
    
    lambda_client = boto3.client("lambda")
    s3_client = boto3.client("s3")
    
    try:
        statement_id = f"{function_name}-s3-trigger"
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{s3_bucket}",
            SourceAccount=boto3.client("sts").get_caller_identity()["Account"]
        )
        print(f"  Added S3 invoke permission")
    except lambda_client.exceptions.ResourceConflictException:
        print(f"  S3 invoke permission already exists")
    except Exception as e:
        print(f"  Warning adding permission: {e}")
    
    try:
        notification_config = {
            'LambdaFunctionConfigurations': [{
                'Id': f'{function_name}-trigger',
                'LambdaFunctionArn': lambda_arn,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': 'pending/'},
                            {'Name': 'suffix', 'Value': '.csv'}
                        ]
                    }
                }
            }]
        }
        s3_client.put_bucket_notification_configuration(
            Bucket=s3_bucket,
            NotificationConfiguration=notification_config,
            SkipDestinationValidation=True
        )
        print(f"  S3 notification configured for bucket: {s3_bucket}")
        print(f"  -> Triggers on: pending/*.csv")
    except Exception as e:
        print(f"  Warning: Could not configure S3 notification: {e}")
        print(f"  You may need to configure manually or add trigger via AWS console")


def deploy_runtime(runtime_name="blocks-db-runtime"):
    """Deploy/Redeploy the Lithops runtime."""
    region = boto3.Session().region_name or "us-east-1"
    
    dockerfile = Path(__file__).parent / "Dockerfile.lambda"
    if not dockerfile.exists():
        print(f"Dockerfile not found at {dockerfile}")
        return False
    
    print(f"Deploying runtime: {runtime_name}")
    
    try:
        ecr_login(region)
        build_and_push_runtime(str(dockerfile), runtime_name)
        print(f"Runtime deployed successfully!")
        return True
    except Exception as e:
        print(f"Runtime deployment failed: {e}")
        return False


def update_lambda_threshold(threshold_bytes: int = None, dataset_name: str = None, s3_bucket: str = None):
    """Update the auto-indexer Lambda threshold.
    
    Args:
        threshold_bytes: New threshold in bytes (optional, auto-calculated if dataset_name provided)
        dataset_name: If provided, read config from S3 and calculate threshold
        s3_bucket: S3 bucket (required if dataset_name provided)
    """
    lambda_client = boto3.client("lambda")
    config = get_infra_config()
    
    function_name = config.get("lambda_function_name")
    
    if dataset_name and s3_bucket and threshold_bytes is None:
        config_key = f"indexes/{dataset_name}/blocks/config.json"
        try:
            import urllib.request
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=s3_bucket, Key=config_key)
            index_config = json.loads(response['Body'].read())
            num_index = index_config.get("num_index", 16)
            features = index_config.get("features", 96)
            bytes_per_vector = 8 + (features * 8)
            total_vectors = index_config.get("total_vectors", 100000)
            vectors_per_block = total_vectors // num_index
            threshold_bytes = vectors_per_block * bytes_per_vector
            print(f"Calculated from index config: {vectors_per_block} vectors/block x {bytes_per_vector} bytes = {threshold_bytes} bytes")
        except Exception as e:
            print(f"Could not read index config: {e}")
            if threshold_bytes is None:
                threshold_bytes = config.get("threshold_size_bytes", 5242880)
                print(f"Using default: {threshold_bytes} bytes")
    
    if threshold_bytes is None:
        threshold_bytes = config.get("threshold_size_bytes", 5242880)
    
    try:
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        current_env = response.get("Environment", {}).get("Variables", {})
        current_threshold = current_env.get("THRESHOLD_SIZE_BYTES", str(config.get("threshold_size_bytes", 5242880)))
        
        print(f"Current threshold: {int(current_threshold):,} bytes")
        print(f"New threshold: {threshold_bytes:,} bytes")
        
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={
                "Variables": {
                    **current_env,
                    "THRESHOLD_SIZE_BYTES": str(threshold_bytes)
                }
            }
        )
        print(f"Threshold updated successfully!")
        return True
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"Lambda function '{function_name}' not found.")
        print("Make sure setup has been run.")
        return False
    except Exception as e:
        print(f"Failed to update threshold: {e}")
        return False


def create_infrastructure_manually(s3_bucket, create_vector_table=True):
    """Create infrastructure components manually if CloudFormation fails."""
    region = boto3.Session().region_name or "us-east-1"
    
    print("\nCreating infrastructure manually...")
    
    if create_vector_table:
        print("\n1. Creating DynamoDB table...")
        create_vector_index_table(s3_bucket)
    
    print("\n2. Creating S3 prefixes...")
    s3_client = boto3.client("s3")
    prefixes = ["pending/", "inputs/", "indexes/"]
    for prefix in prefixes:
        try:
            s3_client.put_object(Bucket=s3_bucket, Key=prefix, Body=b"")
            print(f"  Created: {prefix}")
        except Exception as e:
            print(f"  Warning creating {prefix}: {e}")
    
    print("\n3. Generating Lithops config...")
    generate_lithops_config(region, s3_bucket, "blocks-db-runtime", "execution-role")
    
    print("\n" + "=" * 50)
    print("Manual setup complete!")
    print("=" * 50)
    print(f"\nNote: Auto-indexing Lambda needs to be deployed separately.")
    print(f"Use the CloudFormation template for full auto-indexer support.")


def create_vector_index_table(bucket=None):
    """Create the DynamoDB table for vector index tracking."""
    config = get_infra_config()
    dynamodb = boto3.resource("dynamodb")
    s3_client = boto3.client("s3")
    
    table_name = config.get("dynamodb_table_name", "BlocksDB-default")
    
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "centroid_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "centroid_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        print(f"  Created DynamoDB table: {table_name}")
        
        table.put_item(Item={
            "centroid_id": "GLOBAL",
            "sk": "META",
            "current_centroid_id": 0,
            "current_accumulated_size": 0,
        })
        print(f"  Initialized global metadata")
        
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        print(f"  DynamoDB table already exists: {table_name}")
    except Exception as e:
        print(f"  Warning: Could not create DynamoDB table: {e}")
    
    if bucket:
        try:
            s3_client.put_object(Bucket=bucket, Key="pending/", Body=b"")
            s3_client.put_object(Bucket=bucket, Key="inputs/", Body=b"")
            print(f"  Created S3 prefixes for bucket: {bucket}")
        except Exception as e:
            print(f"  Warning: Could not create S3 prefixes: {e}")

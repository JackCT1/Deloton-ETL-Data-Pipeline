<img src="https://user-images.githubusercontent.com/5181870/188019461-4a27a045-9301-4931-910c-b367f7b2709a.png" alt="Deloton banner">

# Case Study: Deloton

## Data Pipeline

<img src="https://user-images.githubusercontent.com/105182364/195573143-313b8527-dfa3-406f-93f3-4ad556e1bfb0.png" alt="Data Pipeline">

### Ingestion

Extracts logs from a real-time Kafka data stream

- Finds the start of a ride log
- Connects to AWS Aurora PostgreSQL
- Loads data into a staging schema

Automation

- Dockerises files + dependencies
- Runs on an AWS EC2 instance
- Sends end of ride log notifications to an AWS SNS topic
- Sends abnormalities as emails using AWS SES

### Transformation

Reads data from the staging schema

- Cleans & transforms data
- Loads data into a production schema for queries

Automation

- Dockerises files + dependencies
- Connects topic input from AWS SNS topic

### Dashboard

Dash app + pages

- Loads data from the production schema
- Visualises queries in plotly
- Updates data using Dash Live Components

Automation

- Dockerise files + dependencies
- Runs on an AWS EC2 instance

### API

http://3.10.180.227:5001/

Flask app

- Loads data from the production schema
- Model schema using class models
- HTTP CRUD Endpoints

Automation

- Dockerise files + dependencies
- Runs on an AWS EC2 instance

### Report

Daily Email

- Loads data from the production schema
- Visualises queries in plotly
- Sends email in HTML format using AWS SES

Automation

- Dockerise files + dependencies
- Runs on AWS Lambda function
- Daily CloudWatch event trigger at 17:00

## Setup: Docker

### View Images

```Bash
docker images
```

### Local

_Build_

```Bash
docker build . -t {IMAGE_NAME}
```

_Run_

```Bash
docker run -d {IMAGE_NAME}
```

### AWS

_Build_

```Bash
docker build . -t {IMAGE_NAME} --platform "linux/amd64"
```

_Credentials_

```Bash
aws ecr get-login-password --region {REGION} | docker login --username AWS --password-stdin {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com
```

_Tag_

```Bash
docker tag {IMAGE_ID} {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:{IMAGE_NAME}-{TAG}
```

_Push_

```Bash
docker push {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:{IMAGE_NAME}-{TAG}
```

### EC2 SSH

_Copy .env_

```Bash
scp -i {PATH_TO_PEM} {PATH_TO_ENV} ec2-user@{INSTANCE_IP}:/home/ec2-user
```

_Access sudo_

```Bash
ssh -i {PATH_TO_PEM} ec2-user@{INSTANCE_IP}
sudo yum update
```

_Install docker_

```Bash
sudo yum install docker
sudo systemctl enable docker.service
sudo systemctl start docker.service
```

_Pull from AWS_

```Bash
aws ecr get-login-password --region {REGION} | sudo docker login --username AWS --password-stdin {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com

sudo docker pull {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:{IMAGE_NAME}-{TAG}

screen

sudo docker run -d -p 8080:8080 --env-file ./.env {AWS_ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:{IMAGE_NAME}-{TAG}
```

## Dependencies

- boto3
- botocore
- confluent-kafka
- dash
- dash-bootstrap-components
- Flask
- Flask-SQLAlchemy
- flask-cors
- kaleido
- numpy
- pandas
- plotly
- psycopg2-binary
- pyarrow
- python-dotenv
- SQLAlchemy

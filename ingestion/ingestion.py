from datetime import date, datetime
import json
import logging
import os
import re
import uuid

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
import sqlalchemy

def get_logger(log_level: str) -> logging.Logger:
    """
    Returns:
    - formatted logger
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s: %(levelname)s: %(message)s'
    )
    logger = logging.getLogger()                    
    return logger

def start_consumer() -> Consumer:
    """
    Connects consumer to Kafka topic

    Returns:
    - consumer connection
    """
    return Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'zuckerberg-3',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 1000,
        'fetch.wait.max.ms': 6000,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '86400000',
        'topic.metadata.refresh.interval.ms': "-1",
        "client.id": 'id-002-005',
    })

def create_user_row(user_id: int, user_dict: dict) -> dict:
    """
    Extracts relevant keys from user_dict
    
    Returns:
    - updated user dictionary
    """
    name = user_dict['name'].split(' ')
    gender = user_dict['gender']
    postcode_match = re.search(r'([A-Z]{1,2}\d{1,2}\s\d{1}[A-Z]{2})', user_dict['address'])
    postcode = postcode_match.group() if postcode_match else None
    date_of_birth = datetime.utcfromtimestamp(int(user_dict['date_of_birth'])/1000).strftime('%Y-%m-%d %H:%M:%S')
    email = user_dict['email_address']
    height = user_dict['height_cm']
    weight = user_dict['weight_kg']
    account_creation = datetime.utcfromtimestamp(int(user_dict['account_create_date'])/1000).strftime('%Y-%m-%d %H:%M:%S')

    user = {
        'user_id': user_id,
        'first_name': name[0],
        'last_name': name[1],
        'gender': gender,
        'postcode': postcode,
        'date_of_birth': date_of_birth,
        'email': email,
        'height_cm': height,
        'weight_kg': weight,
        'account_creation': account_creation
    }
    return user

def create_user_ride_row(user_id: int, ride_id: str) -> dict:
    """
    Row to track each ride
    
    Returns:
    - dictionary with user_id & ride_id
    """
    user_ride_dict = {
        'user_id': user_id,
        'ride_id': ride_id
    }
    return user_ride_dict

def create_metrics_row(ride_id: str, ride_metrics: dict, telemetry_metrics: dict) -> dict:
    """
    Extracts relevant keys from metrics pairs:
    - Ride
    - Telemetry
    
    Returns:
    - updated metrics dictionary
    """
    time = re.search(r'(\d|\-|\.)+\s(\d{2}:){2}\d{2}', ride_metrics['log']).group()
    bike_model = re.search(r'.\d+\s(\w+\sv\d+)', ride_metrics['log']).group(1)
    duration = float(re.search(r'duration = (\d+\.\d*)', ride_metrics['log']).group(1))
    resistance = int(re.search(r'resistance = (\d+)', ride_metrics['log']).group(1))
    heart_rate =  int(re.search(r'hrt = (\d+)', telemetry_metrics['log']).group(1))
    rpm = int(re.search(r'rpm = (\d+)', telemetry_metrics['log']).group(1))
    power = float(re.search(r'power = (\d+\.\d*)', telemetry_metrics['log']).group(1))

    metrics_dict = {
        'ride_id': ride_id,
        'time': time,
        'bike_model': bike_model,
        'duration_seconds': duration,
        'resistance': resistance,
        'heart_rate': heart_rate,
        'rpm': rpm,
        'power': power
    }
    return metrics_dict

def insert_user_row(user: dict) -> None:
    """
    Insert user dictionary into staging user_table
    """
    try:
        with engine.connect() as con:
            con.execute(
                """
                INSERT INTO zuckerberg_staging.user_table
                (user_id, first_name, last_name, gender, postcode, date_of_birth, email, height_cm, weight_kg, account_creation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                (first_name, last_name, gender, postcode, date_of_birth, email, height_cm, weight_kg, account_creation) =
                (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.postcode, EXCLUDED.date_of_birth, EXCLUDED.email, EXCLUDED.height_cm, EXCLUDED.weight_kg, EXCLUDED.account_creation)
                """,
                (
                    user['user_id'],
                    user['first_name'],
                    user['last_name'],
                    user['gender'],
                    user['postcode'],
                    user['date_of_birth'],
                    user['email'],
                    user['height_cm'],
                    user['weight_kg'],
                    user['account_creation']
                )
            )
    except Exception as e:
        logging.error(f"Error raised whilst inserting user row: {e}")


def insert_user_ride_row(user_ride: dict) -> None:
    """
    Insert user_ride dictionary into staging user_ride
    """
    try:
        with engine.connect() as con:
            con.execute(
                'INSERT INTO zuckerberg_staging.user_ride VALUES (%s, %s)',
                (
                    user_ride['user_id'],
                    user_ride['ride_id']
                )
            )
    except Exception as e:
        logging.error(f"Error raised whilst inserting user_ride row: {e}")


def insert_metrics_row(metrics: dict) -> None:
    """
    Insert metrics dictionary into staging_schema metrics_table
    """
    try:
        with engine.connect() as con:
            con.execute(
                'INSERT INTO zuckerberg_staging.metrics_table VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (
                    metrics['ride_id'],
                    metrics['time'],
                    metrics['bike_model'],
                    metrics['duration_seconds'],
                    metrics['resistance'],
                    metrics['heart_rate'],
                    metrics['rpm'],
                    metrics['power']
                )
            )
    except Exception as e:
        logging.error(f"Error raised whilst inserting metrics row: {e}")

def calculate_age(date_of_birth: int) -> int:
    """
    Calculates user age from date_of_birth, which is in UNIX milliseconds
    
    Returns:
    - age (years)
    """
    today = date.today()

    dob = datetime.utcfromtimestamp(date_of_birth/1000).strftime('%Y-%m-%d %H:%M:%S')
    year = datetime.strptime(dob, '%Y-%m-%d %H:%M:%S').date().year
    month = datetime.strptime(dob, '%Y-%m-%d %H:%M:%S').date().month
    day = datetime.strptime(dob, '%Y-%m-%d %H:%M:%S').date().day

    diff_years = today.year - year
    is_before_birthday = (today.month, today.day) < (month, day)
    age = diff_years - is_before_birthday
    return age


def check_heart_rate(age: int, heart_rate: int) -> bool:
    """
    Checks heart_rate against safe range
    - age (years)
    - heart_rate (bpm)

    Returns:
    - True if Safe
    - False if Unsafe
    """
    max_heart_rate = 220 - age
    lower_limit = max_heart_rate * 0.5
    upper_limit = max_heart_rate * 0.7

    is_safe = lower_limit <= heart_rate <= upper_limit or heart_rate == 0
    return is_safe


def create_heart_rate_alert(user_name: str, heart_rate: int) -> dict:
    """
    Creates personalised SES message for unsafe heart_rate
    Uses HTML formatting

    Returns:
    - message dictionary
    """
    charset = "UTF-8"

    html = f"""
            <html>
                <h2 style="text-align: center;"><span style="color: #ff0000;">Deloton heart rate alert!</span></h2>
                <p><span style="color: #000000;">Dear {user_name},</span></p>
                <p><strong>Whilst riding your Deloton bike, you heart rate was recorded as: <span style="color: #ff0000;">{heart_rate} bpm</span></strong></p>
                <p>This is outside the safe range that we've calculated given your age and weight.</p>
                <p>If you start to feel unwell, call the emergency services.</p>
                <p>From the Deloton Customer Alerts Team.</p>
                <p><img src="https://user-images.githubusercontent.com/5181870/188019461-4a27a045-9301-4931-910c-b367f7b2709a.png" alt="fullwidth" width="302" height="112" /></p>
            </html>
        """
    message = {
                "Body": {
                    "Html": {
                        "Charset": charset,
                        "Data": html,
                    }
                },
                "Subject": {
                    "Charset": charset,
                    "Data": "DELOTON HEART RATE ALERT",
                },
            }
    return message


def sends_heart_rate_alert(message: dict, sender_address: str, recipient_address: str) -> bool:
    """
    Connects to SES via boto3 client
    Sends SES message for unsafe heart_rate

    Returns:
    - True if successfully sent
    - False if error occurred in attempting to send email
    """
    try:
        ses_client = boto3.client('ses', REGION)

        ses_client.send_email(
            Destination={
                "ToAddresses": [
                    recipient_address,
                ],
            },
            Message=message,
            Source=sender_address,
        )
        return True
    except Exception as e:
        logging.error("""
            Error occurred whilst trying to send email FROM %s
            TO %s with SES:
            %s
            """, sender_address, recipient_address, e)
        return False


def get_heart_rate_info(name: str, age: int, email: str, heart_rate: int) -> None:
    """
    Calculates safe heart_rate range
    Sends SES if unsafe
    """
    try:
        log.info("NAME: %s, HRT: %s, AGE: %s", name, heart_rate, age)
        
        if not check_heart_rate(age, heart_rate):
            message = create_heart_rate_alert(name, heart_rate)
            message_sent = sends_heart_rate_alert(message, SES_SENDER_ADDRESS, email)
            log.info("""
                HEART-RATE %s FOR AGE %s UNSAFE
                EMAIL TO %s SENT: %s""",
                heart_rate, age, email, message_sent
            )
    except Exception as e:
        log.error("Heart Rate Alert Error: %s", e)

def publish_message(topic_arn: str, message: str, subject: str) -> str:
    """
    Publishes message to SNS topic and triggers production script
    """
    try:

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject,
        )['MessageId']

    except ClientError:
        log.exception(f'Could not publish message to the topic')
        raise
    else:
        return response

if __name__ == '__main__':
    
    load_dotenv()
    REGION = os.getenv('REGION')

    KAFKA_SERVER = os.getenv('KAFKA_SERVER')
    KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
    KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')

    SES_SENDER_ADDRESS = os.getenv('SES_SENDER_ADDRESS')
    ZUCK_TOPIC = os.getenv('ZUCK_TOPIC')
    STAGING_SCHEMA = 'zuckerberg_staging'

    log = get_logger(logging.INFO)
    sns_client = boto3.client('sns', REGION)
    engine = sqlalchemy.create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

try:
        c = start_consumer()
        c.subscribe([KAFKA_TOPIC])

        user_info = {}
        user_id = ''
        ride_id = ''
        metrics_pair = []
        
        new_ride = False
        while True:
            msg = c.poll()
            if msg is None:
                continue
            elif msg.error():
                print(f"CONSUMER ERROR: {msg.error()}")
            
            elif new_ride == True:
                msg_value = json.loads(msg.value().decode('utf-8'))
                
                if '[SYSTEM]' in msg_value['log']:
                    user_info = json.loads(re.search(r'data = (.+)', msg_value['log']).group(1))
                    user_id = user_info['user_id']
                    ride_id = str(uuid.uuid4())

                    user_row = create_user_row(user_id, user_info)
                    user_ride_row = create_user_ride_row(user_id, ride_id)
                    
                    insert_user_row(user_row)
                    insert_user_ride_row(user_ride_row)
                
                elif 'Ride' in msg_value['log']:
                    metrics_pair = []
                    metrics_pair.append(msg_value)
                
                elif 'Telemetry' in msg_value['log']:
                    metrics_pair.append(msg_value)

                    user_name = user_info['name']
                    user_age = calculate_age(int(user_info['date_of_birth']))
                    user_email = user_info['email_address']
                    heart_rate =  int(re.search(r'hrt = (\d+)', msg_value['log']).group(1))
                    get_heart_rate_info(user_name, user_age, user_email, heart_rate)

                    metrics_row = create_metrics_row(ride_id, metrics_pair[0], metrics_pair[1])
                    insert_metrics_row(metrics_row)
                
                elif 'beginning of main' in msg_value['log']:
                    new_ride = False

                    user_info = {}
                    user_id = ''
                    ride_id = ''
                    metrics = []

                    message = 'start new ride'
                    subject = 'production script'
                    log.info(f'Publishing message to topic: {ZUCK_TOPIC}...')
                    message_id = publish_message(ZUCK_TOPIC, message, subject)
                    log.info(f'Message published to topic: {ZUCK_TOPIC} with message Id - {message_id}')
            else:
                msg_value = json.loads(msg.value().decode('utf-8'))
                if 'beginning of a new ride' in msg_value['log']:
                    new_ride = True
except KafkaException as e:
    logging.error(f"Error raised whilst accessing Kafka stream: {e}")
finally:
    c.close()
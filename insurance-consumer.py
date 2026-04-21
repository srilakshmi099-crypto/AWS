import json
import base64
import boto3
import uuid
from datetime import datetime, timezone

# Clients setup
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('claims-table')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

# Config
# Main SQS Queue for downstream processing
SQS_URL = 'https://sqs.eu-west-2.amazonaws.com/666127452756/Insurance'
# DLQ for failed records
DLQ_URL = 'https://amazonaws.com'
# S3 Bucket for archiving
BUCKET_NAME = 'sri-s3-testing'

def extract_claim_id(data):
    return (
        data.get("claim_id") or
        data.get("claimId") or
        (data.get("data", {}).get("claim_id")) or
        (data.get("data", {}).get("claimId"))
    )

def extract_field(data, field):
    return (
        data.get(field) or
        data.get("data", {}).get(field)
    )

def send_to_dlq(raw_payload, error_message):
    """Utility to rescue failed records"""
    try:
        sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({
                "raw_data": raw_payload,
                "error": error_message,
                "failed_at": datetime.now(timezone.utc).isoformat()
            })
        )
    except Exception as e:
        print(f"CRITICAL: Could not send to DLQ: {str(e)}")

def lambda_handler(event, context):
    print("EVENT RECEIVED")

    if 'Records' not in event:
        return {"statusCode": 400, "body": "No Records found"}

    success_count = 0
    fail_count = 0

    for record in event['Records']:
        raw_kinesis_data = record.get('kinesis', {}).get('data', '')
        try:
            # 1. Decode Kinesis payload
            payload = base64.b64decode(raw_kinesis_data).decode('utf-8')
            data = json.loads(payload)

            # 2. Extract claim_id
            claim_id = extract_claim_id(data)
            if not claim_id:
                raise ValueError("Missing claim_id in payload")

            # 3. Build Processed Item
            item = {
                "claim_id": str(claim_id),
                "policy_id": str(extract_field(data, "policy_id") or ""),
                "claim_amount": int(extract_field(data, "claim_amount") or 0),
                "claim_date": str(extract_field(data, "claim_date") or ""),
                "claim_status": str(extract_field(data, "claim_status") or ""),
                "incident_type": str(extract_field(data, "incident_type") or ""),
                "fraud_flag": int(extract_field(data, "fraud_flag") or 0),
                "processed_at_source": str(extract_field(data, "processed_at") or ""),
                "loaded_at": datetime.now(timezone.utc).isoformat()
            }
            
            item_json = json.dumps(item)

            # 4. Save to DynamoDB
            table.put_item(Item=item)

            # 5. Forward to SQS (Main Queue)
            sqs.send_message(
                QueueUrl=SQS_URL,
                MessageBody=item_json
            )

            # 6. Save to S3 (Archival)
            s3_key = f"claims/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{claim_id}_{uuid.uuid4().hex[:6]}.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=item_json,
                ContentType='application/json'
            )

            success_count += 1
            print(f"Successfully processed claim_id: {claim_id}")

        except Exception as e:
            print(f"Processing failed: {str(e)}")
            fail_count += 1
            # Push original data to DLQ for manual recovery
            send_to_dlq(raw_kinesis_data, str(e))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "successful_processed": success_count,
            "failed_sent_to_dlq": fail_count
        })
    }

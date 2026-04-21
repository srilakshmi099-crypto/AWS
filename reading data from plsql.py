import json
import boto3
import csv
from io import StringIO
import pg8000

def lambda_handler(event, context):
    # List the tables you want to export here
    files_to_export = ["customers", "payments","policies"] 
    s3_bucket = "sri-s3-testing"
    
    pg_conn = None
    try:
        pg_conn = pg8000.connect(
            host="13.42.152.118",
            database="testdb",
            user="admin",
            password="admin123",
            port=5432
        )
        cursor = pg_conn.cursor()
        s3 = boto3.client('s3')

        for files in files_to_export:
            # 1. Fetch data for each table
            cursor.execute(f"SELECT * FROM sri.{files};")
            rows = cursor.fetchall()

            # 2. Convert to CSV
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            
            # Write header and data
            colnames = [desc[0] for desc in cursor.description]
            writer.writerow(colnames)
            writer.writerows(rows)

            # 3. Upload to S3 (using the table name for the filename)
            s3.put_object(
                Bucket=s3_bucket,
                Key=f"Bronze/toprocess/{files}.csv",
                Body=csv_buffer.getvalue()
            )
            print(f"Successfully exported {files}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully exported {len(files_to_export)} files!')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        if pg_conn:
            cursor.close()
            pg_conn.close()

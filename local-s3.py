import oracledb
import csv
import boto3
import io
from datetime import datetime

# to load .env variables
import os
from dotenv import load_dotenv
load_dotenv()

alltablenames = os.getenv('table_names').split(".")
allschemanames = os.getenv('schema_names').split(".")

#creating and uploading all the tables
for s in allschemanames:

    # to convert from thin to thick mode 
    oracledb.init_oracle_client(lib_dir=os.getenv('path_to_instantclient'))

    # to connect to oracle database
    dsn = f"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={os.getenv('host')})(PORT={os.getenv('port')})))(CONNECT_DATA=(SERVICE_NAME={os.getenv('service_name')})))"
    connection = oracledb.connect(user = os.getenv('user'), password = os.getenv('password'), dsn=dsn)

    for t in alltablenames:
        print("running the connection for the schema "+s+"."+t)
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT {os.getenv(f"{t}_columns")} FROM {s}.{t}")
            col_names = [col[0] for col in cursor.description]

            # Create an in-memory file object
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer) 
            writer.writerow(col_names)
            writer.writerows(cursor)
            print(f"Successfully connected to {s} and {t}.csv was created successfully.")

        # printing the error if any occurs
        except Exception as e:
            print(e)

        print("uploading the table "+t+" to the s3")

        # estblishing the conncection with AWS s3
        obj = boto3.client("s3")
        try:
            obj.head_object(
                Bucket=f"{os.getenv('bucket_name')}",
                Key=f"{s}/{t}/{s}_{t}.csv"
            )
            print(f"A previous version already exists for the table {s}.{t}")
            old_file=obj.get_object(
                Bucket=f"{os.getenv('bucket_name')}",
                Key=f"{s}/{t}/{s}_{t}.csv"
            )
            old_ver=old_file['Body'].read().decode('utf-8')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                obj.put_object(
                    Bucket=f"{os.getenv('bucket_name')}",
                    Key=f"{s}/{t}/{s}_{t}_olderversions/{s}_{t}_{timestamp}.csv",
                    Body=csv_buffer.getvalue()
                )
                print(f"{s}_{t}.csv was successfully uploaded to the AWS bucket!")
            # printing the error if any occurs
            except Exception as e:
                print("Backup was NOT successful, overwriting the existing file")

        except Exception as e:
            print(f"\nNo existing file found. Creating a .csv file for {s}.{t}")
        # trying to upload the file
        try:
            obj.put_object(
                Bucket=f"{os.getenv('bucket_name')}",
                Key=f"{s}/{t}/{s}_{t}.csv",
                Body=csv_buffer.getvalue()
            )
            print(f"{s}_{t}.csv was successfully uploaded to the AWS bucket!")

        # printing the error if any occurs
        except Exception as e:
            print(e)

        # Close the connection
        csv_buffer.close()
    cursor.close()
    connection.close()
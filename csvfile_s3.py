import boto3
from botocore.exceptions import NoCredentialsError

def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use the file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"File '{file_name}' uploaded to bucket '{bucket}' as '{object_name}'.")
        return True
    except FileNotFoundError:
        print(f"The file '{file_name}' was not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available.")
        return False

# Usage example
if __name__ == "__main__":
    # File to upload
    file_name = r"C:\Users\pradeep.peetla\Desktop\python_task\test.csv"

    # S3 bucket name
    bucket_name = "python-1328"

    # Optional: S3 object name. If not specified, the file name will be used as the object name.
    s3_object_name = "offices.csv"

    # Upload the file
    upload_file_to_s3(file_name, bucket_name, s3_object_name)

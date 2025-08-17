from google.cloud import storage


def move_files_to_processed(bucket_name, source_prefix, processed_prefix="processed/"):
    """
    Move all JSON files from source_prefix to processed_prefix in the given GCS bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_prefix (str): The folder/path prefix of files to move.
        processed_prefix (str): The destination folder/prefix for processed files.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        if blob.name.endswith("/") or not blob.name.endswith(".json"):
            continue

        filename = blob.name.split("/")[-1]
        destination_blob_name = f"{processed_prefix.rstrip('/')}/{filename}"

        bucket.copy_blob(blob, bucket, destination_blob_name)
        blob.delete()

        print(f"Moved {blob.name} to {destination_blob_name}")

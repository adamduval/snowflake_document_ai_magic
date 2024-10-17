import os
import time
import json
import snowflake.connector
from typing import Any, Dict, List

SNOWFLAKE_CONN_PARAMS = {
    'user': '',
    'password': '',
    'account': '',
    'warehouse': '',
    'database': '',
    'schema': ''
}
WATCH_DIR = ''  # Directory to monitor for new files
STAGE = ''  # Snowflake stage for file uploads
TABLE_NAME = ''  # Snowflake table for inserting prediction data
MODEL_NAME = ''  # Model name for running predictions


def find_jpeg_files(directory: str) -> List[str]:
    """
    Recursively searches for all JPEG (.jpeg and .jpg) files in a given directory and its subdirectories.

    Args:
        directory (str): The root directory to start searching from.

    Returns:
        List[str]: A list of file paths for all the JPEG files found.
    """
    jpeg_files = []
    for root, _, files in os.walk(directory):
        for file_name in files:
            if file_name.lower().endswith(('.jpeg', '.jpg')):
                jpeg_files.append(os.path.join(root, file_name))
    return jpeg_files


def upload_to_snowflake(file_path: str, stage_name: str, cursor) -> None:
    """
    Uploads a local file to a specified Snowflake stage.

    Args:
        file_path (str): The path of the local file to upload.
                         Backslashes are replaced with forward slashes for compatibility with Snowflake.
        stage_name (str): The name of the Snowflake stage where the file will be uploaded.
        cursor: The Snowflake cursor to execute the query.
    """
    file_path = file_path.replace('\\', '/')
    print(f"PUT 'file://{file_path}' @{stage_name}")
    cursor.execute(f"PUT 'file://{file_path}' @{stage_name} auto_compress=FALSE")
    print(f"Uploaded {file_path} to Snowflake stage {stage_name}")


def run_prediction(stage_name: str, file_name: str, model_name: str, cursor) -> Dict[str, Any]:
    """
    Runs a stored procedure to get the prediction data for a given file from the Snowflake stage.

    Args:
        stage_name (str): The name of the Snowflake stage where the file is stored.
        file_name (str): The name of the file for which to run the prediction.
        model_name (str): The model used for prediction.
        cursor: The Snowflake cursor to execute the query.

    Returns:
        Dict[str, Any]: Parsed JSON prediction result.
    """
    print(f"Running stored procedure for {file_name} using model {model_name}")
    query = f"""
        SELECT {model_name}!PREDICT(
            GET_PRESIGNED_URL(@{stage_name}, '{file_name}'), 3
        ) as data;
    """
    cursor.execute(query)

    result = cursor.fetchone()[0]
    result_json = json.loads(result)
    print(result_json)

    return result_json


def insert_prediction_data(result_json: Dict[str, Any], file_name: str, table_name: str, cursor) -> None:
    """
    Inserts the extracted prediction data into the target table.

    Args:
        result_json (Dict[str, Any]): Parsed JSON prediction result containing data to insert.
        file_name (str): The name of the file being processed.
        table_name (str): The target Snowflake table for inserting data.
        cursor: The Snowflake cursor to execute the query.
    """
    score = result_json["__documentMetadata"]["ocrScore"]
    date_value = result_json["date"][0]["value"]
    text_value = result_json["text"][0]["value"]
    dropdown_value = result_json["dropdown"][0]["value"]
    numeric_value = result_json["numeric"][0]["value"]
    free_text_writing_value = result_json["free_text_writing"][0]["value"]

    print(f"Inserting data for {file_name} into table {table_name}")
    insert_query = f"""
        INSERT INTO {table_name} (score, date_value, text_value, dropdown_value, numeric_value, free_text_writing_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        score, date_value, text_value, dropdown_value, numeric_value, free_text_writing_value
    ))
    print(f"Data for {file_name} inserted successfully")


def watch_directory_and_upload(directory: str, stage_name: str, table_name: str, model_name: str, cursor, interval: int = 1) -> None:
    """
    Monitors a directory for new JPEG files and uploads them to a Snowflake stage.
    After uploading, it runs a prediction and inserts the extracted data into a Snowflake table.

    Args:
        directory (str): The directory to monitor for new files.
        stage_name (str): The Snowflake stage where files are uploaded.
        table_name (str): The Snowflake table for inserting prediction data.
        model_name (str): The model used for prediction.
        cursor: The Snowflake cursor to execute the query.
        interval (int, optional): The time interval (in seconds) to wait between directory checks. Default is 1 second.
    """
    seen_files = set(find_jpeg_files(directory))

    while True:
        time.sleep(interval)
        current_files = set(find_jpeg_files(directory))
        new_files = current_files - seen_files

        if new_files:
            for file_path in new_files:
                print(f"New file detected: {file_path}")
                upload_to_snowflake(file_path, stage_name, cursor)

                # Extract the file name from the full path
                file_name = os.path.basename(file_path)

                # Run prediction
                result_json = run_prediction(stage_name, file_name, model_name, cursor)

                # Insert prediction data into the table
                insert_prediction_data(result_json, file_name, table_name, cursor)

        seen_files = current_files


if __name__ == "__main__":
    # Open a single connection and cursor for the entire process
    with snowflake.connector.connect(**SNOWFLAKE_CONN_PARAMS) as conn:
        with conn.cursor() as cursor:
            watch_directory_and_upload(WATCH_DIR, STAGE, TABLE_NAME, MODEL_NAME, cursor)

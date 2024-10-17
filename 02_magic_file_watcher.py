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
WATCH_DIR = ''
STAGE = ''
TABLE_NAME = ''
MODEL_NAME = ''


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


def upload_to_snowflake(file_path: str, stage_name: str, conn_params: Dict[str, str]) -> None:
    """
    Uploads a local file to a specified Snowflake stage.

    Args:
        file_path (str): The path of the local file to upload. 
                         Backslashes are replaced with forward slashes for compatibility with Snowflake.
        stage_name (str): The name of the Snowflake stage where the file will be uploaded.
        conn_params (Dict[str, str]): A dictionary of connection parameters for Snowflake. 
                                      Typically includes 'user', 'password', 'account', etc.
    """
    # Ensure the file path is compatible with Snowflake by replacing backslashes with forward slashes
    file_path = file_path.replace('\\', '/')

    # Use a context manager to handle the connection and cursor
    with snowflake.connector.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            print(f"PUT 'file://{file_path}' @{stage_name}")
            cursor.execute(f"PUT 'file://{file_path}' @{stage_name} auto_compress=FALSE")
            print(f"Uploaded {file_path} to Snowflake stage {stage_name}")


def run_prediction(stage_name: str, file_name: str, cursor) -> Dict[str, Any]:
    """
    Runs a stored procedure to get the prediction data for a given file from the Snowflake stage.

    Args:
        stage_name (str): The name of the Snowflake stage where the file is stored.
        file_name (str): The name of the file for which to run the prediction.
        cursor: The Snowflake cursor to execute the query.

    Returns:
        Dict[str, Any]: Parsed JSON prediction result.
    """
    print(f"Running stored procedure for {file_name}")
    query = f"""
        SELECT SANDBOX.ADUVAL.DOCAI_POC!PREDICT(
            GET_PRESIGNED_URL(@{stage_name}, '{file_name}'), 3
        ) as data;
    """
    cursor.execute(query)
    
    # Retrieve and parse the result
    result = cursor.fetchone()[0]
    result_json = json.loads(result)
    print(result_json)
    
    return result_json


def insert_prediction_data(result_json: Dict[str, Any], file_name: str, cursor) -> None:
    """
    Inserts the extracted prediction data into the target table.

    Args:
        result_json (Dict[str, Any]): Parsed JSON prediction result containing data to insert.
        file_name (str): The name of the file being processed.
        cursor: The Snowflake cursor to execute the query.
    """
    # Extract relevant fields from the result JSON
    score = result_json["__documentMetadata"]["ocrScore"]
    date_value = result_json["date"][0]["value"]
    text_value = result_json["text"][0]["value"]
    dropdown_value = result_json["dropdown"][0]["value"]
    numeric_value = result_json["numeric"][0]["value"]
    free_text_writing_value = result_json["free_text_writing"][0]["value"]

    # Insert data into the table
    print(f"Inserting data for {file_name} into table")
    insert_query = """
        INSERT INTO form_table(score, date_value, text_value, dropdown_value, numeric_value, free_text_writing_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        score, date_value, text_value, dropdown_value, numeric_value, free_text_writing_value
    ))
    print(f"Data for {file_name} inserted successfully")


def watch_directory_and_upload(directory: str, stage_name: str, conn_params: Dict[str, str], interval: int = 1) -> None:
    """
    Monitors a directory for new JPEG files and uploads them to a Snowflake stage.
    After uploading, it runs a prediction and inserts the extracted data into a Snowflake table.

    Args:
        directory (str): The directory to monitor for new files.
        stage_name (str): The Snowflake stage where files are uploaded.
        conn_params (Dict[str, str]): A dictionary of connection parameters for Snowflake.
        interval (int, optional): The time interval (in seconds) to wait between directory checks. Default is 1 second.
    """
    # Track files that have already been processed
    seen_files = set(find_jpeg_files(directory))

    while True:
        time.sleep(interval)
        current_files = set(find_jpeg_files(directory))
        new_files = current_files - seen_files

        if new_files:
            for file_path in new_files:
                print(f"New file detected: {file_path}")
                # Upload the new file to Snowflake
                upload_to_snowflake(file_path, stage_name, conn_params)
                
                # Run prediction and insert the data into Snowflake
                with snowflake.connector.connect(**conn_params) as conn:
                    with conn.cursor() as cursor:
                        # Extract the file name from the full path
                        file_name = os.path.basename(file_path)

                        # Run prediction
                        result_json = run_prediction(stage_name, file_name, cursor)

                        # Insert prediction data into the table
                        insert_prediction_data(result_json, file_name, cursor)
        
        # Update the set of seen files to include the new files
        seen_files = current_files

if __name__ == "__main__":
    watch_dir = 'C:/Users/adam.duval/OneDrive - Cooke Aquaculture/files/'  # Replace with the directory you want to monitor
    snowflake_stage = 'docai_stage'   # Replace with your Snowflake stage name

    watch_directory_and_upload(WATCH_DIR, STAGE, SNOWFLAKE_CONN_PARAMS)
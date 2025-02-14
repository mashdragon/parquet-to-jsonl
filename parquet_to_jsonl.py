import argparse
import pyarrow.parquet as pq
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import Optional, Any

def convert_to_json_serializable(obj) -> Any:
    """
    Recursively converts NumPy arrays and other non-JSON-serializable objects to JSON-serializable objects.
    """
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.number):
        return obj.item()
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj

def parquet_to_jsonl(parquet_file_path: str, jsonl_file_path: str) -> None:
    """
    Converts a Parquet file to a JSONL file.

    Args:
    - parquet_file_path (str): Path to the input Parquet file.
    - jsonl_file_path (str): Path to the output JSONL file.
    """
    try:
        # Read the Parquet file
        table = pq.read_table(parquet_file_path)
        # Convert the table to DataFrame
        df: pd.DataFrame = table.to_pandas()

        with open(jsonl_file_path, 'w') as f:
            for index, row in tqdm(df.iterrows(), total=len(df)):
                # Convert each row to a JSON-serializable dictionary
                row_dict = row.to_dict()
                row_dict = convert_to_json_serializable(row_dict)
                
                # Write the dictionary as a JSON line to the file
                json.dump(row_dict, f)
                f.write('\n')

        print(f"Successfully converted {parquet_file_path} to {jsonl_file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a Parquet file to a JSONL file.")
    parser.add_argument("input_file", type=str, help="Path to the input Parquet file.")
    parser.add_argument("output_file", type=str, help="Path to the output JSONL file.")

    args = parser.parse_args()

    parquet_to_jsonl(args.input_file, args.output_file)

if __name__ == "__main__":
    main()

import os
import pandas as pd 
from pathlib import Path

def silver_layer_transform(input_path: Path, output_path: Path)->None:
    json_files = os.listdir(input_path)
    for file in json_files:
        if file.endswith('.json'):
            file_path = os.path.join(input_path, file)
            df = pd.read_json(file_path)
            df = df[df['latitude'].notna() & df['longitude'].notna()]
            parquet_file_path = os.path.join(output_path, file.replace('.json', '.parquet'))
            df.to_parquet(parquet_file_path, index=False)
            print(f'Transformed {file} to {parquet_file_path}')

if __name__ == '__main__':
    input_path = Path('./data/bronze')
    output_path = Path('./data/silver')
    os.makedirs(output_path, exist_ok=True)
    silver_layer_transform(input_path, output_path)
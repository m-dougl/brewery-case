import requests 
import json
import os 
from datetime import datetime

def fetch_data(page: int, data_per_page: int = 50):
    """
    Fetch data from the Open Brewery DB API and save it as a JSON file.
    """
    url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={data_per_page}"

    try:
        response = requests.get(url)
        data = response.json()

        output_dir = './data/bronze'
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        file_path = os.path.join(output_dir, f'breweries_{timestamp}.json')
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        
        print(f'Data saved to {file_path}')

    except Exception as e:
        print(f'An error ocurred: {e}')

if __name__ == '__main__':
    fetch_data(page=0)
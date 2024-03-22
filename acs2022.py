import json
import requests
import csv
from constants import VARIABLES, API_KEY


def download_2022_table(table_name: str):
    URL = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,group({table_name})&for=tract:*&in=state:42&key={API_KEY}'
    response = requests.get(URL)
    raw_data = response.json()
    table_data = VARIABLES['variables']
    headers_with_titles = [f'{item}: {table_data[item]["concept"]}, {table_data[item]["label"]}'
                           if item[:6] == table_name
                           and len(item) == 11
                           and item[-1] == 'E'
                           else item
                           for item in raw_data[0]]
    output_data = [headers_with_titles]
    output_data.extend(raw_data[1:])
    output_file_name = f'{table_name}_2022_acs.csv'
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f, delimiter=',')
        for row in output_data:
            writer.writerow(row)

import requests
import pandas as pd
import yaml
import csv
from web3 import Web3
from web3.middleware import validation
from application_logging.logger import logger


# Params
params_path = "params.yaml"


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)
subgraph = config["query"]["fusion_subgraph"]
id_data_fusion_query = config["query"]["id_data_fusion_query"]
provider_url = config["web3"]["provider_url"]
token_abi = config["web3"]["token_abi"]
data_list = []

# Web3
validation.METHODS_TO_VALIDATE = []
w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

try:
    logger.info("ID Data Started")
    

    response = requests.post(
                subgraph, json=id_data_fusion_query, timeout=20)
    data = response.json()
    # Extract the 'gauges' data
    gauges_data = data['data']['gauges']
    # Define the CSV file name
    csv_file = 'ids_data.csv'


    # Write the data to the CSV file
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
    
       # Write the header row
        header = ['id', 'poolAddress', 'externalBribe_id', 'internalBribe_id']
        writer.writerow(header)
    
        # Write data for each gauge
        for gauge in gauges_data:
            contract_instance = w3.eth.contract(address=Web3.to_checksum_address(gauge['poolAddress']), abi=token_abi)
            symbol = contract_instance.functions.symbol().call()
            row = {
                'symbol':symbol,
                'poolAddress': gauge['poolAddress'],
                'gaugeAddress': gauge['id'],
                'externalBribe': gauge['externalBribe']['id'],
                'internalBribe': gauge['internalBribe']['id']
            }
            print(row)
            data_list.append(row)

        print(f'Data has been written to {csv_file}')           

    df = pd.DataFrame(data_list)
    df.to_csv('data/ids_data.csv', index=False)

    logger.info("ID Data Ended")
except Exception as e:
    logger.error("Error occurred during ID Data process. Error: %s" %
                 e, exc_info=True)

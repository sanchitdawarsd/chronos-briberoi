import requests
import pandas as pd
import yaml
from datetime import datetime, timezone, timedelta
from application_logging.logger import logger
from web3 import Web3
from web3.middleware import validation
from flask import Flask, jsonify

# Params
params_path = "params.yaml"

def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

# Fusion
try:
    # Params Data
    subgraph = config["query"]["fusion_subgraph"]
    id_data = config["files"]["id_data"]
    briberoi_data = config["files"]["bribe_data"]
    roi_data = config["files"]["roi_data"]
    pair_data_fusion_query = config["query"]["pair_data_fusion_query"]
    epoch_csv = config["files"]["epoch_data"]
    pair_data_fusion_csv = config["files"]["pair_data_fusion"]
    provider_url = config["web3"]["provider_url"]
    token_abi = config["web3"]["token_abi"]
    starting_date = datetime(2023, 4, 19)  # Replace with your starting date

    # Pulling Pair Data
    logger.info("Pair Data Fusion Started")
    # Request and Edit Pair Data
    ids_df = pd.read_csv(id_data)
    roi_df = pd.read_csv(roi_data)
    print(ids_df, "dat2a")
    # Today and 2 Day Ago
    todayDate = datetime.utcnow()
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())

    # Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    # Function to extract ID by symbol
    def get_id_by_symbol(data, symbol):
        for item in data:
            if item.get("symbol") == symbol:
                return item.get("id")
        return None

    pairdata_fusion_df = pd.DataFrame()
    data_list = []
    for name, contract_address in zip(ids_df["symbol"], ids_df["address"]):
        try:
            pair_data_fusion_query["variables"]["poolAddress"] = contract_address.lower(
            )
          #  pair_data_fusion_query["variables"]["startTime"] = timestamp
            response = requests.post(
                subgraph, json=pair_data_fusion_query, timeout=60)

            symbolResponse = requests.get(
                "https://pro-api.coingecko.com/api/v3/coins/list?x_cg_pro_api_key=CG-79M3wHmkFuqNTRPxbcpytif4", timeout=60)
            symboldata = symbolResponse.json()

            # print(pair_data_fusion_query, "data")
            data = response.json()["data"]
            # Print the error response for debugging

            # Access the rewards list
            rewards = data['gauges'][0]['externalBribe']['rewards']
            print(len(rewards))

            # Iterate over the length of rewards
            for reward in rewards:
                # Example
                contract_instance = w3.eth.contract(
                    address=Web3.to_checksum_address(
                        reward['tokenAddress']), abi=token_abi)

                symbol_to_find = contract_instance.functions.symbol().call()
                decimal_to_find = contract_instance.functions.decimals().call()

                found_id = get_id_by_symbol(symboldata, symbol_to_find.lower())

                # Convert Unix timestamp to a datetime object & Format the date as dd-mm-yyyy
                date_obj = datetime.fromtimestamp(int(reward["timestamp"]))
                formatted_date = date_obj.strftime('%d-%m-%Y')

                try:
                    priceResponse = requests.get(
                        f"https://pro-api.coingecko.com/api/v3/coins/{found_id}/history?x_cg_pro_api_key=CG-79M3wHmkFuqNTRPxbcpytif4&date={formatted_date}", timeout=60)
                    pricedata = priceResponse.json()
                    priceusd = pricedata["market_data"]["current_price"]["usd"]
                    rewardAmountUsd = "{:.2f}".format((
                        int(reward["tokenAmount"])/10**decimal_to_find)*priceusd)
                    print(contract_address, reward["tokenAddress"], reward["tokenAmount"], rewardAmountUsd,
                          formatted_date, found_id)

                    # 24 hours in a day, 3600 seconds in an hour
                    time_difference = (
                        int(reward["timestamp"]) - starting_date.timestamp()) / (24 * 3600)
                    epoch = int(time_difference / 7)

                    new_data = {
                        'poolSymbol': name,
                        'poolAddress': contract_address,
                        'rewardSymbol': symbol_to_find,
                        'rewardAddress': reward["tokenAddress"],
                        'rewardUsd': rewardAmountUsd,
                        "epoch": epoch,
                        'id': found_id
                        # Add more columns and values as needed
                    }
                    data_list.append(new_data)

                except Exception as e:
                    priceusd = 1
                    rewardAmountUsd = "{:.2f}".format((
                        int(reward["tokenAmount"])/10**decimal_to_find)*priceusd)
                    print(contract_address, reward["tokenAddress"], reward["tokenAmount"], rewardAmountUsd,
                          formatted_date, found_id)
                    # 24 hours in a day, 3600 seconds in an hour
                    time_difference = (
                        int(reward["timestamp"]) - starting_date.timestamp()) / (24 * 3600)
                    epoch = int(time_difference / 7)
                    new_data = {
                        'poolSymbol': name,
                        'poolAddress': contract_address,
                        'rewardSymbol': symbol_to_find,
                        'rewardAddress': reward["tokenAddress"],
                        'rewardUsd': rewardAmountUsd,
                        'id': found_id,
                        'epoch': epoch
                        # Add more columns and values as needed
                    }
                    data_list.append(new_data)
                    logger.error(
                        "Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)
            df = pd.DataFrame(data_list)
            df.to_csv('data/roi_data.csv', index=False)

        except Exception as e:
            print(e)
            logger.error("Error occurred during Pair Data Fusion process. Pair: %s, Address: %s, Error: %s" % (
                name, contract_address, e))
  
    logger.info("Pair Data Fusion Ended")
except Exception as e:
    logger.error(
        "Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)

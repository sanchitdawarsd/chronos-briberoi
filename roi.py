import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone, timedelta
from application_logging.logger import logger
import gspread
from web3 import Web3
from web3.middleware import validation

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
    roi_data = config["files"]["roi_data"]
    pair_data_fusion_query = config["query"]["pair_data_fusion_query"]
    epoch_daily_csv = config["files"]["epoch_daily_data"]
    pair_data_fusion_csv = config["files"]["pair_data_fusion"]
    provider_url = config["web3"]["provider_url"]
    token_abi = config["web3"]["token_abi"]

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
                    # new_data = pd.DataFrame({
                    #     'Column1': [token_amount],
                    #     'Column2': [token_address],
                    #     # Add more columns and values as needed
                    # })
                    # combined_data = pd.concat(
                    #     [roi_df, new_data], ignore_index=True)
                    # combined_data.to_csv('roi_data.csv', index=False)
                    with open('output.txt', 'a') as file:
                        # Write data to the file
                        file.write(str(len(rewards)))
                        token_address = reward["tokenAddress"]
                        token_amount = reward["tokenAmount"]

                # Create a string with the values separated by commas
                        data_to_write = f"{contract_address}, {rewardAmountUsd},{token_address},{token_amount} , {formatted_date}, {found_id}\n"

                # Write the string to the file
                        file.write(data_to_write)
                        file.write('\n\n\n')

                except Exception as e:
                    priceusd = 1
                    rewardAmountUsd = "{:.2f}".format((
                        int(reward["tokenAmount"])/10**decimal_to_find)*priceusd)
                    print(contract_address, reward["tokenAddress"], reward["tokenAmount"], rewardAmountUsd,
                          formatted_date, found_id)
                    with open('output.txt', 'a') as file:
                        # Write data to the file
                        file.write(str(len(rewards)))
                        token_address = reward["tokenAddress"]
                        token_amount = reward["tokenAmount"]
                # Create a string with the values separated by commas
                        data_to_write = f"{contract_address}, {rewardAmountUsd},{token_address},{token_amount} , {formatted_date}, {found_id}\n"

                # Write the string to the file
                        file.write(data_to_write)
                        file.write('\n\n\n')
                    logger.error(
                        "Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)
            df = pd.json_normalize(data)
            df["name"] = name
            # pairdata_fusion_df = pd.concat(
            #     [pairdata_fusion_df, df], axis=0, ignore_index=True)
            # pairdata_fusion_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(e)
            logger.error("Error occurred during Pair Data Fusion process. Pair: %s, Address: %s, Error: %s" % (
                name, contract_address, e))
    # epoch_data = pd.read_csv(epoch_daily_csv)
    # epoch_data["date"] = epoch_data["date"].apply(
    #     lambda date: datetime.strptime(date, "%d-%m-%Y").date())

    # pairdata_fusion_df["date"] = pairdata_fusion_df["date"].apply(
    #     lambda timestamp: datetime.utcfromtimestamp(timestamp).date())
    # pairdata_fusion_df = pd.merge(pairdata_fusion_df, ids_df[[
    #                               "symbol", "underlyingPool", "type"]], how="left", left_on="name", right_on="symbol")
    # pairdata_fusion_df.drop("symbol", axis=1, inplace=True)
    # pairdata_fusion_df = pd.merge(
    #     pairdata_fusion_df, epoch_data[["date", "epoch"]], how="left", on="date")
    # pairdata_fusion_df.sort_values("date", ascending=True, inplace=True)
    # pairdata_fusion_df["date"] = pairdata_fusion_df["date"].apply(
    #     lambda date: datetime.strftime(date, "%Y-%m-%d"))

    # pairdata_fusion_old = pd.read_csv(pair_data_fusion_csv)
    # drop_index = pairdata_fusion_old[pairdata_fusion_old['date'] > datetime.fromtimestamp(
    #     timestamp).strftime(format='%Y-%m-%d')].index
    # index_list = drop_index.to_list()
    # index_list = list(map(lambda x: x + 2, index_list))
    # pairdata_fusion_df['__typename'] = 'Fusion'
    # pairdata_fusion_df = pairdata_fusion_df[['id', 'date', 'tvlUSD', 'volumeUSD', 'volumeToken0', 'volumeToken1',
    #                                          'token0Price', 'token1Price', 'feesUSD', '__typename', 'name', 'underlyingPool', 'type', 'epoch']]
    # pairdata_fusion_df = pairdata_fusion_df.astype({'tvlUSD': 'float', 'volumeUSD': 'float', 'volumeToken0': 'float',
    #                                                'volumeToken1': 'float', 'token0Price': 'float', 'token1Price': 'float', 'feesUSD': 'float'})
    # df_values = pairdata_fusion_df.values.tolist()

    # # Write to GSheets
    # credentials = os.environ["GKEY"]
    # credentials = json.loads(credentials)
    # gc = gspread.service_account_from_dict(credentials)

    # # Open a google sheet
    # sheetkey = config["gsheets"]["pair_data_fusion_sheet_key"]
    # gs = gc.open_by_key(sheetkey)

    # # Select a work sheet from its name
    # worksheet1 = gs.worksheet("Master")
    # if index_list != []:
    #     worksheet1.delete_rows(index_list[0], index_list[-1])

    # # Append to Worksheet
    # gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {
    #                  "values": df_values})

    logger.info("Pair Data Fusion Ended")
except Exception as e:
    logger.error(
        "Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)

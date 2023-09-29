import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta, TH
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

try:
    # Params Data
    id_data = config["files"]["id_data"]
    provider_url = config["web3"]["provider_url"]
    bribe_abi = config["web3"]["bribe_abi"]
    epoch_csv = config["files"]["epoch_data"]
    price_api = config["api"]["price_api"]
    bribe_csv = config["files"]["bribe_data"]

    # Pulling Bribe Data
    logger.info("Bribe Data Started")
    print(id_data)
    ids_df = pd.read_csv(id_data)
    # Get Epoch Timestamp
    todayDate = datetime.utcnow()

    if todayDate.isoweekday() == 4 and todayDate.hour > 1:
        nextThursday = todayDate + relativedelta(weekday=TH(2))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("Yes, The next Thursday date:", my_datetime, timestamp)
    else:
        nextThursday = todayDate + relativedelta(weekday=TH(0))
        my_time = datetime.min.time()
        my_datetime = datetime.combine(nextThursday, my_time)
        timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
        print("No, The next Thursday date:", my_datetime, timestamp)

    # Read Epoch Data
    epoch_data = pd.read_csv(epoch_csv)
    epoch = epoch_data[epoch_data["timestamp"]
                       == timestamp]["epoch"].values[0] - 1

    # Pull Bribes Web3
    validation.METHODS_TO_VALIDATE = []
    w3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={"timeout": 60}))

    bribes_list = []
    print(ids_df)
    for name, bribe_ca in zip(ids_df["symbol"], ids_df["gauge.bribe"]):
        if bribe_ca == "0x0000000000000000000000000000000000000000":
            pass
        else:
            contract_address = bribe_ca
            contract_instance = w3.eth.contract(
                address=contract_address, abi=bribe_abi)

            rewardsListLength = contract_instance.functions.rewardsListLength().call()

            rewardTokens = []
            for reward_num in range(rewardsListLength):
                rewardTokens.append(
                    contract_instance.functions.rewardTokens(reward_num).call()
                )

            for reward_addy in rewardTokens:
                rewarddata = contract_instance.functions.rewardData(
                    reward_addy, timestamp
                ).call()
                if rewarddata[1] > 0:
                    bribes_list.append(
                        {"name": name,
                            "bribes": rewarddata[1], "address": reward_addy}
                    )

    bribe_df = pd.DataFrame(bribes_list)
    if bribe_df.empty:
        raise Exception("Bribe DF is Empty.")
    bribe_df["address"] = bribe_df["address"].apply(str.lower)

    # Pull Prices
    response = requests.get(price_api)
    pricelist = []
    for i in response.json()["data"]:
        pricelist.append([i["name"], i["address"], i["price"], i["decimals"]])

    price_df = pd.DataFrame(
        pricelist, columns=["name", "address", "price", "decimals"])

    # Bribe Amounts
    bribe_df = bribe_df.merge(
        price_df[["name", "address", "price", "decimals"]], on="address", how="left"
    )
    null_data = bribe_df[bribe_df.isnull().any(axis=1)]
    if not null_data.empty:
        logger.error("Null Data. Error: %s" % null_data)

    bribe_df = bribe_df.dropna(axis=0)
    bribe_df.reset_index(drop=True, inplace=True)
    bribe_df["bribe_amount"] = bribe_df["price"] * bribe_df["bribes"]
    bribe_df["decimals"] = bribe_df["decimals"].astype(int)

    bribe_amount = []
    for dec, amt in zip(bribe_df["decimals"], bribe_df["bribe_amount"]):
        decimal = "1"
        decimal = decimal.ljust(dec + 1, "0")
        bribe_amount.append((amt / int(decimal)))

    bribe_df["bribe_amount"] = bribe_amount
    bribe_df["epoch"] = epoch
    bribe_df.drop(["bribes", "decimals"], axis=1, inplace=True)
    bribe_df.columns = [
        "name_pool",
        "address",
        "name_token",
        "price",
        "bribe_amount",
        "epoch",
    ]

    # Rewriting current Epoch's Bribe Data
    bribor = pd.read_csv(bribe_csv)
    drop_index = bribor[bribor["epoch"] == epoch].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))
    df_values = bribe_df.values.tolist()

    # Write to GSheets
    credentials = os.environ["GKEY"]
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config["gsheets"]["bribe_data_sheet_key"]
    gs = gc.open_by_key(sheetkey)
    print(bribe_df)
    # Select a work sheet from its name
    worksheet1 = gs.worksheet("Master")
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append("Master", {"valueInputOption": "USER_ENTERED"}, {
                     "values": df_values})

    logger.info("Bribe Data Ended")
except Exception as e:
    logger.error(
        "Error occurred during Bribe Data process. Error: %s" % e, exc_info=True
    )

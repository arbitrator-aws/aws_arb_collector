import json
import boto3
import datetime as dt
from decimal import Decimal
import pytz
import requests
import sys

# home dir
sys.path.insert(0, "~/vscode_proj/aws/aws_arb_collector/layers/collector_common/python")
import collector_functions as f

def lambda_handler(event, context):
    timestamp = dt.datetime.now(pytz.utc)
    
    # define cryptowatch endpoints for exchanges
    luno_ethzar = "https://api.cryptowat.ch/markets/luno/ethzar/summary"
    luno_btczar = "https://api.cryptowat.ch/markets/luno/btczar/summary"

    kraken_etheur = "https://api.cryptowat.ch/markets/kraken/etheur/summary"
    kraken_btceur = "https://api.cryptowat.ch/markets/kraken/btceur/summary"

    valr_ethzar = ""
    valr_btczar = ""


    # ret responses
    resp_luno_ethzar = requests.get(luno_ethzar)
    resp_luno_btczar = requests.get(luno_btczar)

    resp_kraken_etheur = requests.get(kraken_etheur)
    resp_kraken_btceur = requests.get(kraken_btceur)
    

    # format responses
    json_luno_btczar = f.formatter(resp_luno_btczar, timestamp)
    json_kraken = f.formatter(resp_kraken_btceur, timestamp)
    # print(json_luno)
    # print(json_kraken)

    
    # # write to dynamo
    dynamodb = boto3.resource('dynamodb')
    
    # table = dynamodb.Table('arbitrator-btc-hist')
    # f.ddb_btc_updater(table, timestamp.replace(second=0, microsecond=0), "luno", json_luno)
    # f.ddb_btc_updater(table, timestamp.replace(second=0, microsecond=0), "kraken", json_kraken)
    
    table = dynamodb.Table('arbitrator-btc-hist-minutely')
    f.ddb_btc_updater_minutely(table, timestamp.replace(second=0, microsecond=0), "luno", json_luno)
    f.ddb_btc_updater_minutely(table, timestamp.replace(second=0, microsecond=0), "kraken", json_kraken)
    
    return {
        'statusCode': 200,
        'body': json.dumps('btc hist table updated')
    }

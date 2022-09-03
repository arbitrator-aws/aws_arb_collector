import json
import boto3
import botocore
import pickle
import datetime as dt
import requests
import pytz
from decimal import Decimal
import pandas as pd
import s3fs

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def formatter(response, rate, currency, timestamp):
    if (response.status_code == 200) :
        req = flatten_json(json.loads(response.content, parse_float=Decimal))
        req["rate"] = rate
        req["datetime_utc"] = str(timestamp)
        req["currency"] = currency
        req["allowance_cost"] = None
        req["allowance_remaining"] = None
    else:
        req = None
        req
    return(req)
    
def s3_csv_writer(bucket, exchange, data, ts):
    filename="data/" + exchange + "/" + str(ts.date()) + "_" + exchange + ".csv"
    try:
        boto3.resource('s3').Object("arbitrator-store", filename).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            data.to_csv("s3://" + bucket + "/" + filename, index=False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist.
        stemp = pd.read_csv("s3://" + bucket + "/" + filename)
        stemp = stemp.append(data)
        stemp.to_csv("s3://" + bucket + "/" + filename, index=False)
    
def lambda_handler(event, context):
    # This function doesnt need to know what to do at the moment.
    # in future I may try and extend is to do different things, but for now it has 1 simple role

    kraken = "https://api.cryptowat.ch/markets/kraken/btceur/summary"
    luno = "https://api.cryptowat.ch/markets/luno/btczar/summary"
    exch = "https://api.exchangeratesapi.io/latest?base=EUR"

    timestamp = dt.datetime.now(pytz.utc)
    resp_exch = requests.get(exch)
    resp_luno = requests.get(luno)
    resp_kraken = requests.get(kraken)

    ex_rate = json.loads(resp_exch.content, parse_float=Decimal)["rates"]["ZAR"]

    json_luno = formatter(resp_luno, ex_rate, "zar", timestamp)
    json_kraken = formatter(resp_kraken, ex_rate, "eur", timestamp)
    
    if json_luno is not None:
        s3_csv_writer(bucket="arbitrator-store", exchange="luno", data=pd.DataFrame(json_luno, index=[0]), ts=timestamp)
    if json_kraken is not None:
        s3_csv_writer(bucket="arbitrator-store", exchange="kraken", data=pd.DataFrame(json_kraken, index=[0]), ts=timestamp)
    
    # # write to dynamo

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('arb_exchange_data')
    
    if json_luno is not None:
        table.put_item(
           Item={
                'timestamp_utc': int(timestamp.timestamp()),
                'exchange': "luno",
                'data': json_luno
            }
        )
        
    if json_kraken:
        table.put_item(
            Item = {
                'timestamp_utc': int(timestamp.timestamp()),
                'exchange': "kraken",
                'data': json_kraken.to_json(orient='index')
            }
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('db updated')
    }

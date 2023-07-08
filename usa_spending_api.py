import os
from dotenv import load_dotenv
import json 
import requests
import pandas as pd

load_dotenv("./env_info/.env")

url = os.getenv('usaspending_url')
endpoint = "/api/v2/autocomplete/accounts/aid/"

# create a class to pull data from the USA Spending API
# table would then be converted to pandas dataframe
class api_pull:
    def __init__(self, url, endpoint):
        self.url = url
        self.endpoint = endpoint

    def agency_aid():
        values = []
        for i in range(0, 10):
            body = {
                "filters": {
                    "aid": str(i),
                    },
                    "limit": 500,
                    }
            response = requests.post(url + endpoint, json=body)
            for key in response.json():
                for element in response.json()[key]:
                    values.append(element)

        # turn list of dictionaries into a pandas table
        df = pd.DataFrame(values)
        # add today's date and time to the table using universal time
        df['update_dt'] = pd.to_datetime('today',utc=True).strftime("%Y-%m-%d %H:%M:%S")
        # drop duplicates
        df = df.drop_duplicates()
        return(df)
    


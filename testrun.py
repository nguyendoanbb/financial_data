import os
from dotenv import load_dotenv
import json 
import requests
import pandas as pd

load_dotenv("./env_info/.env")

url = os.getenv('usaspending_url')
parm = {'toptier_code': '097', 'fiscal_year': 2022}
endpoint = f"/api/v2/agency/:{parm.get('toptier_code')}?fiscal_year={parm.get('fiscal_year')}"
response = requests.get(url + endpoint, params=parm)

# convert dictionary to pandas dataframe when the dictionary key has a list of dictionaries as its value
values = []
for key in response.json():
    if key == 'def_codes':
        for element in response.json()[key]:
            values.append(element)

df = pd.DataFrame(values)
# print(df)

# drop a key from a dictionary and assign to a new dictionary 
resp_new_dict = response.json()
resp_new_dict.pop('def_codes')
# convert dictionary to pandas dataframe with dictionary key as column
df2 = pd.DataFrame.from_dict(resp_new_dict, orient='index').transpose() 
print(df2)



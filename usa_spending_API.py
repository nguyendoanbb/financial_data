import json 
import requests
import pandas as pd

url = "https://api.usaspending.gov/"
endpoint = "/api/v2/autocomplete/accounts/aid/"

# extract element from json
# write a for loop to extract data from json
# for key in response.json():
#     print(key)
#     for element in response.json()[key]:
#         print(element)

# append dictionary value into a list and create a pandas table
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
print(df.head())

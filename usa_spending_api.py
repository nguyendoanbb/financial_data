import os
from dotenv import load_dotenv
import json 
import requests
import pandas as pd

load_dotenv("./env_info/.env")

url = os.getenv('usaspending_url')

# create a class to pull data from the USA Spending API
# table would then be converted to pandas dataframe
class api_pull:
    def __init__(self, url):
        self.url = url
        
    def agency_aid():
        endpoint = "/api/v2/autocomplete/accounts/aid/"
        values = []
        for i in range(0, 10):
            body = {
                "filters": {
                    "aid": str(i),
                    },
                    "limit": 500
                    }
            response = requests.post(url + endpoint, json=body)
            if response.status_code == 200:
                for key in response.json():
                    for element in response.json()[key]:
                        values.append(element)
            else:
                print("Error: ", response.status_code)

        # turn list of dictionaries into a pandas table
        df = pd.DataFrame(values)
        # add today's date and time to the table using universal time
        df['update_dt'] = pd.to_datetime('today',utc=True).strftime("%Y-%m-%d %H:%M:%S")
        # drop duplicates
        df = df.drop_duplicates()
        return(df)
    
    def aid_check(aid_list):
        # convert pd column into list with unique values
        try:
            aid_unique = aid_list['aid'].unique().tolist()
            aid_unique = aid_unique[0:10]
            return(aid_unique)
        except:
            print("Need a list of AIDs.")

    def agency_overview(aid_list):
        # agency_aid_df = api_pull.agency_aid()

        def_codes_df = pd.DataFrame()
        agency_overview = pd.DataFrame()

        aid_unique = api_pull.aid_check(aid_list)

        # get current fiscal year that end in September and convert to integer
        fiscal_year = int(pd.to_datetime('today').strftime("%Y")) + 1 if int(pd.to_datetime('today').strftime("%m")) > 9 else int(pd.to_datetime('today').strftime("%Y"))

        for aid in aid_unique:
            print("Currently processing AID: ", aid)
            parm = {'toptier_code': aid, 'fiscal_year': fiscal_year}
            endpoint = f"/api/v2/agency/:{parm.get('toptier_code')}?fiscal_year={parm.get('fiscal_year')}"
            response = requests.get(url + endpoint, params=parm)

            if response.status_code == 200:
                values = [] # list of dictionaries
                for key in response.json():
                    if key == 'def_codes':
                        for element in response.json()[key]:
                            values.append(element)
                def_codes = pd.DataFrame(values)
                def_codes["toptier_code"] = parm.get('toptier_code')
                if def_codes_df.empty:    
                    def_codes_df = def_codes
                else:
                    def_codes_df = pd.concat([def_codes_df, def_codes], ignore_index=True)

                resp_new_dict = response.json()
                resp_new_dict.pop('def_codes')
                agency_df = pd.DataFrame.from_dict(resp_new_dict, orient='index').transpose() 
                if agency_overview.empty:
                    agency_overview = agency_df
                else:
                    agency_df = pd.concat([agency_overview, agency_df], ignore_index=True)
            else:
                print("AID ", aid, " ---- ", "Error: ", response.status_code)

    def agency_def_code(aid_list): # disaster emergency fund code
        endpoint = "/api/v2/references/def_codes/"
        response = requests.get(url + endpoint)
        values = [] # list of dictionaries
        if response.status_code == 200:
            for key in response.json():
                for element in response.json()[key]:
                    values.append(element)
        else:
            print("Error: ", response.status_code)
        def_codes = pd.DataFrame(values)
        print(def_codes)
        return(def_codes)
    
    def drop_na_reset_index(df):
        df = df.dropna(axis=0, how='all')
        df = df.dropna(axis=1, how='all')
        df = df.reset_index(drop=True)
        return(df)
    
    def agency_federal_account(aid_list):
        # aid_unique = aid_list
        aid_unique = api_pull.aid_check(aid_list)
        fiscal_year = int(pd.to_datetime('today').strftime("%Y")) + 1 if int(pd.to_datetime('today').strftime("%m")) > 9 else int(pd.to_datetime('today').strftime("%Y"))
        results_all = pd.DataFrame()
        results_children_all = pd.DataFrame()
        for aid in aid_unique:
            parm = {'toptier_code': aid, 'fiscal_year': fiscal_year}
            endpoint = f"/api/v2/agency/{parm.get('toptier_code')}/federal_account?fiscal_year={parm.get('fiscal_year')}"
            response = requests.get(url + endpoint, params=parm)
            
            if response.status_code == 200:
                all_df = response.json()
                all_df.pop('page_metadata')
                all_df = pd.DataFrame.from_dict(all_df, orient='index').transpose() 

                results = all_df.results.apply(pd.Series)
                results.columns = results.columns.map(str)
                results_children = pd.DataFrame()
                for value in results.columns:
                    results = pd.concat([results.drop([value], axis=1), results[value].apply(pd.Series)], axis=0)
                results = api_pull.drop_na_reset_index(results)
                results["aid"] = aid
                i = 0
                for value in results.children:
                    temp = pd.DataFrame(value)
                    temp["main_code"] = results.code[i]
                    results_children = pd.concat([results_children, temp], axis=0)
                    i += 1
                results_children = api_pull.drop_na_reset_index(results_children)
                results_children["aid"] = aid
                print("Processed AID: ", aid)
            else:
                print("Processed AID: ", aid, " ---- ", "Error: ", response.status_code)
            results_all = pd.concat([results_all, results], axis=0)
            results_children_all = pd.concat([results_children_all, results_children], axis=0)
        return(results_all, results_children_all)

    
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
    
    def clean_df(df):
        df = df.dropna(axis=0, how='all')
        df = df.dropna(axis=1, how='all')
        df = df.drop_duplicates()
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
                results = pd.json_normalize(response.json(), record_path=["results"])
                results.pop("children")
                results["aid"] = aid
                results["fiscal_year"] = fiscal_year

                results_children = pd.json_normalize(response.json(), record_path=["results", "children"])
                results_children["aid"] = aid
                results_children["fiscal_year"] = fiscal_year

                print("Processed AID: ", aid)
            else:
                print("Processed AID: ", aid, " ---- ", "Error: ", response.status_code)

            results_all = pd.concat([results_all, results], axis=0)
            results_children_all = pd.concat([results_children_all, results_children], axis=0)
        return(results_all, results_children_all)
    
    def agency_award(aid_list):
        aid_unique = api_pull.aid_check(aid_list)
        fiscal_year = int(pd.to_datetime('today').strftime("%Y")) + 1 if int(pd.to_datetime('today').strftime("%m")) > 9 else int(pd.to_datetime('today').strftime("%Y"))
        awards_all = pd.DataFrame()

        for aid in aid_unique:
            parm = {'toptier_code': aid, 'fiscal_year': fiscal_year}
            endpoint = f"/api/v2/agency/{parm.get('toptier_code')}/awards?fiscal_year={parm.get('fiscal_year')}"
            response = requests.get(url + endpoint, params=parm)

            if response.status_code == 200:
                awards = pd.json_normalize(response.json())
                awards.pop("messages")
                awards["aid"] = aid
                print("Processed AID: ", aid)
            else:
                print("Processed AID: ", aid, " ---- ", "Error: ", response.status_code)

            awards_all = pd.concat([awards_all, awards], axis=0)

        return(awards_all)

    def agency_budget_function(aid_list):
        aid_unique = api_pull.aid_check(aid_list)
        fiscal_year = int(pd.to_datetime('today').strftime("%Y")) + 1 if int(pd.to_datetime('today').strftime("%m")) > 9 else int(pd.to_datetime('today').strftime("%Y"))
        budget_function_all = pd.DataFrame()

        for aid in aid_unique:
            parm = {'toptier_code': aid, 'fiscal_year': fiscal_year}
            endpoint = f"/api/v2/agency/{parm.get('toptier_code')}/budget_function"
            response = requests.get(url + endpoint, params=parm)
            
            if response.status_code == 200:
                budget_function = pd.json_normalize(response.json(), record_path=["results", "children"],
                                                    meta=["toptier_code", "fiscal_year", ["results", "name"]])
                print("Processed AID: ", aid)
            else:
                print("Processed AID: ", aid, " ---- ", "Error: ", response.status_code)
            budget_function_all = pd.concat([budget_function_all, budget_function], axis=0)
        budget_function_all = api_pull.clean_df(budget_function_all)
        return(budget_function_all)
    
    def agency_budgetary_resources(aid_list):
        aid_unique = api_pull.aid_check(aid_list)
        budgetary_all = pd.DataFrame()

        for aid in aid_unique:
            parm = {'toptier_code': aid}
            endpoint = f"/api/v2/agency/{parm.get('toptier_code')}/budgetary_resources"
            response = requests.get(url + endpoint, params=parm)

            if response.status_code == 200:
                budgetary = pd.json_normalize(response.json(), 
                                              record_path=["agency_data_by_year", "agency_obligation_by_period"],
                                              meta = ["toptier_code", 
                                                      ["agency_data_by_year", "fiscal_year"],
                                                      ["agency_data_by_year", "agency_budgetary_resources"],
                                                      ["agency_data_by_year", "agency_total_obligated"],
                                                      ["agency_data_by_year", "total_budgetary_resources"]])
                print("Processed AID: ", aid)
            else:
                print("Processed AID: ", aid, " ---- ", "Error: ", response.status_code)
            budgetary_all = pd.concat([budgetary_all, budgetary], axis=0)
            budgetary_all = api_pull.clean_df(budgetary_all)
        return(budgetary_all)
    

            
        

    
import usa_spending_api

AID = usa_spending_api.api_pull.agency_aid()
# usa_spending_api.api_pull.agency_overview(aid_list=AID) 
test = usa_spending_api.api_pull.agency_budgetary_resources(aid_list=AID)
print(test)
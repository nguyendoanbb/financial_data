import usa_spending_api

AID = usa_spending_api.api_pull.agency_aid()
# usa_spending_api.api_pull.agency_overview(aid_list=AID) 
federal_acc, treasury_acc = usa_spending_api.api_pull.agency_federal_account(aid_list=AID)
print(federal_acc)
print(treasury_acc)
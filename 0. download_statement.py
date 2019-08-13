from time import sleep
from finance_api import CnInfoAPI, DatabaseAPI

liu_lei_key, liu_lei_secret = 'fa4e980eaf7e4302811fb72336a648d0', '939563c8f0df4092b58823ae0d53ccb0'
liu_lei_account = CnInfoAPI(liu_lei_key, liu_lei_secret)
db = DatabaseAPI()

liu_lei_account.download_industry_lists(database_con=db.database, table_name=db.industry_table)

code_list = db.get_stock_codes(db.get_industry(stock_name='格力电器'))
liu_lei_account.download_statements(database_con=db.database, table_name=db.statement_table, code_list=code_list)
db.close()

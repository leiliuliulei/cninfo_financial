from finance_api import DatabaseAPI

db = DatabaseAPI()
mm = db.get_statement('贝因美', whole_industry=True)
print(mm)

db.close()

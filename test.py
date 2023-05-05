import datetime

hoje = datetime.datetime.now().date() - datetime.datetime(day=4, month=4, year=2023).date()
print(hoje)
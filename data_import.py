import csv
import json

import pandas as pd

import requests

# url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"
#
# headers = {
# 	"X-RapidAPI-Key": "40b27a5d62mshead33450c6e50ccp159aeejsn1841eea50cff",
# 	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
# }
#
# response = requests.request("GET", url, headers=headers)
#
# print(response.text)

company_name = ["ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS", "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH",
			   "AIN", "AIR", "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX"]

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
headers = {
		"X-RapidAPI-Key": "40b27a5d62mshead33450c6e50ccp159aeejsn1841eea50cff",
		"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
	}

for company in company_name:
	querystring = {"ticker_symbol":company,"years":"5","format":"json"}

	response = requests.request("GET", url, headers=headers, params=querystring)
	data = response.text
	df = pd.DataFrame(json.loads(data)["historical prices"])

	df.to_csv(f"/Users/ranadilendrasingh/PycharmProjects/python-spark-assignment/stock_data/{company}.csv",  encoding='utf-8')

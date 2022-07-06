from bs4 import BeautifulSoup
import requests
import re 
import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime


from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {'start_date': datetime(2020, 1, 1)}

#################################################       -WEBSCRAPE FUNCTION-       #################################################################
def scrape(date):
    urlpattern = "https://www.atptour.com/en/rankings/singles?rankRange=0-100&rankDate={}"
    url = urlpattern.format(date)
    headers={'User-Agent': ''}
    response = requests.get(url,timeout=15, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        rows = soup.find("div", {"class": "table-rankings-wrapper"}).find_all('tr') 
        lst = []
        for row in rows[1:]:
            
            #Ranking
            try:
                ranking = row.find("td", {"class": "rank-cell"}).get_text().strip()
            except:
                ranking = np.nan 
            
            #Country
            try:
                country = row.find("div", {"class": "country-item"}).find("img")['alt']
            except:
                country = np.nan
            
            #Player
            try:
                player = row.find("td", {"class": "player-cell"}).get_text().strip()
            except:
                player = np.nan
            
            #Age
            try:
                age = row.find("td", {"class": "age-cell"}).get_text().strip()
            except:
                age = np.nan
            
            #Points
            try:
                points = row.find("a", {"ga-label": "rankings-breakdown"}).get_text().strip()
            except:
                points = np.nan
            
            #Tournaments
            try:
                tournaments = row.find("td", {"class": "tourn-cell"}).get_text().strip()
            except:
                tournaments = np.nan
            
            
            temp = {
                "ranking": ranking,
                "country": country,
                "player": player,
                "age": age,
                "points": points,
                "tournaments": tournaments,
                "date": date
            }
            lst.append(temp)                
    else:
        print('Scraper is down!')
            
    return pd.DataFrame(lst)



##################################################################################################################################

url = "https://www.atptour.com/en/rankings/singles?rankRange=0-100&rankDate=2022-06-27"
headers={'User-Agent': ''}

# this will get me <Response [200]>
page = requests.get(url,timeout=15, headers= headers)
# print(page)

#main ranking table
soup = BeautifulSoup(page.content, "html.parser")
rows = soup.find("div", {"class": "table-rankings-wrapper"}).find_all('tr')

# grab most recent date
date = soup.find('div', class_= 'dropdown-layout-wrapper rank-detail-filter').find_all('ul')[2].find_all("li")[1].get_text().strip().replace(".", "-", 2)
print("date to be scraped:", date)


with DAG('user_processing', schedule_interval='@weekly', 
    default_args=default_args, catchup=False) as dag:

    webscraping = PythonOperator(
            task_id='webscraping',
            python_callable=scrape(date)
        )


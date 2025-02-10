#import pendulum
import datetime
import pymongo

from datetime import timedelta, datetime, tzinfo
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator 
#from airflow.providers.mongo.hooks.mongo
from airflow.hooks import base
from bs4 import BeautifulSoup, Comment  
from urllib.request import Request, urlopen
import json
import re
from pymongo import MongoClient
from fastapi import FastAPI


"""
app = FastAPI()
client = pymongo.MongoClient("mongodb://localhost:27017/")

@app.get("/api/v1/news/news-by-keyword")
def news_by_keyword(keyword:str):
    query = [
        {
            '$match': {
                'Title': re.compile(keyword)
            }
        }, {
            '$project': {
                '_id': 0
            }
        }
    ]
    #data = client.News.Kompas.aggregate(query)
    #data = client.News.Cnn.aggregate(query)
    #data = client.News.Tribun.aggregate(query)
    #data = client.News.Detik.aggregate(query)
    #data = client.News.Okezone.aggregate(query)
    data = client.crawler.news.aggregate(query)
    return {"data": list(data)}
"""

default_args = {
    'owner': 'Anom',
    'retries': 3,
    'retry_delay': timedelta(minutes=60)
}

dag = DAG('Crawling_News',
          default_args=default_args,
          description='CrawlingTest DAG',
          start_date= datetime(2021, 1, 1, tzinfo=='UTC'),  
          schedule='*/1 * * * *',
          catchup=False,
          max_active_tasks=1,
          tags=['example', 'Crawling'])


def get_database():
  client = pymongo.MongoClient("mongodb://localhost:27017/")
  return client['Crawler']

db = get_database()
news_db = db['News']
#kompas_db = db['Kompas']
#tribun_db = db['Tribun']
#detik_db = db['Detik']
#cnn_db = db['Cnn']
#okezone_db = db['Okezone']


def run_kompas():
  url = f"https://indeks.kompas.com/?site=all&date=2023-06-28&page=3"
  page = urlopen(url)
  html = page.read().decode("utf-8")
  soup = BeautifulSoup(html, "html.parser")
  paging = soup.find_all("div",{'class':'paging clearfix'})
  paging_link = paging[0].find_all('a',{'class':'paging__link'})
  last_page = int([item.get('href').split('/')[-1] for item in paging_link][-1].split('=')[-1])     
  data = []
  for page in range(1, last_page+1):
    print(f"https://indeks.kompas.com/?site=all&date=2023-06-28&page={page}")
    url = f"https://indeks.kompas.com/?site=all&date=2023-06-28&page={page}"
    page = urlopen(url)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.find_all("div", {"class":"article__list clearfix"})
    for article in articles:
      document = {
          "Title": article.find('h3', {'class':'article__title article__title--medium'}).text,
          "Url": article.find('div', {'class':'article__list__title'}).find('a')['href'],
          "Published_at": article.find('div', {'class':'article__date'}).text,
          "Tumb": article.find('div', {'class':'article__asset'}).find('img')['src']
          }
      data.append(document)
  #kompas_db.insert_many(data)
  news_db.insert_many(data)

  dt_now = datetime.now()
  print(f"crawling kompas succes in {dt_now}") 


with dag:
  kompas_task=PythonOperator(
    task_id='CrawlingKompas',
    execution_timeout=timedelta(minutes=5),
    python_callable=run_kompas
  )

  def run_tribun():
    url = "https://www.tribunnews.com/index-news?date=2023-6-28&page=4"
    page = urlopen(url)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    paging = soup.find_all("div", {"class":"paging"})
    paging_link = paging[0].find_all("a")
    last_page = int([item.get('href') for item in paging_link][-1].split('=')[-1])

    data = []
    for page in range(1, last_page+1):
      url = f"https://www.tribunnews.com/index-news?date=2023-6-28&page={page}"
      page = urlopen(url)
      html = page.read().decode("utf-8")
      soup = BeautifulSoup(html, "html.parser")
      articles = soup.find_all("li", {"class":"ptb15"})
      for article in articles:
        document = {
          "Title": article.find("h3", {"class":"f16 fbo"}).find('a')['title'],
          "Url": article.find("h3", {"class":"f16 fbo"}).find('a')['href'],
          "Published_at": article.find("time", {"class":"grey"}).text
          }
        data.append(document)
    #tribun_db.insert_many(data)
    news_db.insert_many(data)

    dt_now = datetime.now()
    print(f"crawling tribun succes in {dt_now}")


  tribun_task=PythonOperator(
    task_id='CrawlingTribunn',
    execution_timeout=timedelta(minutes=5),
    python_callable=run_tribun
  )

  def run_detik():
    url = f"https://news.detik.com/indeks/3?date=06/28/2023"
    page = urlopen(url)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    paging = soup.find_all("div", {"class":"pagination text-center mgt-16 mgb-16"})
    paging_link = paging[0].find_all("a", {"class":"pagination__item"})
    last_page = [int(re.search(r"/indeks/(\d+)\?", item['href']).group(1)) for item in paging_link if re.search(r"/indeks/(\d+)\?", item['href'])]
    data = []
    
    for page in range(1, max(last_page)+1):
      url = f"https://news.detik.com/indeks/{page}?date=06/28/2023"
      page = urlopen(url)
      html = page.read().decode("utf-8")
      soup = BeautifulSoup(html, "html.parser")
      articles = soup.find_all("article", {"class":"list-content__item"})
      
      for article in articles:
        document = {
          "Title": article.find('h3', {'class':'media__title'}).text.strip().title(),
          "Url": article.find('h3', {'class':'media__title'}).find('a')['href'],
          "Published_at": article.find('div', {'class':'media__date'}).text,
          "Tumb": article.find('div', {'class':'media__image'}).find('a')['href']
          }
        data.append(document)
    #detik_db.insert_many(data)
    news_db.insert_many(data)

    dt_now = datetime.now()
    print(f"crawling detik succes in {dt_now}")

  detik_task=PythonOperator(
    task_id='CrawlingDetik',
    execution_timeout=timedelta(minutes=5),
    python_callable=run_detik
  )

  def run_cnn():
    url = f"https://www.cnnindonesia.com/nasional/indeks/3?date=2023/06/28"
    page = urlopen(url)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    hal = soup.find_all("a",{"dtr-evt":"halaman"})
    last = []
    for h in hal:
      try:
        last_ = int(h["dtr-act"].split(' ')[1])
        last.append(last_)
      except:
        pass
    data = []
    
    for page in range(1, max(last)+1):
      url = f"https://www.cnnindonesia.com/nasional/indeks/3/{page}?date=2023/06/28"
      page = urlopen(url)
      html = page.read().decode("utf-8")
      soup = BeautifulSoup(html, "html.parser")
      articles = soup.find_all('article', {'class':''})
      
      for article in articles:
        document = {
          "Title": article.find('h2', {'class':'title'}).text,
          "Url": article.find('a')['href'],
          "Published_at": article.find('span', {'class':'date'}).find(string=lambda text: isinstance(text, Comment)),
          "Tumb": article.find('span', {'class':'ratiobox ratio_16_9 box_img'}).find('img')['src']
          }
        data.append(document)
    #cnn_db.insert_many(data)
    news_db.insert_many(data)

    dt_now = datetime.now()
    print(f"crawling cnn succes in {dt_now}")

  cnn_task=PythonOperator(
    task_id='CrawlingCnn',
    execution_timeout=timedelta(minutes=5),
    python_callable=run_cnn
  )

  def run_okezone():
    url = f"https://news.okezone.com/indeks/2023/06/28/150"
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" }
    req = Request(url, headers=headers) 
    page = urlopen(req)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    paging = soup.find('div', {'class':'pagination-komentar'})
    links = paging.find_all('a')
    last =[int(link['data-ci-pagination-page']) for link in links]
    hal = max(last)
    data = []
    
    for page in range(1, hal+1):
      url = f"https://news.okezone.com/indeks/2023/06/28/{page}0"
      headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
      req = Request(url, headers=headers) 
      page = urlopen(req)
      html = page.read().decode("utf-8")
      soup = BeautifulSoup(html, "html.parser")
      articles = soup.find_all('li', {'class':'col-md-12 p-nol m-nol hei-index'})
      
      for article in articles:
        document = {
          "Title": article.find('h4', {'class':'f17'}).text.strip().title(),
          "Url": article.find('h4', {'class':'f17'}).find('a')['href'],
          "Published_at": article.find('time', {'class':'category-hardnews f12'}).text.strip().splitlines()[1]
          }
        if document["Url"] not in [item["Url"] for item in data]:
          data.append(document)
    #okezone_db.insert_many(data)
    news_db.insert_many(data)

    dt_now = datetime.now()
    print(f"crawling okezone succes in {dt_now}")

  okezone_task=PythonOperator(
    task_id='CrawlingTribun',
    execution_timeout=timedelta(minutes=5),
    python_callable=run_okezone
  )

  kompas_task>>tribun_task>>detik_task>>cnn_task>>okezone_task
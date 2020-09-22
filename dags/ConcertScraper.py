from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import requests
import json
from bs4 import BeautifulSoup
import datetime

args = {
    "owner": "ian",
    "start_date": days_ago(1)
}

json_content = {
  "a-day-to-remember": "470482",
  "a-will-away": "4671508",
  "architects": "538395",
  "bring-me-the-horizon": "347077",
  "broadside": "355338",
  "circa-survive": "532805",
  "dance-gavin-dance": "348492",
  "dayseeker": "5537428",
  "deaf-havana": "973130",
  "don-broco": "2360321",
  "eluveitie": "281032",
  "enter-shikari": "16773",
  "falling-in-reverse": "1611290",
  "grayscale": "259932",
  "hands-like-houses": "4396173",
  "hot-mulligan": "8664924",
  "issues": "200182",
  "knuckle-puck": "4921853",
  "lonely-the-brave": "2374342",
  "neck-deep": "6054939",
  "parkway-drive": "514468",
  "seaway": "6036559",
  "selfish-things": "8956304",
  "senses-fail": "109675",
  "sleep-on-it": "7849589",
  "state-champs": "213767",
  "stories-untold": "762482",
  "taking-back-sunday": "117092",
  "the-menzingers": "675050",
  "the-story-so-far": "219967",
  "the-wonder-years": "589293",
  "thrice": "498261",
  "trophy-eyes": "6695469",
  "with-confidence": "8419158",
  "youth-fountain": "9506139"
}

dag = DAG(dag_id="ConcertScraperDAG",
          default_args=args, schedule_interval=None)


def parser(**kwargs):
    idDict = kwargs['idDict']
    msgSet = set()
    for k, v in idDict.items():
        url = f"https://www.songkick.com/artists/{v}-{k}/calendar"
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, "html")
        data = soup.findAll('script')

        allTourDates = soup.findAll('script', type="application/ld+json")
        for i in allTourDates:
            try:
                test = json.loads(i.text)
                address_info = test[0]['location']['address']
                if address_info['addressLocality'] == 'Brooklyn' or address_info['addressLocality'] == 'New York (NYC)':
                    date = datetime.datetime.strptime(
                        test[0]['endDate'], "%Y-%m-%d")
                    today = datetime.datetime.now()
                    date_diff = (date - today).days
                    if date_diff < 0:
                        break
                    else:
                        venue = test[0]['location']['name']
                        date = test[0]['endDate']
                        band = test[0]['name']
                        msg = f"{band} is playing at {venue} on {date}"
                        print(msg)
                        msgSet.add(msg)
            except:
                pass
    return msgSet


with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=parser,
        provide_context=False,
        op_kwargs={"idDict": json_content}
    )

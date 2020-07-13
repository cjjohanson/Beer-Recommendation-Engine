#import libraries
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import psycopg2
import config
from twilio.rest import Client

account_sid = config.TWILIO['ACCOUNT_SID']
auth_token = config.TWILIO['AUTH_TOKEN']
client = Client(account_sid, auth_token)

#establish connection to AWS RDS
conn = psycopg2.connect(
    host=config.RDS['RDS_HOST'],
    port=config.RDS['RDS_PORT'],
    user=config.RDS['RDS_USER'],
    password=config.RDS['RDS_PASSWORD'],
    dbname=config.RDS['RDS_DBNAME']
)

cur=conn.cursor()

cur.execute('''DROP TABLE IF EXISTS beers''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS beers
    (beer_id SERIAL PRIMARY KEY, beer_name text, brewery_name text, beer_style text,
    beer_substyle text, number_of_reviews integer, average_rating real, abv real, beer_name_link text,
    brewery_name_link text)
''')
#save query to variable in order to access each style's url
cur.execute('''SELECT * FROM beer_styles ''')
info = cur.fetchall()

#save base url for site and add relative path to have a full link as the result
ba_base_url = 'https://www.beeradvocate.com'

#create counter to increment/check on progress
counter = 0

for i_info in range(len(info)):
    req = requests.get(info[i_info][2])

    html = req.content

    soup = BeautifulSoup(html, 'lxml')

    table_rows = soup.find_all('tr')[3:-1]


    for i_table_row in range(len(table_rows)):

        temp_dict = dict()

        b_all = [item for item in table_rows[i_table_row].find_all('b')]
        span_all = [item for item in table_rows[i_table_row].find_all('span')]
        a_all = [item for item in table_rows[i_table_row].find_all('a')]

        temp_dict['beer_name'] = b_all[0].text
        temp_dict['brewery_name'] = a_all[1].text
        temp_dict['beer_style'] = info[i_info][0]
        temp_dict['beer_substlye'] = info[i_info][1]
        temp_dict['number_of_reviews'] = int(b_all[1].text.replace(',', ''))
        temp_dict['average_rating'] = float(b_all[2].text)
        try:
            temp_dict['abv'] = float(span_all[0].text)
        except:
            temp_dict['abv'] = 0
        temp_dict['beer_name_link'] = urljoin(ba_base_url, a_all[0]['href'])
        temp_dict['brewery_name_link'] = urljoin(ba_base_url, a_all[1]['href'])



        cur.execute('''
            INSERT INTO beers (beer_name, brewery_name, beer_style, beer_substyle,
                                number_of_reviews, average_rating, abv, beer_name_link, brewery_name_link )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (temp_dict['beer_name'], temp_dict['brewery_name'], temp_dict['beer_style'],
            temp_dict['beer_substlye'], temp_dict['number_of_reviews'] ,temp_dict['average_rating'],
            temp_dict['abv'], temp_dict['beer_name_link'], temp_dict['brewery_name_link']))

    counter += 1
    if counter % 500 == 0:
        conn.commit()

    print(f"{counter} COMPLETE: {info[i_info][:2]}")
    time.sleep(3)

conn.commit()

message = client.messages.create(
    to = config.cell,
    from_ = config.twilio_number,
    body = 'Beers table completed.'
)

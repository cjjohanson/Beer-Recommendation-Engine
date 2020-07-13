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

#create beer_styles table
cur.execute('''
    CREATE TABLE IF NOT EXISTS beer_styles
    (beer_style text, beer_substyle text, beer_substyle_link text)
''')


beer_styles_url = 'https://www.beeradvocate.com/beer/styles/'

req = requests.get(beer_styles_url)

html = req.content

soup = BeautifulSoup(html, 'lxml')

#get divs for each style section (i.e., Bocks, Brown Ales, etc.)
style_divs = soup.find_all('div', {'class' : 'stylebreak'})

#save base url to join relative paths to for a full link as result
ba_base_url = 'https://www.beeradvocate.com'

#create empty list and append each beer
beer_substyle_list = []

for i in range(len(style_divs)):

    beer_style = style_divs[i].find('b').text


    #save all the a_tags since thats where the info is in the HTML
    a_tags = style_divs[i].find_all('a')
    for i in range(len(a_tags)):
        #form a dictionary to organize the data
        temp_dict = dict()

        temp_dict['beer_style'] = beer_style
        temp_dict['beer_substyle'] = a_tags[i].text
        temp_dict['beer_substyle_link'] = urljoin(ba_base_url, a_tags[i]['href'])

        #add each entry to a list
        beer_substyle_list.append(temp_dict)

        #insert data into table
        cur.execute('''
            INSERT INTO beer_styles VALUES (%s, %s, %s)
        ''', (temp_dict['beer_style'], temp_dict['beer_substyle'], temp_dict['beer_substyle_link']))

#commit changes to database table
conn.commit()

message = client.messages.create(
    to = config.cell,
    from_ = config.twilio_number,
    body = 'Beer styles completed.'
)

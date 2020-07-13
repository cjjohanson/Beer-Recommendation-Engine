#import libraries
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import psycopg2
import config
from twilio.rest import Client
from urllib.parse import urljoin
from datetime import datetime

#function to increment start parameter for review pages by 25
#this allows you to get to the "next" page
def reviews_incrementer(url):
    param_len = len('start=')
    number_pos = url.find('start=')
    current_start_point = url[number_pos + param_len:]
    next_start_point = str(int(current_start_point) + 25)

    next_site = url[:-len(current_start_point)] + next_start_point

    return next_site

#credentials for text messaging service
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

#execute SQL code
cur=conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS ratings
    (rating_id SERIAL PRIMARY KEY, username text, rating_date date, rating real,
    beer_name text, brewery_name text, beer_style text, beer_substyle text, abv real)
''')

cur.execute('''
    SELECT * FROM beers WHERE beer_id >= 4810
''')

beers = cur.fetchall()


#test list to append data to, use it to double check results will be written
#so SQL table correctly
testing_list = []

#website login information
login_info = {'login' : config.SITE_LOGIN['login'], 'password' : config.SITE_LOGIN['password']}

#establish a session so you can login
#you have to be logged in to view all ratings
with requests.session() as session:
    post = session.post(config.SITE_LOGIN['url'], data=login_info)
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15'})

    #save cookies to variable and add cookies that shows all ratings
    session_cookies = session.cookies.get_dict()
    session_cookies['hideRatings'] = 'N'

    #this is None since the first link you use will not be incremented
    site_incremented = None
    #query parameter to add to each beer link
    url_params = '?view=beer&sort=&start=0'

    #iterate through all the beers (from the beers table)


    for i in range(len(beers)):
        start_site = urljoin(beers[i][8], url_params)

        counter = 0
        while True:
            counter += 1
            #for each beer, the first site will be 'www.website.com/?view=beer&sort=&start=0'
            #after the first site, you'll be using the incrementer to see next 25 results
            if counter == 1:
                req = session.post(start_site, cookies=session_cookies)
            else:
                req = session.post(site_incremented, cookies=session_cookies)

            html = req.content
            soup = BeautifulSoup(html, 'lxml')

            #isolate all divs that contain ratings infomation
            #each div representings all info for ONE user's rating
            ratings_divs = soup.find_all('div', {'id' : 'rating_fullview_content_2'})

            if len(ratings_divs) > 0 and counter == 1:
                site_incremented = reviews_incrementer(start_site)
            elif len(ratings_divs) > 0 and counter > 1:
                site_incremented = reviews_incrementer(site_incremented)
            else:
                time.sleep(2)
                break


            for i_div in range(len(ratings_divs)):
                temp_dict = {}

                #isolate a tags and span tags (these contain the info you want)
                a_all = ratings_divs[i_div].find_all('a')
                span_all = ratings_divs[i_div].find_all('span', {'class' : 'BAscore_norm'})

                #save info to variables so it can be written to SQL table
                temp_dict['username'] = a_all[0].text
                try:
                    temp_dict['rating_date'] = datetime.strptime(a_all[1].text.replace(',', ''), '%b %d %Y')
                except:
                    temp_dict['rating_date'] = None
                temp_dict['rating'] = float(span_all[0].text)
                temp_dict['beer_name'] = beers[i][1]
                temp_dict['brewery_name'] = beers[i][2]
                temp_dict['beer_style'] = beers[i][3]
                temp_dict['beer_substyle'] = beers[i][4]
                temp_dict['abv'] = beers[i][7]

                testing_list.append(temp_dict)


                cur.execute('''
                INSERT INTO ratings (username, rating_date, rating, beer_name, brewery_name,
                beer_style, beer_substyle, abv)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (temp_dict['username'], temp_dict['rating_date'], temp_dict['rating'],
                temp_dict['beer_name'], temp_dict['brewery_name'] ,temp_dict['beer_style'],
                temp_dict['beer_substyle'], temp_dict['abv']))

                conn.commit()

            time.sleep(2)


    #wait 2 seconds after each beer
    time.sleep(2)

message = client.messages.create(
    to = config.cell,
    from_ = config.twilio_number,
    body = 'Ratings table complete'
)

conn.close()

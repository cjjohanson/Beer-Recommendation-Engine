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

cur = conn.cursor()
cur.execute('''DROP TABLE IF EXISTS breweries''')
cur.execute('''
    CREATE TABLE breweries (brewery_id SERIAL PRIMARY KEY, brewery_name text,
    address text, beer_stats_average real, beer_stats_beers_count integer,
    beer_stats_reviews_count integer, beer_stats_ratings_count integer,
    place_stats_average real, place_stats_reviews_count integer,
    place_stats_ratings_count integer)
''')

cur.execute('''
    SELECT * FROM beers
''')
#DELETE THIS BEFORE CONTINUING
conn.commit()

beer_info = cur.fetchall()

cur.execute('''
    SELECT DISTINCT beer_substyle FROM beer_styles
''')

#beer styles are Bocks, Brown Ales, etc
beer_styles = cur.fetchall()

#create empty list for brewery names. if a name isn't in list, add it to SQL table
brewery_names = []


with requests.session() as session:

    session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15'})

    counter = 0
    for style in beer_styles:

        for beer in beer_info:
            if beer[2] not in brewery_names and beer[4] == style[0]:
                #connect to the page
                brewery_req = requests.get(beer[9])
                html = brewery_req.content
                soup = BeautifulSoup(html, 'lxml')

                #grab the divs that have brewery info
                id_divs = soup.find_all('div', {'id' : 'info_box'})

                #add the address if its available, if its not available make it NULL

                a_all = [div.find_all('a') for div in id_divs][0]

                try:
                    map_links_list = [item['href'] if item['href'].startswith('https://maps.google.com/') == True else None for item in a_all]
                    map_link = [item for item in map_links_list if item != None][0]

                    address = map_link[map_link.find('q=')+2:].replace('%2C', ',').replace('+', ' ').replace('%', ' ')
                    #if the address is too short, then its cant be an address
                    #in that case, make it None so SQL will recognize it as NULL
                    if len(address) > 10:
                        address = address
                    else:
                        address = None
                except:
                    address = None


                #save brewery name
                brewery_name = soup.find('h1').text

                #save the beer stats and place stats
                #not all breweries have place stats so make it None/NULL when that happens
                stats_div = soup.find_all('div', {'id' : 'item_stats'})
                beer_stats_div = stats_div[0]
                beer_stats_dd_all = (beer_stats_div.find_all('dd'))
                beer_stats_average = beer_stats_dd_all[0].text
                beer_stats_beers_count = int(beer_stats_dd_all[1].text.replace(',',''))
                beer_stats_reviews_count = int(beer_stats_dd_all[2].text.replace(',',''))
                beer_stats_ratings_count = int(beer_stats_dd_all[3].text.replace(',',''))

                try:
                    place_stats_div = stats_div[1]
                    place_stats_span_all = place_stats_div.find_all('span')[:-1]
                    place_stats_a_all = place_stats_div.find_all('a')
                    place_stats_average = place_stats_a_all[1].text
                    place_stats_reviews_count = int(place_stats_span_all[0].text.replace(',',''))
                    place_stats_ratings_count = int(place_stats_span_all[1].text.replace(',',''))
                except:
                    place_stats_div = None
                    place_stats_average = None
                    place_stats_reviews_count = None
                    place_stats_ratings_count = None

                #add data to SQL breweries table
                cur.execute('''
                    INSERT INTO breweries (brewery_name, address, beer_stats_average, beer_stats_beers_count, beer_stats_reviews_count,
                    beer_stats_ratings_count, place_stats_average, place_stats_reviews_count, place_stats_ratings_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (brewery_name, address, beer_stats_average, beer_stats_beers_count, beer_stats_reviews_count,
                    beer_stats_ratings_count, place_stats_average, place_stats_reviews_count, place_stats_ratings_count))

                #add the brewery name to the names list to make sure you dont add duplicates in table
                brewery_names.append(beer[2])

                #wait three seconds between each call
                time.sleep(3)

        # counter += 1
        # if counter % 1000 == 0:
        #     conn.commit()

    #save SQL table
    conn.commit()

message = client.messages.create(
    to = config.cell,
    from_ = config.twilio_number,
    body = 'Breweries table complete'
)

conn.close()

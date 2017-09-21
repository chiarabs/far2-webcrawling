#!/usr/bin/python

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError
<<<<<<< HEAD
=======

>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
import requests
import re
import psycopg2

base_url='http://www.booking.com'

def crawler(location,day_in,day_out):

#for the specific location,day_in(yyy-mm-dd),day_out(yyyy-mm-dd) return a list of class object: hotel_list=[hotel_table(name,av_rating,price)]
    dest_id='-110502' #Aosta

    class hotel_table(object):
<<<<<<< HEAD
        def __init__(self, name,address,av_rating,price):
            self.name = name
            self.address=address
            self.av_rating=av_rating
            self.price = price
    
=======
        def __init__(self, name,av_rating,price):
            self.name = name
            self.av_rating=av_rating
            self.price = price
    

>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
    hotel_list=[]

    #setting random user agent
    #ua = UserAgent()
    try:
        ua = UserAgent()
    except FakeUserAgentError:
        print("Connection error, please verify your connection")
        return

<<<<<<< HEAD
    headers={'User-Agent': ua.random}
    print (headers)
    
  
=======
    #setting random user agent 
    ua=UserAgent()
    headers={'User-Agent': ua.random}
    print (headers)
    
    day_in=research_options[0]
    day_out=research_options[3]
    month_in=research_options[1]
    month_out=research_options[4]
    year_in=research_options[2]
    year_out=research_options[5]

>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
    #loop  over all the pages containing research results iterating on the URL offset parameter
    #loop  stops when it finds error message "take control of your search"

    for i in range(0,1): #set the number of visited pages 
        offset=i*30
        url='https://www.booking.com/searchresults.it.html?;sid=1542c45b9ca387a9d5dc25d864e1ee17;&checkin_monthday=20&checkin_month=9;checkin_year=2017;checkout_monthday=21;checkout_month=9;checkout_year=2017;dest_id='+dest_id+';ss='+location+';dest_type=city;sb_travel_purpose=leisure;no_rooms=1;group_adults=2;group_children=0;offset='+str(offset)


        print (url)

        try:
            response = requests.get(url, headers=headers)
<<<<<<< HEAD
        except requests.exceptions.RequestException as e:  
=======
        except requests.exceptions.RequestException as e:  # This is the correct syntax
>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
            print (e)
            return
        
        response = requests.get(url,headers=headers).text
        soup = bs(response,'lxml')
        if soup.get('class') == 'Take Control of Your Search':
            break


        #search in each hotel link the respective name, average rating and price
        for link in soup.find_all('a',class_='hotel_name_link url'):
            hotel_link=link['href']
            hotel_link=base_url+hotel_link
            print(hotel_link)

<<<<<<< HEAD
            #find hotel destination id
            dest_id=hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')]
            print(dest_id)
            try:
                response = requests.get(url, headers=headers)
            except requests.exceptions.RequestException as e:  
=======
            try:
                response = requests.get(url, headers=headers)
            except requests.exceptions.RequestException as e:  # This is the correct syntax
>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
                print (e)
                return

            response = requests.get(hotel_link, headers=headers).text.strip('\t\r\n')
            soup = bs(response,'lxml')#.encode('utf-8')
            if soup.find('h2', class_='hp__hotel-name'):
                hotel_name=soup.find('h2', class_='hp__hotel-name').text.strip('\t\r\n')
            else:
                hotel_name= None

            if soup.find('span', class_='hp_address_subtitle'):
                hotel_address=soup.find('span', class_='hp_address_subtitle').text.strip('\t\r\n')
            else:
                hotel_address=None

            av_rating=soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
            #av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
<<<<<<< HEAD

            if soup.find('span', class_='hprt-price-price-standard'):
                price=soup.find('span', class_='hprt-price-price-standard')#.select('b')
                price = price.text.strip('\t\r\n\€xa')
=======
            if soup.select_one('strong.availprice'):
                price=soup.select_one('strong.availprice').select('b')
                price = price[0].text.strip('\t\r\n\€xa')
>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
                price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
            else :
                price= None
            
<<<<<<< HEAD
            #script=soup.find_all('script', type_='application/ld+jso')
=======

>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566
            #text_file = open("Output_soup.txt", "w")
            #text_file.write(str(script))
            #text_file.close()

<<<<<<< HEAD
            print(hotel_name)
            print(hotel_address)
            print(av_rating)
            print(price)

            hotel_list.append(hotel_table(hotel_name,hotel_address,av_rating,price))

            for i in hotel_list :
                print (i.name)
            #return (hotel_list)

    return (hotel_list)


def db_hotel_name_update(new_name) :
    #connect to database and insert new row data if not present
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    SQL = '''BEGIN;
             INSERT INTO hotels (name)
	     SELECT %s
	     WHERE NOT EXISTS (
	     SELECT * FROM hotels WHERE name=%s
	     );
             COMMIT;'''

    data = (new_name,new_name, )

    cur.execute(SQL, data)

    cur.close()
    conn.close()

#research_options
day_in='2017-09-20'
day_out='2017-09-21'



hotel_list=crawler('Aosta',day_in,day_out)
=======
            #print(hotel_name)
            #print(av_rating)
            #print(price)
            hotel_list.append(hotel_table(hotel_name,av_rating,price))

            for i in hotel_list :
                print (i.name)

    return (hotel_list)


#research_options=[day_in,month_in,year_in,day_out,month_out,year_out]
research_options=[20,9,2017,21,9,2017]
>>>>>>> c8c2a1827e13d4ac30e48dac21693d2df1c26566

#db updating hotels table
for i in hotel_list:
    db_hotel_name_update(i.name)



#for script in scripts:
#   if(pattern.match(str(script.string))):
#       data = pattern.match(script.string)
#       stock = json.loads(data.groups()[0])
#       print stock

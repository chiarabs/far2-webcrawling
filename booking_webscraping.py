#!/usr/bin/python

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError
import requests
import re
import psycopg2
import argparse




base_url='http://www.booking.com'

def crawler(location,day_in,day_out):

#for the specific location,day_in(yyy-mm-dd),day_out(yyyy-mm-dd) return a list of class object: hotel_list=[hotel_table(name,av_rating,price)]
    dest_id='-110502'

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--scraping_type", action="count", default=0)
    args = parser.parse_args()
    
    
    #setting random user agent
    #ua = UserAgent()
    try:
        ua = UserAgent()
    except FakeUserAgentError:
        print("Connection error, please verify your connection")
        return

    headers={'User-Agent': ua.random}
    print (headers)
    
  
    #loop  over all the pages containing research results iterating on the URL offset parameter
    #loop  stops when it finds error message "take control of your search"

    for i in range(0,1): #set the number of visited pages 
        offset=i*30
        url='https://www.booking.com/searchresults.it.html?;sid=1542c45b9ca387a9d5dc25d864e1ee17;&checkin_monthday=20&checkin_month=9;checkin_year=2017;checkout_monthday=21;checkout_month=9;checkout_year=2017;dest_id='+dest_id+';ss='+location+';dest_type=city;sb_travel_purpose=leisure;no_rooms=1;group_adults=2;group_children=0;offset='+str(offset)
                
        print (url)

        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:  
            print (e)
            return
        
        response = requests.get(url,headers=headers).text
        soup = bs(response,'lxml')
        if soup.get('class') == 'Take Control of Your Search':
            break

        #loop over hotel  links to get each hotel_soup
        for link in soup.find_all('a',class_='hotel_name_link url'):
            hotel_link=link['href']
            hotel_link=base_url+hotel_link
            print(hotel_link)
            try:
                response = requests.get(url, headers=headers)
            except requests.exceptions.RequestException as e:  
                print (e)
                return
            response = requests.get(hotel_link, headers=headers).text.strip('\t\r\n')
            soup = bs(response,'lxml')#.encode('utf-8')
   
            #script=soup.find_all('script', type_='application/ld+jso')
            text_file = open("Output_soup.txt", "w")
            text_file.write(str(soup))
            text_file.close()

            if args.scraping_type >= 2:
                print("type of scraping: hotel_table_update")
                hotel_table_list=[] 
                hotel_table=hotel_table_update(hotel_link,soup)
                hotel_table_list.append(hotel_table)
 
            if args.scraping_type < 2:   
                print("type of scraping: hotel_data_update ")
                hotel_data_list=[]
                hotel_data=hotel_data_update(day_in,day_out,hotel_link,soup)
                hotel_data_list.append(hotel_data)
        

##################################################################################################

def hotel_table_update(hotel_link,hotel_soup) :
#find hotel destination id,name,address  

    class hotel_table(object):
        def __init__(self,name,address,dest_id): #add hotel id
            self.name = name
            self.address=address
            self.dest_id=dest_id
               
    dest_id=hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')]

    #add hotel_id=hotel.soup.........

    if hotel_soup.find('h2', class_='hp__hotel-name'):
        hotel_name=hotel_soup.find('h2', class_='hp__hotel-name').text.strip('\t\r\n')
    else:
        hotel_name= None
    print(hotel_name)
                      
    if hotel_soup.find('span', class_='hp_address_subtitle'):
        hotel_address=hotel_soup.find('span', class_='hp_address_subtitle').text.strip('\t\r\n')
    else:
        hotel_address=None
    print(hotel_address)

    return hotel_table(hotel_name,hotel_address,dest_id)

     

##################################################################################################

def hotel_data_update (day_in,day_out,hotel_link,hotel_soup):

    class hotel_data(object):
        def __init__(self,day_in,day_out,price,av_rating): #add hotel id
            self.day_in=day_in
            self.day_out=day_out
            self.price=price
            self.av_rating=av_rating

    av_rating=hotel_soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
    #av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
    print(av_rating)
    if hotel_soup.find('span', class_='hprt-price-price-standard'):
        price=hotel_soup.find('span', class_='hprt-price-price-standard')#.select('b')
        price = price.text.strip('\t\r\n\€xa')
        price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
    else :
        price= None
    print(price)       
    
    return hotel_data(day_in,day_out,price,av_rating)

##################################################################################################
                        
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

####################################################################################

#research_options
day_in='2017-09-20'
day_out='2017-09-21'

crawler('Aosta',day_in,day_out)

#db updating hotels table
#for i in hotel_list:
#    db_hotel_name_update(i.name)



#for script in scripts:
#   if(pattern.match(str(script.string))):
#       data = pattern.match(script.string)
#       stock = json.loads(data.groups()[0])
#       print stock




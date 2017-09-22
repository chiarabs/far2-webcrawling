#!/usr/bin/python

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError
import requests
import re
import psycopg2
import argparse
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--scraping_type", action="count", default=0)
args = parser.parse_args()
    


base_url='http://www.booking.com'

def crawler(location,day_in,day_out):

#for the specific location,day_in(yyy-mm-dd),day_out(yyyy-mm-dd) return a list of class object
    
    dest_id='-110502'
    
    monthday_in=day_in.day
    month_in=day_in.month
    year_in=day_in.year
    monthday_out=day_out.day
    month_out=day_out.month
    year_out=day_out.year
    
    #setting random user agent
    #ua = UserAgent()
    try:
        ua = UserAgent()
    except FakeUserAgentError:
        print("Connection error, please verify your connection")
        return

    headers={'User-Agent': ua.random}
    print (headers)
    
    hotel_list=[]
  
    #loop  over all the pages containing research results iterating on the URL offset parameter
    #loop  stops when it finds error message "take control of your search"
    offset_range=0 
    for i in range(0,2): #set the number of visited pages 
        offset=i*offset_range
        print(offset)
        url='https://www.booking.com/searchresults.it.html?;&checkin_monthday='+str(monthday_in)+'&checkin_month='+str(month_in)+';checkin_year='+str(year_in)+';checkout_monthday='+str(monthday_out)+';checkout_month='+str(month_out)+';checkout_year='+str(year_out)+';dest_id='+dest_id+';ss='+location+';dest_type=city;sb_travel_purpose=leisure;no_rooms=1;group_adults=2;group_children=0;offset='+str(offset)
                
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

        last_show=int(soup.find('span', class_='sr_showed_amount_last').text.strip('\t\r\n'))
        print(last_show)
        first_show=(soup.find('span' ,class_='sr_showed_amount').text)
        first_show=[int(s) for s in first_show.split() if s.isdigit()][0]
        print(first_show)
        offset_range=last_show-(first_show-1)
        print(offset_range)

        #loop over hotel  links to get each hotel_soup
        for link in soup.find_all('a',class_='hotel_name_link url'):
            hotel_link=link['href']
            hotel_link=base_url+hotel_link
            #print(hotel_link)
            try:
                response = requests.get(url, headers=headers)
            except requests.exceptions.RequestException as e:  
                print (e)
                return
            response = requests.get(hotel_link, headers=headers).text.strip('\t\r\n')
            soup = bs(response,'lxml')#.encode('utf-8')
   
            #script=soup.find_all('script', type_='application/ld+jso')
            #text_file = open("Output_soup.txt", "w")
            #text_file.write(str(soup))
            #text_file.close()

    

            if args.scraping_type == 2:
                hotel_table=hotel_table_update(hotel_link,soup)
                hotel_list.append(hotel_table)
            
 
            elif args.scraping_type < 2:   
                hotel_data=hotel_data_update(day_in,day_out,hotel_link,soup)
                hotel_list.append(hotel_data)
            
            elif args.scraping_type == 3:
                hotel_review_list=hotel_review_update(headers,soup)
                hotel_list.append(hotel_review_list)
        
    return hotel_list

##################################################################################################

def hotel_table_update(hotel_link,hotel_soup) :
#find hotel destination id,name,address  

    class hotel_table(object):
        def __init__(self,hotel_id,name,address,dest_id): 
            self.hotel_id=hotel_id
            self.name = name
            self.address=address
            self.dest_id=dest_id

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
               
    dest_id=int(hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')])

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

    return hotel_table(hotel_id,hotel_name,hotel_address,dest_id)

     

##################################################################################################

def hotel_data_update (day_in,day_out,hotel_link,hotel_soup):

    class hotel_data(object):
        def __init__(self,hotel_id,day_in,day_out,price,av_rating):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.price=price
            self.av_rating=av_rating
            self.search_date=(datetime.datetime.now())

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])

    av_rating=hotel_soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
    av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
    print(av_rating)
    if hotel_soup.select_one('strong.js-track-hp-rt-room-price'):
        price=hotel_soup.select_one('strong.js-track-hp-rt-room-price') 
        price = price.text.strip('\t\r\n\€xa')
        price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
    elif hotel_soup.find('span', class_='hprt-price-price-standard'):
         price=hotel_soup.find('span', class_='hprt-price-price-standard')
         price = price.text.strip('\t\r\n\€xa')
         price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
    else :
        text_file = open("soup_error_price.txt", "w")
        text_file.write(str(hotel_soup))
        text_file.close()
        price= None
    print(price)       
    
    return hotel_data(hotel_id,day_in,day_out,price,av_rating)

##################################################################################################

def hotel_review_update (headers, hotel_soup) :
    
    class hotel_review(object):
        def __init__(self,hotel_id,pos_comment,neg_comment,post_date,author):
            self.hotel_id=hotel_id
            self.pos_comment=pos_comment
            self.neg_comment=neg_comment
            self.post_date=post_date
            self.author=author
    hotel_review_list=[]

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])

    link_to_rev=hotel_soup.find('a', class_='show_all_reviews_btn')['href']

    link_to_rev=base_url+link_to_rev
    print(link_to_rev)

    for i in range (1,2): #set number of reviews visited pages

        #(change N relative of the page during iteration: string-substitution "pageN" with "page"+i+")
        link_to_rev=link_to_rev+';page='+str(i)

        print (link_to_rev)

        response = requests.get(link_to_rev,headers=headers).text
        review_soup=bs(response,'lxml')

        for element in review_soup.find_all('p', class_='review_neg'):
            neg_comment=element.text #to do: remove 눇
        
        for element in review_soup.find_all('p', class_='review_pos'):
            pos_comment = element.text
    
        #post_date = datetime.date(review_soup.find(.......))

        #author = review_soup.find(.......)

        hotel_review_list.append(hotel_review(hotel_id,pos_comment,neg_comment,post_date,author))
    
    return hotel_review_list
            
##################################################################################################
                        
def db_hotel_list_update(hotel_table_list) :
    #connect to database "webcrawling", insert new row data in hotel_list table  if hotel_id is not present
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()
    
    for i in hotel_table_list:
        hotel_id=i.hotel_id
        name=i.name
        hotel_address=i.address
        location=i.dest_id

        SQL = '''BEGIN;
                 INSERT INTO hotel_list (name,location,hotel_address,hotel_id)
	         SELECT %s,%s,%s,%s
	         WHERE NOT EXISTS (SELECT hotel_id  FROM hotel_list WHERE hotel_id=%s
	         );
                 COMMIT;'''

        data = (name,location,hotel_address,hotel_id,hotel_id)

        cur.execute(SQL, data)

    cur.close()
    conn.close()

def db_hotel_data_update(hotel_data_list) :
    #connect to database "webcrawling", insert new row data in hotel_data table
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    for i in hotel_data_list:
        hotel_id=i.hotel_id
        day_in=i.day_in
        day_out=i.day_out
        price=i.price
        av_rating=i.av_rating
        search_date=i.search_date
    
        SQL = '''BEGIN;
                 INSERT INTO hotel_data (hotel_id,day_in,day_out,price,av_rating,search_date)
	         VALUES (%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

        data = (hotel_id,day_in,day_out,price,av_rating,search_date )

        cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################

def  db_hotel_reviews_update(hotel_reviews_list):
#connect to database "webcrawling", insert new row data in hotel_data table
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    for i in hotel_reviews_list:
        hotel_id=i.hotel_id
        pos_comment=i.pos_comment
        neg_comment=i.neg_comment
        post_date=i.post_date
        author=i.author
    
        SQL = '''BEGIN;
                 INSERT INTO hotel_reviews (hotel_id,) 
	         VALUES (%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''
############### TO COMPLETEEEEEEE
        data = (hotel_id,day_in,day_out,price,av_rating,search_date )

        cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################
#research_options
day_in_str='2017-09-23'
day_out_str='2017-09-24'



day_in = datetime.datetime.strptime(day_in_str,'%Y-%m-%d')
day_out= datetime.datetime.strptime(day_out_str, '%Y-%m-%d')

if args.scraping_type == 2:
    print("type of scraping: hotel_table_update")
    hotel_table_list=crawler('Aosta',day_in,day_out)
    #db updating hotel_list in webcraling postgress db
    db_hotel_list_update(hotel_table_list)

elif args.scraping_type < 2:   
    print("type of scraping: hotel_data_update ")
    hotel_data_list=crawler('Aosta',day_in,day_out)
    #db updating hotel_data in webcrawling postgress db
    db_hotel_data_update(hotel_data_list)

elif args.scraping_type == 3:   
    print("type of scraping: hotel_reviews_update ")
    hotel_review_list=crawler('Aosta',day_in,day_out)
    #db updating hotel_reviews in webcrawling postgress db
    db_hotel_reviews_update(hotel_reviews_list)

else:
    print("chose type of scraping")


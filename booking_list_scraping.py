#!/usr/bin/python3

"""
======================================================
               Booking.com list hotel scraping
======================================================

Starting from a hotel list, hotel names, data, ratings and reviews are collected from booking.com and stored in a postgres database: 4 type of scraping are possible, corresponding to the database tables update

-t for hotel_list update
-tt for hotel_data update
-ttt for hotel rating update
-tttt for hotel_reviews update

If connection db is not present yet, new_db.py will be run and a psql database will be created.

Required packages: BeautifulSoup, request, fake_useragent, psycopg2.  

"""

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError
import requests
import re
import psycopg2
import argparse
import datetime
import time
import os
import locale
locale.setlocale(locale.LC_ALL, 'it_IT.utf8')

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--scraping_type", action="count", default=0,help='select the type of scraping: -t for hotel_list update (get hotel names), -tt for hotel_data update (get hotel_id, price, average rating), -ttt for hotel_ratings update, -tttt fot hotel_reviews update (get users comments) ')
args = parser.parse_args()
    
base_url='http://www.booking.com'

def crawler(link_list,day_in,day_out):
    
    ti=time.time()

    base_url='http://www.booking.com'
    
    global n_link
    
    #cleaning error msg
    try:
        os.remove('ConnectionError_msg.txt') 
    except:
        pass
    try:
        os.remove('Error_msg.txt')
    except:
        pass

    #setting random user agent
    #ua = UserAgent()
    try:
        ua = UserAgent()
    except FakeUserAgentError:
        print("\nConnection error, please verify your connection")
        return 'connection error'

    headers={'User-Agent': ua.random}
    print (headers)
  
    #loop  over all the link contained in link_list
    for n_link, link in enumerate(link_list,n_link): 
        resume('w',str(n_link))
        print(link[0])
        print(n_link)

        hotel_link=base_url+link[0]+'?;checkin='+str(day_in)+';checkout='+str(day_out)
        print (hotel_link)
        
        n_iter=1
        max_it=10
        while n_iter<max_it:
            try:
                response = requests.get(hotel_link, headers=headers).text
                break
            except requests.exceptions.RequestException as e:  
                print (e,' attempt n. ',max_it)
                text_file = open("ConnectionError_msg.txt", "a")
                text_file.write('\nConnection error: %s'%e)
                text_file.close()
                n_iter+=1
                time.sleep(10)
        if n_iter==10:
            return 'connection error'

        soup = bs(response,'lxml')

        if args.scraping_type < 2:
            hotel_table_check=hotel_table_update(hotel_link,link[0],None,soup)
            if hotel_table_check==0:
                text_file = open("Error_msg.txt", "a")
                text_file.write('Hotel_table_update: error getting data for %s'%hotel_link)
                text_file.close()
                return 0
            else:
                continue

        if args.scraping_type == 2:   
            hotel_data_check=hotel_data_update(day_in,day_out,soup)
            if hotel_data_check==0:
                text_file = open("Error_msg.txt", "a")
                text_file.write('\nHotel_data_update: error getting data for %s '%hotel_link)
                text_file.close()
                return 0
            else:
                continue

        elif args.scraping_type == 3:   
            hotel_ratings_check=hotel_ratings_update(day_in,day_out,soup)
            if hotel_ratings_check==0:
                text_file = open("Error_msg.txt", "a")
                text_file.write('\nHotel_ratings_update: error getting data for %s'%hotel_link)
                text_file.close()
                return 0
            else:
                continue

        elif args.scraping_type == 4:
            hotel_reviews_check=hotel_reviews_update(headers,soup)
            if hotel_reviews_check==0:
                text_file = open("Error_msg.txt", "a")
                text_file.write('\nHotel_reviews_update: error getting data for %s'%hotel_link)
                text_file.close()
                return 0
            else:
                continue

    print("scraping done in %0.3fs" % (time.time() - ti))
    return 1

##################################################################################################

def hotel_table_update(hotel_link,link,hotel_type,hotel_soup) :
#find hotel destination id,name,address  

    class hotel_table(object):
        def __init__(self,hotel_id,hotel_type,hotel_name,hotel_address,dest_id,hotel_star,hotel_facilities): 
            self.hotel_id=hotel_id
            self.hotel_type=hotel_type
            self.hotel_name = hotel_name
            self.hotel_address=hotel_address
            self.dest_id=dest_id
            self.hotel_star=hotel_star
            self.hotel_facilities=hotel_facilities
           
    t0 = time.time()
    
    try:
        hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
        print(hotel_id)
    except:
        print('Error hotel_id')
        hotel_id=None
    try:
        dest_id=int(hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')])
    except:
        print(hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')])
        dest_id=None
    if hotel_soup.find('h2', class_='hp__hotel-name'):
        hotel_name=hotel_soup.find('h2', class_='hp__hotel-name').text.strip('\t\r\n')
    elif  hotel_soup.find('span', class_='sr-hotel__name') :
        hotel_name=hotel_soup.find('span', class_='sr-hotel__name').text.strip('\t\r\n')
    else:
        hotel_name= None
    print(hotel_name)
                      
    if hotel_soup.find('span', class_='hp_address_subtitle'):
        hotel_address=hotel_soup.find('span', class_='hp_address_subtitle').text.strip('\t\r\n')
    else:
        hotel_address=None
    print(hotel_address)

    try:
        hotel_soup.find('span', class_='hp__hotel_ratings__stars').find('span', class_='invisible_spoken')
        hotel_star=hotel_soup.find('span', class_='hp__hotel_ratings__stars').find('span', class_='invisible_spoken').text
        hotel_star=int(re.sub('[^0-9,]', "", hotel_star))
    except:
        hotel_star=None
    #print(hotel_star)

    hotel_facilities=[]
    try:
        facilities_list=hotel_soup.find('div', class_='facilitiesChecklist')
        for el in facilities_list.find_all('li'):
            try:
                hotel_facilities.append(el['data-name-en'])
            except KeyError:
                continue
    except:
        facilities_list=None
    

    print("done in %0.3fs" % (time.time() - t0))
    hotel_table=hotel_table(hotel_id,hotel_type,hotel_name,hotel_address,dest_id,hotel_star,hotel_facilities)
    try:
        db_hotel_list_update(hotel_table,link)
        return 1
    except:
        return 0

######################################################################################################

def hotel_data_update (day_in,day_out,hotel_soup):
    t0 = time.time()   
    try:
        hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    except: 
        print('error: no hotel_id')
        text_file = open("Error_msg.txt", "a")
        text_file.write('\nNo id found for %s'%hotel_link)
        text_file.close()
        hotel_id=None

    room_list=[]
    if hotel_soup.find(id='room_availability_container'):
        room_availability_container= hotel_soup.find(id='room_availability_container')

        #loop on rooms to get each one characteristics
        for tr in room_availability_container.find_all('tr', class_=re.compile("^room_loop.*maintr $")):
            room_counter=int(re.sub('[^0-9]', "", str(tr['class'])))
            try: 
                room_size=hotel_soup.select_one('tr.room_loop_counter'+str(room_counter)+'.extendedRow').select_one('span.info.rooms-block__pills-container__pill').text
                room_size=int(re.sub('[^0-9]', "", room_size))
            except:
                try:
                    room_size=hotel_soup.select_one('tr.room_loop_counter'+str(room_counter)+'.extendedRow').select_one('div.info').select_one('strong').nextSibling
                    room_size=int(re.sub('[^0-9]', "", room_size))
                except:
                    room_size=None

            room_data=room_get_main_data(tr,room_size)

            #loop on room offers
            for tr1 in room_availability_container.find_all('tr',{'id':re.compile("^%s"%room_data.room_id)}):
                room_alldata=room_get_all_data(hotel_id,day_in,day_out,room_data,tr1)
                room_list.append(room_alldata)
                    
    else:
        print('no available rooms')
        room_list=[room_get_all_data(hotel_id,day_in,day_out,'no room','no av room')]
    
    print('single scraping done in %0.3fs' % (time.time() - t0))
    try:    
        db_hotel_data_update(room_list)
        print('single scraping and updating done in %0.3fs' % (time.time() - t0))
        return 1
    except:
        raise 
        return 0
    
##################################################################################################

def room_get_main_data (tr,room_size):
    
    class room_main_data(object):
        def __init__(self,room_id,room_type,room_size,room_facilities,room_inc1,room_inc0):
            self.room_id=room_id
            self.room_type=room_type
            self.room_size=room_size
            self.room_facilities=room_facilities
            self.room_inc1=room_inc1
            self.room_inc0=room_inc0
   
    room_id=tr.select_one('div.rt-room-info')['id']

    room_facilities=[]
    if tr.select_one('a.jqrt'):
        room_type=tr.select_one('a.jqrt')['data-room-name-en']
    else:
        room_type=None

    try:
        facility=tr.select_one('div.rt-all-facilities-hidden').select('span')
        for el in facility:
            try:
                room_facilities.append(el['data-name-en'])
            except KeyError:
                continue
    except:
        room_facilities=None
    
    

    room_inc1=None
    room_inc0=None
    if tr.select('div.incExcInPriceNew'):       
        for el in tr.select('div.incExcInPriceNew'):
            if el.select_one('span.incExcEmphasize').text in ['include','incluso','Include','Incluso']:
                room_inc1=el.text.split(':')[1].strip('.\t\r\n')
            elif el.select_one('span.incExcEmphasize').text in ['non include','Non include','non incluso','Non incluso']:
                room_inc0=el.text.split(':')[1].strip('.\t\r\n')
    
    if room_size is None:
        try:
            room_size=tr.select_one('i.bicon-roomsize').next_sibling
            room_size=int(re.sub('[^0-9]', "", room_size))
        except:
            print('no room size')
            room_size=None
   
    return room_main_data(room_id,room_type,room_size,room_facilities,room_inc1,room_inc0)

##############################################################################################

def room_get_all_data(hotel_id,day_in,day_out,room_data,tr1):
    d=room_data
    class room_all_data(object):
        def __init__(self,hotel_id,day_in,day_out,room_id,room_type,room_size,room_facilities,room_inc1,room_inc0,price,breakfast_opt,policy_opt,max_occ,room_left):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.search_date=(datetime.datetime.now().date())
            self.room_id=room_id
            self.room_type=room_type
            self.room_size=room_size
            self.room_facilities=room_facilities
            self.room_inc1=room_inc1
            self.room_inc0=room_inc0   
            self.price=price
            self.breakfast_opt=breakfast_opt
            self.policy_opt=policy_opt
            self.max_occ=max_occ
            self.room_left=room_left
    
    if tr1 == 'no av room' and room_data == 'no room' :
        return room_all_data(hotel_id,day_in,day_out,None,'no av room',None,None,None,None,None,None,None,None,None)

    try:
        price=tr1.select_one('strong.js-track-hp-rt-room-price').text.strip('\t\r\n\€xa')
        price=(float(re.sub('[^0-9,]', "", price).replace(",", ".")))
    except:
        try:
            price=tr1.find('span', class_='hprt-price-price-standard').text.strip('\t\r\n\€xa')
            price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
        except :
            price=None
       
    policy_opt=[]
    breakfast_opt=[]
    for el in tr1.select('li.hp-rt__policy__item'):
        if el.find('span', class_='bicon-coffee') in el:
            breakfast_opt.append(el.text.strip('\n'))
        else:
            policy_opt.append(el.text.strip('\n'))
       
    try :
        max_occ=int(tr1['data-occupancy'])
    except KeyError:
        max_occ=None

    try:
        room_left=tr1.select_one('span.only_x_left').text
        room_left=re.sub('[^0-9,]', "", room_left)
    except:
        room_left=None
    #print(hotel_id,day_in,day_out,d.room_id,d.room_type,d.room_size,d.room_facilities,d.room_inc1,d.room_inc0,price,breakfast_opt,policy_opt,max_occ,room_left)
    return room_all_data(hotel_id,day_in,day_out,d.room_id,d.room_type,d.room_size,d.room_facilities,d.room_inc1,d.room_inc0,price,breakfast_opt,policy_opt,max_occ,room_left)

#######################################################################################################

def hotel_ratings_update(day_in,day_out,hotel_soup):
    
    class hotel_ratings(object):
        def __init__(self,hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,brakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.search_date=(datetime.datetime.now().date())
            self.av_rating=av_rating
            self.n_ratings=n_ratings
            self.superb_score=superb_score
            self.good_score=good_score
            self.average_score=average_score
            self.poor_score=poor_score
            self.very_poor_score=very_poor_score
            self.breakfast_score=breakfast_score
            self.clean_score=clean_score
            self.comfort_score=comfort_score
            self.location_score=location_score
            self.services_score=services_score
            self.value_score=value_score
            self.staff_score=staff_score
            self.wifi_score=wifi_score
            

    t0 = time.time()

    try:
        hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    except: 
        text_file = open("Error_msg.txt", "a")
        text_file.write('\nNo id found for %s'%hotel_link)
        text_file.close()
        hotel_id=None

    if hotel_soup.find('span', class_='review-score-badge'):
        av_rating=hotel_soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
        av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
    else:
        av_rating=None

    try:
        score_dist=hotel_soup.select('span.review_list_score_breakdown_col')[0]
    except:
        score_dist=None

    try:
        superb_score=score_dist.find('li',{'data-question':'review_adj_superb'}).find('p', class_='review_score_value').text
        superb_score=int(superb_score)
    except:
        superb_score=None

    try:
        good_score=score_dist.find('li', {'data-question':'review_adj_good'}).find('p', class_='review_score_value').text
        good_score=int(good_score)
    except:
        good_score=None

    try:
        average_score=score_dist.find('li', {'data-question':'review_adj_average_okay'}).find('p', class_='review_score_value').text
        average_score=int(average_score)
    except:
        average_score=None

    try:
        poor_score=score_dist.find('li', {'data-question':'review_adj_poor'}).find('p', class_='review_score_value').text
        poor_score=int(poor_score)
    except:
        poor_score=None

    try:
        very_poor_score=score_dist.find('li', {'data-question':'review_adj_very_poor'}).find('p', class_='review_score_value').text
        very_poor_score=int(very_poor_score)
    except:
        very_poor_score=None
            
    #print(superb_score, good_score, average_score, poor_score, very_poor_score)

    try:
        score_spec=hotel_soup.select('span.review_list_score_breakdown_col')[1]
    except:
        score_spec=None

    try:
        breakfast_score=score_spec.find('li',{'data-question':'breakfast'}).find('p', class_='review_score_value').text
        breakfast_score=float(re.sub('[^0-9,]', "", breakfast_score).replace(",", "."))
    except:
        breakfast_score=None

    try:
        clean_score=score_spec.find('li',{'data-question':'hotel_clean'}).find('p', class_='review_score_value').text
        clean_score=float(re.sub('[^0-9,]', "", clean_score).replace(",", "."))
    except:
        clean_score=None

    try:
        comfort_score=score_spec.find('li',{'data-question':'hotel_comfort'}).find('p', class_='review_score_value').text
        comfort_score=float(re.sub('[^0-9,]', "", comfort_score).replace(",", "."))
    except:
        comfort_score=None

    try:
        location_score=score_spec.find('li',{'data-question':'hotel_location'}).find('p', class_='review_score_value').text
        location_score=float(re.sub('[^0-9,]', "", location_score).replace(",", "."))
    except:
        location_score=None

    try:
        services_score=score_spec.find('li',{'data-question':'hotel_services'}).find('p', class_='review_score_value').text
        services_score=float(re.sub('[^0-9,]', "", services_score).replace(",", "."))
    except:
        services_score=None

    try:
        staff_score=score_spec.find('li',{'data-question':'hotel_staff'}).find('p', class_='review_score_value').text
        staff_score=float(re.sub('[^0-9,]', "", staff_score).replace(",", "."))
    except:
        staff_score=None

    try:
        value_score=score_spec.find('li',{'data-question':'hotel_value'}).find('p', class_='review_score_value').text
        value_score=float(re.sub('[^0-9,]', "", value_score).replace(",", "."))
    except:
        value_score=None

    try:
        wifi_score=score_spec.find('li',{'data-question':'hotel_wifi'}).find('p', class_='review_score_value').text
        wifi_score=float(re.sub('[^0-9,]', "", wifi_score).replace(",", "."))
    except:
        wifi_score=None

    try:
        n_ratings=hotel_soup.select_one('span.review-score-widget__subtext').text
        n_ratings=int(re.sub('[^0-9,]', "", n_ratings))
    except:
        n_ratings=None
    #print(wifi_score,staff_score,services_score,n_ratings)

    hotel_ratings=hotel_ratings(hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,breakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score)

    print('single scraping done in %0.3fs' % (time.time() - t0))

    try:
        db_hotel_ratings_update(hotel_ratings)
        print('single scraping and updating done in %0.3fs' % (time.time() - t0))
        return 1
    except:
        return 0

#########################################################################################################

def hotel_reviews_update (headers, hotel_soup) :
    
    class hotel_review(object):
        def __init__(self,hotel_id,score,post_title,pos_comment,neg_comment,post_date,author_name,author_nat,author_group):
            self.hotel_id=hotel_id
            self.score=score
            self.post_title=post_title
            self.pos_comment=pos_comment
            self.neg_comment=neg_comment
            self.post_date=post_date
            self.author_name=author_name
            self.author_nat=author_nat
            self.author_group=author_group

    t0 = time.time()

    try:
        hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    except: 
        text_file = open("Error_msg.txt", "a")
        text_file.write('\nNo id found for %s'%hotel_link)
        text_file.close()
        hotel_id=None
    print(hotel_id)

    try:
        link_to_rev=hotel_soup.find('a', class_='show_all_reviews_btn')['href']
        link_to_rev=base_url+link_to_rev
        print(link_to_rev)

        for i in range (1,2): #set number of review visited pages

            link_to_rev=link_to_rev+';page='+str(i)

            try:
                response = requests.get(link_to_rev,headers=headers).text
            except:
                return 0

            review_soup=bs(response,'lxml')
            for element in review_soup.select('li.review_item.clearfix'):
                try:
                    post_date=element.select_one('p.review_item_date').text.strip('\t\r\n\€xa')
                    post_date=datetime.datetime.strptime(post_date,'%d %B %Y')
                except:
                    post_date=None

                for el in element.find_all('div', class_='review_item_review_container'):
                    try:
                        score=el.find('span', class_="review-score-badge").text.strip('\t\r\n\€xa')
                        score=float(re.sub('[^0-9,]', "",score).replace(",", "."))
                    except:
                        score=None
                    try:
                        post_title=el.find('span',itemprop='name').text.strip('\t\r\n')
                    except:
                        post_title='na'
                    try:
                        neg_comment=el.find('p', class_='review_neg').text.strip('\t\r\n\눇') #to do: remove 눇
                    except:
                        neg_comment='empty'
                    try:
                        pos_comment=element.find('p', class_='review_pos').text.strip('\t\r\n\눇')
                    except:
                        pos_comment='empty'
        
                for el in element.find_all('div', class_="review_item_reviewer"):
                    try:
                        author_name=el.h4.text.strip('\t\r\n')
                    except:
                        author_name='unknown'
                    try:
                        author_nat=el.find('span', itemprop="nationality").text.strip('\t\r\n')
                    except:
                        author_nat='unknown'
                    try:
                        author_group=el.find('div',class_='user_age_group').text.strip('\t\r\n')
                    except:
                        author_group='unknown'
                
            hotel_reviews=hotel_review(hotel_id,score,post_title,pos_comment,neg_comment,post_date,author_name,author_nat,author_group)
            
            try:
                db_hotel_reviews_update(hotel_reviews)
                print('single scraping and updating done in %0.3fs' % (time.time() - t0))
                return 1
            except:
                return 0
    except:
        hotel_review=hotel_review(hotel_id,None,'na','na','na',None,None,None,None)
        try:
            db_hotel_reviews_update(hotel_review)
            print('single scraping and updating done in %0.3fs' % (time.time() - t0))
            return 1
        except:
            return 0
    print('single scraping done in %0.3fs' % (time.time() - t0))

  
            
#################################################################################################

def db_hotel_list_update(hotel_table,link) :
#connect to database "webcrawling", insert new row data in hotel_list table  if hotel_id is not present
    
    conn=psycopg2.connect('dbname='+db_name+' user='+user_name)

    cur=conn.cursor()
    
    i=hotel_table
     
        
    SQL = '''BEGIN;
                 UPDATE hotel_list SET hotel_name=(%s),location=(%s),hotel_address=(%s),hotel_id=(%s),hotel_type=(%s),hotel_star=(%s),hotel_facilities=(%s) WHERE hotel_link=(%s) ;
                 INSERT INTO hotel_list (hotel_name,location,hotel_address,hotel_id,hotel_type,hotel_star,hotel_facilities)
	         SELECT %s,%s,%s,%s,%s,%s,%s
	         WHERE NOT EXISTS (SELECT hotel_link  FROM hotel_list WHERE hotel_link=%s
	         );
                 COMMIT;'''

    data = (i.hotel_name,i.dest_id,i.hotel_address,i.hotel_id,i.hotel_type,i.hotel_star,i.hotel_facilities,link,i.hotel_name,i.dest_id,i.hotel_address,i.hotel_id,i.hotel_type,i.hotel_star,i.hotel_facilities,link)

    cur.execute(SQL, data)

    cur.close()
    conn.close()

###############################################################################

def db_hotel_data_update(hotel_data_list) :
#connect to database "webcrawling", insert new row data in hotel_data table
   
    conn=psycopg2.connect('dbname='+db_name+' user='+user_name)

    cur=conn.cursor()

    for i in hotel_data_list:

        SQL = '''BEGIN;
                 UPDATE hotel_data SET hotel_id=(%s),day_in=(%s),day_out=(%s),room_id=(%s),room_type=(%s),room_size=(%s),room_facilities=(%s),inclusive=(%s),non_inclusive=(%s),price=(%s),breakfast_opt=(%s),policy_opt=(%s),max_occ=(%s),search_date=(%s) WHERE price=(%s) AND search_date=(%s);
                 INSERT INTO hotel_data (hotel_id,day_in,day_out,room_id,room_type,room_size,room_facilities,inclusive,non_inclusive,price,breakfast_opt,policy_opt,max_occ,search_date)
	         SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT * FROM hotel_data WHERE price=(%s) AND search_date=(%s))
                 ;
                 COMMIT;'''

        data = (i.hotel_id,i.day_in,i.day_out,i.room_id,i.room_type,i.room_size,i.room_facilities,i.room_inc1,i.room_inc0,i.price,i.breakfast_opt,i.policy_opt,i.max_occ,i.search_date,i.price,i.search_date,i.hotel_id,i.day_in,i.day_out,i.room_id,i.room_type,i.room_size,i.room_facilities,i.room_inc1,i.room_inc0,i.price,i.breakfast_opt,i.policy_opt,i.max_occ,i.search_date,i.price,i.search_date)

        cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################

def  db_hotel_reviews_update(hotel_reviews):
#connect to database "webcrawling", insert new data in hotel_reviews table
    
    i=hotel_reviews

    conn=psycopg2.connect('dbname='+db_name+' user='+user_name)

    cur=conn.cursor()

 
    SQL = '''BEGIN;
                 INSERT INTO hotel_reviews (hotel_id,score,post_title,positive_comment,negative_comment,post_date,author_name,author_nat,author_group) 
	         SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT * FROM hotel_reviews WHERE post_date=(%s) AND author_name=(%s))
                 ;
                 COMMIT;'''

    data = (i.hotel_id,i.score,i.post_title,i.pos_comment,i.neg_comment,i.post_date,i.author_name,i.author_nat,i.author_group,i.post_date,i.author_name)

    cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################

def db_hotel_ratings_update(hotel_ratings):
#connect to database "webcrawling", insert new row data in hotel_ratings table
   
    conn=psycopg2.connect('dbname='+db_name+' user='+user_name)

    cur=conn.cursor()

    i=hotel_ratings

    SQL = '''BEGIN;
                 INSERT INTO hotel_ratings (hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,breakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score,search_date)
	         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

    data = (i.hotel_id,i.day_in,i.day_out,i.av_rating,i.n_ratings,i.superb_score,i.good_score,i.average_score,i.poor_score,i.very_poor_score,i.breakfast_score,i.clean_score,i.comfort_score,i.location_score,i.services_score,i.staff_score,i.value_score,i.wifi_score,i.search_date)

    cur.execute(SQL, data)

    cur.close()
    conn.close()

#######################################################################################
def db_hotel_link():
     
    conn=psycopg2.connect('dbname='+db_name+' user='+user_name)

    cur=conn.cursor()

    SQL = 'SELECT hotel_link FROM hotel_list'

    cur.execute(SQL)
  
    if cur.rowcount==0:
        print('Please, verify hotel_link list ')
        link_list=None

    else:
        link_list=cur.fetchall()

    cur.close()
    conn.close()

    return link_list
###################################################################################

def readingdbkey():
    with open('dbkey.txt', 'r') as f:
        s = f.read()
        key=eval(s)
        return key
   
####################################################################################

def resume(opt,var=''):
    if opt == 'w':
        text_file = open("backup.txt", "w")
        text_file.write(str(var))
        text_file.close()
    elif opt =='r':
        with open('backup.txt', 'r') as f:
            s = f.read()
            var=eval(s)
            return var
    
####################################################################################
start=time.time()

#research_options
day_in_str='2017-12-20'

delta_day_out=7
delta_days=7
date_iter=4

print(__doc__)

global n_link
n_link=0

import conn_db
try:   
    key=readingdbkey()
    db_name=key['db_name']
    user_name=key['user_name']
except:
    key=db_key_mod()

try:
    n_link=resume('r')
    print('\nLast section was stopped at link ', n_link)
    import sys, select
    print('\nPress "ent" to restart from there, or a default start will be done\n') #add wait time -->deafult N
    i, o, e = select.select( [sys.stdin], [], [], 10 )
    if (i):
        print ('Restating from', n_link)
    else:
        print ('Default start')
        os.remove('backup.txt')
        n_link=0
except:
    pass

# loop on date search
day_in = datetime.datetime.strptime(day_in_str,'%Y-%m-%d')
day_out = datetime.datetime.strptime(day_in_str, '%Y-%m-%d')+datetime.timedelta(days=delta_day_out)
day_in=day_in.date()
day_out=day_out.date()


link_list=db_hotel_link()

# loop on date search
for i in range (0,date_iter):
    n_iter=0 
    max_it=10 #fix the max number of request attempt
    if  args.scraping_type == 1 or args.scraping_type == 2 or args.scraping_type == 3 or args.scraping_type == 4:
        print('type of scraping: ',args.scraping_type)
        result=crawler(link_list,day_in,day_out)
        if result == 'connection error':
            print('connection error at ',n_link)
        else:
            while result == 0 and n_iter<max_it: 
                resutl=crawler(link_list,day_in,day_out)
                n_iter+=1
                print('Error during scraping: look at "Error_msg.txt"')
            os.remove('backup.txt')
            print("global scraping and udating done in %0.3fs" % (time.time() - start))

    else:
        print("chose type of scraping, -h for help")

    day_in=day_in+datetime.timedelta(days=delta_days)
    day_out=day_out+datetime.timedelta(days=delta_days)
       



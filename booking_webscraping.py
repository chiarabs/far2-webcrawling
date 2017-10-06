#!/usr/bin/python

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError
import requests
import re
import psycopg2
import argparse
import datetime
from time import time
import locale
locale.setlocale(locale.LC_ALL, 'it_IT.utf8')

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--scraping_type", action="count", default=0,help='select the type of scraping: -t for hotel_list update (get hotel names), -tt for hotel_data update (get hotel_id, price, average rating), -ttt for hotel_ratings update, -tttt fot hotel_reviews update (get users comments) ')
args = parser.parse_args()
    


base_url='http://www.booking.com'

def crawler(type_of_location,location,day_in,day_out):
    ti=time()

    #look for id_location in db_locations: if it is present get id_loc else ask to search id_loc to insert in db_locations
    dest_id=id_db_locations(location,type_of_location)
    
    print(dest_id)
    #for the specific location,day_in(yyy-mm-dd),day_out(yyyy-mm-dd) return a list of class object
    
    #dest_id=-110502
  
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
        return 'connection error'

    headers={'User-Agent': ua.random}
    print (headers)
    
    hotel_list=[]
  
    #loop  over all the pages containing research results iterating on the URL offset parameter
    #loop  stops when it finds error message "take control of your search"
    offset_range=15 
    for i in range(0,1): #set the number of visited pages 
        print(i)
        offset=i*offset_range
        print(offset_range)

        if type_of_location=='city':
            url='https://www.booking.com/searchresults.it.html?;&checkin_monthday='+str(monthday_in)+'&checkin_month='+str(month_in)+';checkin_year='+str(year_in)+';checkout_monthday='+str(monthday_out)+';checkout_month='+str(month_out)+';checkout_year='+str(year_out)+';dest_id='+str(dest_id)+';dest_type=city;sb_travel_purpose=leisure;no_rooms=1;group_adults=2;group_children=0;rows=15;offset='+str(offset)
        elif type_of_location=='region':
            url='https://www.booking.com/searchresults.it.html?;&checkin_monthday='+str(monthday_in)+'&checkin_month='+str(month_in)+';checkin_year='+str(year_in)+';checkout_monthday='+str(monthday_out)+';checkout_month='+str(month_out)+';checkout_year='+str(year_out)+';region='+str(dest_id)+';sb_travel_purpose=leisure;no_rooms=1;group_adults=2;group_children=0;rows=15;offset='+str(offset)
        else:
            print('verify type_of_location: city or region admitted')
            return
        print (url)

        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:  
            print (e)
            return
        
        response = requests.get(url,headers=headers).text
        soup = bs(response,'lxml')

        if soup.get('class') == 'Take Control of Your Search':
            print('scraping end in %0.3fs' % (time() - ti))
            break
        if soup.get('class') == 'trclassack_broaden_search':
            print('scraping end in done in %0.3fs' % (time() - ti))
            break

        try:
            last_show=int(soup.find('span', class_='sr_showed_amount_last').text.strip('\t\r\n'))
            first_show=(soup.find('span' ,class_='sr_showed_amount').text)
            first_show=[int(s) for s in first_show.split() if s.isdigit()][0]
            diff_pg=last_show-(first_show-1)
            if diff_pg<0:
                print('offset out of range:serch end')
                break
        except:
            offset_range=15
        
        
        #loop over hotel  links to get each hotel_soup
        for link in soup.find_all('a',class_='hotel_name_link url'):
            hotel_link=link['href']
            hotel_link=base_url+hotel_link
            print(hotel_link)

            if  link.find('span',class_='sr-hotel__type'):
                hotel_type=link.find('span', class_='sr-hotel__type').text
            else:
                hotel_type=None

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

    

            if args.scraping_type < 2:
                hotel_table=hotel_table_update(hotel_link,hotel_type,soup)
                hotel_list.append(hotel_table)
            
 
            elif args.scraping_type == 2:   
                hotel_data=hotel_data_update(day_in,day_out,hotel_link,soup)
                if hotel_data is not None:
                    hotel_list=hotel_list+hotel_data
                else:
                    continue
            elif args.scraping_type == 3:   
                hotel_ratings=hotel_ratings_update(day_in,day_out,soup)
                hotel_list.append(hotel_ratings)

            elif args.scraping_type == 4:
                hotel_reviews=hotel_reviews_update(headers,soup)
                hotel_list.append(hotel_reviews)

    print("scraping done in %0.3fs" % (time() - ti))
    return hotel_list

##################################################################################################

def hotel_table_update(hotel_link,hotel_type,hotel_soup) :
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
           
    t0 = time()
    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    print(hotel_id)
    print(hotel_type)           
    dest_id=int(hotel_link[hotel_link.find('dest_id=')+len('dest_id='):hotel_link.rfind(';srfid')])

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

    if hotel_soup.find('span', class_='hp__hotel_ratings__stars').find('span', class_='invisible_spoken'):
        hotel_star=hotel_soup.find('span', class_='hp__hotel_ratings__stars').find('span', class_='invisible_spoken').text
        hotel_star=int(re.sub('[^0-9,]', "", hotel_star))
    else:
        hotel_star=None
    print(hotel_star)

    hotel_facilities=[]
    facilities_list=hotel_soup.find('div', class_='facilitiesChecklist')
    for el in facilities_list.find_all('li'):
        try:
            hotel_facilities.append(el['data-name-en'])
        except KeyError:
            continue
    

    print("done in %0.3fs" % (time() - t0))
    return hotel_table(hotel_id,hotel_type,hotel_name,hotel_address,dest_id,hotel_star,hotel_facilities)

     

##################################################################################################

def hotel_data_update (day_in,day_out,hotel_link,hotel_soup):

    t0 = time()

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    
    room_list=[]
    if hotel_soup.find(id='room_availability_container'):
        room_availability_container= hotel_soup.find(id='room_availability_container')
       
        text_file = open("Output_per_room_size.html", "w")
        text_file.write(str(hotel_soup))
        text_file.close()

        for tr in room_availability_container.find_all('tr', class_=re.compile("^room_loop.*maintr $")):
        #loop on room to get each one characteristics
            room_data=room_get_main_data(tr)
            for tr1 in room_availability_container.find_all('tr',{'id':re.compile("^%s"%room_data.room_id)}):
            #loop on room offers
                room_alldata=room_get_all_data(hotel_id,day_in,day_out,room_data,tr1)
                #print(room_alldata.room_id,room_alldata.room_type,room_alldata.room_facilities,room_alldata.room_inc1,room_alldata.room_inc0,room_alldata.breakfast_opt,room_alldata.policy_opt,room_alldata.max_occ,room_alldata.room_left,room_alldata.price)
                room_list.append(room_alldata)
            
        return room_list
    else:
        print('no available rooms')
        room_alldata=room_get_all_data(hotel_id,day_in,day_out,'no room','no av room')
        return [room_alldata]
    
##################################################################################################

def room_get_main_data (tr):
    
    class room_main_data(object):
        def __init__(self,room_id,room_type,room_size,room_facilities,room_inc1,room_inc0):
            self.room_id=room_id
            self.room_type=room_type
            self.room_size=room_size
            self.room_facilities=room_facilities
            self.room_inc1=room_inc1
            self.room_inc0=room_inc0
   
    room_id=tr.select_one('div.rt-room-info')['id']

    if tr.select_one('a.jqrt'):
        room_type=tr.select_one('a.jqrt')['data-room-name-en']
    else:
        room_type=None

    facility=tr.select_one('div.rt-all-facilities-hidden')
    room_facilities=[]
    for el in facility.select('span'):
        try:
            room_facilities.append(el['data-name-en'])
        except KeyError:
            continue
    if tr.select('div.incExcInPriceNew'):       
        for el in tr.select('div.incExcInPriceNew'):
            if 'Include' in el.select_one('span'):
                room_inc1=el.text.split(':')[1]
                if 'Non include' in el.select_one('span'):
                    room_inc0=el.text.split(':')[1]
                else:
                    room_inc0=None
            elif 'Non include' in el.select_one('span'):
                 room_inc1=None
                 room_inc0=el.text.split(':')[1]
            else:
                room_inc1=None
                room_inc0=None
    else:
        room_inc1=None
        room_inc0=None
    print(room_inc1,room_inc0)

    if tr.select_one('span.info'):
        room_size=tr.select_one('span.info').text
        room_size=float(re.sub('[^0-9,]', "", room_size))
    elif tr.select_one('div.info'):
        room_size=tr.select_one('div.info').text
        room_size=float(re.sub('[^0-9,]', "", room_size))
    else:
        room_size=None
    print(room_size)
    return room_main_data(room_id,room_type,room_size,room_facilities,room_inc1,room_inc0)

##############################################################################################

def room_get_all_data(hotel_id,day_in,day_out,room_data,tr1):
    d=room_data
    class room_all_data(object):
        def __init__(self,hotel_id,day_in,day_out,room_id,room_type,room_size,room_facilities,room_inc1,room_inc0,price,breakfast_opt,policy_opt,max_occ,room_left):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.search_date=(datetime.datetime.now())
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

    if tr1.select_one('strong.js-track-hp-rt-room-price'):
        price=tr1.select_one('strong.js-track-hp-rt-room-price').text.strip('\t\r\n\€xa')
        price=(float(re.sub('[^0-9,]', "", price).replace(",", ".")))
    elif tr1.find('span', class_='hprt-price-price-standard'):
        price=tr1.find('span', class_='hprt-price-price-standard').text.strip('\t\r\n\€xa')
        price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
    else :
        text_file = open("soup_error_price.txt", "w")
        text_file.write(str(hotel_soup))
        text_file.close()
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
            
    return room_all_data(hotel_id,day_in,day_out,d.room_id,d.room_type,d.room_size,d.room_facilities,d.room_inc1,d.room_inc0,price,breakfast_opt,policy_opt,max_occ,room_left)

#######################################################################################################

def hotel_ratings_update(day_in,day_out,hotel_soup):
    
    class hotel_ratings(object):
        def __init__(self,hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,brakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.search_date=(datetime.datetime.now())
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
            self.wifi_score=wifi_score
            

    t0 = time()

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])

    if hotel_soup.find('span', class_='review-score-badge'):
        av_rating=hotel_soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
        av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
    else:
        av_rating=None
    #print(av_rating)

    try:
        score_dist=hotel_soup.select('span.review_list_score_breakdown_col')[0]
    except:
        score_dist=None

    try:
        superb_score=score_dist.find('li',{'data-question':'review_adj_superb'}).find('p', class_='review_score_value').text
    except:
        superb_score=None

    try:
        good_score=score_dist.find('li', {'data-question':'review_adj_good'}).find('p', class_='review_score_value').text
    except:
        good_score=None

    try:
        average_score=score_dist.find('li', {'data-question':'review_adj_average_okay'}).find('p', class_='review_score_value').text
    except:
        average_score=None

    try:
        poor_score=score_dist.find('li', {'data-question':'review_adj_poor'}).find('p', class_='review_score_value').text
    except:
        poor_score=None

    try:
        very_poor_score=score_dist.find('li', {'data-question':'review_adj_very_poor'}).find('p', class_='review_score_value').text
    except:
        very_poor_score=None
            
    print(superb_score, good_score, average_score, poor_score, very_poor_score)

    try:
        score_spec=hotel_soup.select('span.review_list_score_breakdown_col')[1]
    except:
        score_spec=None

    try:
        breakfast_score=score_spec.find('li',{'data-question':'breakfast'}).find('p', class_='review_score_value').text
    except:
        breakfast_score=None

    try:
        clean_score=score_spec.find('li',{'data-question':'hotel_clean'}).find('p', class_='review_score_value').text
    except:
        clean_score=None

    try:
        comfort_score=score_spec.find('li',{'data-question':'hotel_comfort'}).find('p', class_='review_score_value').text
    except:
        comfort_score=None

    try:
        location_score=score_spec.find('li',{'data-question':'hotel_location'}).find('p', class_='review_score_value').text
    except:
        location_score=None

    try:
        services_score=score_spec.find('li',{'data-question':'hotel_services'}).find('p', class_='review_score_value').text
    except:
        services_score=None

    try:
        staff_score=score_spec.find('li',{'data-question':'hotel_staff'}).find('p', class_='review_score_value').text
    except:
        staff_score=None

    try:
        value_score=score_spec.find('li',{'data-question':'hotel_value'}).find('p', class_='review_score_value').text
    except:
        value_score=None

    try:
        wifi_score=score_spec.find('li',{'data-question':'hotel_wifi'}).find('p', class_='review_score_value').text
    except:
        wifi_score=None

    try:
        n_ratings=hotel_soup.select_one('span.review-score-widget__subtext').text
        n_ratings=int(re.sub('[^0-9,]', "", n_ratings))
    except:
        n_ratings=None
    print(n_ratings)
    return hotel_ratings(hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,breakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score)

#########################################################################################################

def hotel_reviews_update (headers, hotel_soup) :
    
    class hotel_review(object):
        def __init__(self,hotel_id,score,pos_comment,neg_comment,post_date,author_name,author_nat,author_group):
            self.hotel_id=hotel_id
            self.score=score
            self.pos_comment=pos_comment
            self.neg_comment=neg_comment
            self.post_date=post_date
            self.author_name=author_name
            self.author_nat=author_nat
            self.author_group=author_group

    t0 = time()
            
    score=[]
    pos_comment=[]
    neg_comment=[]
    post_date=[]
    author_name=[]
    author_nat=[]
    author_group=[]

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])
    print(hotel_id)

    if hotel_soup.find('a', class_='show_all_reviews_btn'):
        link_to_rev=hotel_soup.find('a', class_='show_all_reviews_btn')['href']
     
        link_to_rev=base_url+link_to_rev
        print(link_to_rev)

        for i in range (1,2): #set number of reviews visited pages

            link_to_rev=link_to_rev+';page='+str(i)

            response = requests.get(link_to_rev,headers=headers).text
            review_soup=bs(response,'lxml')
            for element in review_soup.find_all('div', class_='review_item_review_container'):
                    single_score=element.find('span', class_="review-score-badge").text.strip('\t\r\n\€xa')
                    score.append(float(re.sub('[^0-9,]', "",single_score).replace(",", ".")))
                    if element.find('p', class_='review_neg'):
                        neg_comment.append(element.find('p', class_='review_neg').text) #to do: remove 눇
                    else:
                        neg_comment.append('empty')
                    if element.find('p', class_='review_pos'):
                        pos_comment.append(element.find('p', class_='review_pos').text)
                    else:
                        pos_comment.append('empty')
        
            for element in review_soup.find_all('p', class_='review_item_date'):
                post_date.append( datetime.datetime.strptime(element.text.strip('\t\r\n\€xa'),'%d %B %Y'))
        
            for element in review_soup.find_all('div', class_="review_item_reviewer"):
                author_name.append(element.h4.text.strip('\t\r\n'))
                if element.find('span', itemprop="nationality"):
                    author_nat.append(element.find('span', itemprop="nationality").text.strip('\t\r\n'))
                else:
                    author_nat.append('unknown')
                if element.find('div',class_='user_age_group'):
                    author_group.append(element.find('div',class_='user_age_group').text.strip('\t\r\n'))
                else:
                    author_group.append('unknown')
                
        hotel_review=hotel_review(hotel_id,score,pos_comment,neg_comment,post_date,author_name,author_nat,author_group)
    else:
        hotel_review=hotel_review(hotel_id,'empty','empty','empty','empty','empty','empty','empty')
    print("done in %0.3fs" % (time() - t0))
    return hotel_review
            
##################################################################################################
                        
def db_hotel_list_update(hotel_table_list) :
#connect to database "webcrawling", insert new row data in hotel_list table  if hotel_id is not present
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()
    
    for i in hotel_table_list:
        hotel_id=i.hotel_id
        hotel_name=i.hotel_name
        hotel_address=i.hotel_address
        location=i.dest_id
        hotel_type=i.hotel_type
        hotel_star=i.hotel_star
        hotel_facilities=i.hotel_facilities
    

        SQL = '''BEGIN;
                 INSERT INTO hotel_list (hotel_name,location,hotel_address,hotel_id,hotel_type,hotel_star,hotel_facilities)
	         SELECT %s,%s,%s,%s,%s,%s,%s
	         WHERE NOT EXISTS (SELECT hotel_id  FROM hotel_list WHERE hotel_id=%s
	         );
                 COMMIT;'''

        data = (hotel_name,location,hotel_address,hotel_id,hotel_type,hotel_star,hotel_facilities,hotel_id)

        cur.execute(SQL, data)

    cur.close()
    conn.close()

###########################################################################################

def db_hotel_data_update(hotel_data_list) :
#connect to database "webcrawling", insert new row data in hotel_data table
   
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    for i in hotel_data_list:

        SQL = '''BEGIN;
                 INSERT INTO hotel_data (hotel_id,day_in,day_out,room_id,room_type,room_size,room_facilities,inclusive,non_inclusive,price,breakfast_opt,policy_opt,max_occ,search_date)
	         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

        data = (i.hotel_id,i.day_in,i.day_out,i.room_id,i.room_type,i.room_size,i.room_facilities,i.room_inc1,i.room_inc0,i.price,i.breakfast_opt,i.policy_opt,i.max_occ,i.search_date)

        cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################

def  db_hotel_reviews_update(hotel_reviews_list):
#connect to database "webcrawling", insert new row data in hotel_data table
    
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    for i in hotel_reviews_list:
        for j in range(0,len(i.pos_comment)):
            hotel_id=i.hotel_id
            score=i.score[j]
            pos_comment=i.pos_comment[j]
            neg_comment=i.neg_comment[j]
            post_date=i.post_date[j]
            author_name=i.author_name[j]
            author_nat=i.author_nat[j]
            author_group=i.author_group[j]
    
            SQL = '''BEGIN;
                 INSERT INTO hotel_reviews (hotel_id,score,positive_comment,negative_comment,post_date,author_name,author_nat,author_group) 
	         VALUES (%s,%s,%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

            data = (hotel_id,score,pos_comment,neg_comment,post_date,author_name,author_nat,author_group)

            cur.execute(SQL, data)

    cur.close()
    conn.close()

####################################################################################

def db_hotel_ratings_update(hotel_ratings_list):
#connect to database "webcrawling", insert new row data in hotel_ratings table
   
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    for i in hotel_ratings_list:

        SQL = '''BEGIN;
                 INSERT INTO hotel_data (hotel_id,day_in,day_out,av_rating,n_ratings,superb_score,good_score,average_score,poor_score,very_poor_score,breakfast_score,clean_score,comfort_score,location_score,services_score,staff_score,value_score,wifi_score)
	         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

        data = (i.hotel_id,i.day_in,i.day_out,i.av_rating,i.n_ratings,i.superb_score,i.good_score,i.average_score,i.poor_score,i.very_poor_score,i.breakfast_score,i.clean_score,i.comfort_score,i.location_score,i.services_score,i.staff_score,i.value_score,i.wifi_score)

        cur.execute(SQL, data)

    cur.close()
    conn.close()

#######################################################################################
def id_db_locations(loc_name,loc_type) :
     
    conn=psycopg2.connect('dbname=webcrawling user=chiara')

    cur=conn.cursor()

    SQL = 'SELECT id_loc FROM locations WHERE loc_name=%s;'
    data= (loc_name,)

    cur.execute(SQL,data)
    #print(cur.rowcount)
    if cur.rowcount==0:
        new_id=input('Please, verify location spelling or insert dest_id or region_id: ')
        SQL = '''BEGIN;
             INSERT INTO locations (id_loc,loc_name,loc_type)
	     SELECT %s,%s,%s
	     WHERE NOT EXISTS (
	     SELECT * FROM locations WHERE id_loc=%s OR loc_name=%s
	     );
             COMMIT;'''

        data = (new_id,loc_name,loc_type,new_id,loc_name)

        cur.execute(SQL, data)
        return new_id
    else:
        return (cur.fetchone())[0]

    cur.close()
    conn.close()
    
 
####################################################################################
start=time()
#research_options
day_in_str='2017-12-27'
day_out_str='2017-12-28'

type_of_location='city'     #set = 'city' or 'region'
location='Aosta'

day_in = datetime.datetime.strptime(day_in_str,'%Y-%m-%d')
day_out= datetime.datetime.strptime(day_out_str, '%Y-%m-%d')

if args.scraping_type == 1:
    print("type of scraping: hotel_table_update")
    hotel_table_list=crawler(type_of_location,location,day_in,day_out)
    if hotel_table_list != 'connection error':
        while hotel_table_list is None:
            hotel_table_list=crawler(type_of_location,location,day_in,day_out)
            time.sleep(30)
        #db updating hotel_list in webcraling postgress db
        db_hotel_list_update(hotel_table_list)

elif args.scraping_type == 2:   
    print("type of scraping: hotel_data_update ")
    hotel_data_list=crawler(type_of_location,location,day_in,day_out)
    if hotel_data_list != 'connection error':
        while hotel_data_list is None:
            hotel_data_list=crawler(type_of_location,location,day_in,day_out)
            time.sleep(30)
        #db updating hotel_data in webcrawling postgress db
        db_hotel_data_update(hotel_data_list)

elif args.scraping_type == 3:   
    print("type of scraping: hotel_ratings_update ")
    hotel_ratings_list=crawler(type_of_location,location,day_in,day_out)
    if hotel_ratings_list != 'connection error':
        while hotel_ratings_list is None:
            hotel_ratings_list=crawler(type_of_location,location,day_in,day_out)
            time.sleep(30)
        #db updating hotel_ratings in webcrawling postgress db
        db_hotel_ratings_update(hotel_ratings_list)

elif args.scraping_type == 4:   
    print("type of scraping: hotel_reviews_update ")
    hotel_reviews_list=crawler(type_of_location,location,day_in,day_out)
    if hotel_reviews_list != 'connection error':
        while hotel_reviews_list is None:
            hotel_reviews_list=crawler(type_of_location,location,day_in,day_out)
            time.sleep(30)
        #db updating hotel_reviews in webcrawling postgress db
        db_hotel_reviews_update(hotel_reviews_list)

else:
    print("chose type of scraping, -h for help")

print("scraping and db updating done in %0.3fs" % (time() - start))

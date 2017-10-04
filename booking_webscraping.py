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
parser.add_argument("-t", "--scraping_type", action="count", default=0,help='select the type of scraping: -t for hotel_list update (get hotel names), -tt for hotel_data update (get hotel_id, price, average rating), -ttt for hotel_reviews update (get users comments) ')
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
        return

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
                    hotel_list.append(hotel_data)
                else:
                    continue
            
            elif args.scraping_type == 3:
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

    class hotel_data(object):
        def __init__(self,hotel_id,day_in,day_out,price,av_rating,room_type,room_size,breakfast_opt,policy_opt,room_left,room_facilities):
            self.hotel_id=hotel_id
            self.day_in=day_in
            self.day_out=day_out
            self.price=price
            self.av_rating=av_rating
            self.search_date=(datetime.datetime.now())
            self.room_type=room_type
            self.room_size=room_size
            self.breakfast_opt=breakfast_opt
            self.policy_opt=policy_opt
            self.room_left=room_left
            self.room_facilities=room_facilities
          
            
    t0 = time()

    hotel_id=int(hotel_soup.find("input", {"name":"hotel_id"})['value'])

    if hotel_soup.find('span', class_='review-score-badge'):
        av_rating=hotel_soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
        av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
    else:
        av_rating=None
    #print(av_rating)
    
    price=[]
    room_type=[]
    room_size=[]
    room_left=[]
    room_facilities=[]
    room_facilities_=[]
    breakfast_opt=[]
    policy_opt=[]
    policy_opt_=[]
    max_occ=[]
    if hotel_soup.find(id='room_availability_container'):
        room_availability_container= hotel_soup.find(id='room_availability_container')

        for tr in room_availability_container.find_all('tr', class_=re.compile("^room_loop.*maintr $")):
        #loop on room to get for each one: room_type, room_size,list of breakfast_options, list of policy_options, max_occ(occupancy),list of facilities
            room_id=tr.select_one('div.rt-room-info')['id']

            if tr.select_one('a.jqrt'):
                room_type.append(tr.select_one('a.jqrt')['data-room-name-en'])
            else:
                room_type.append(None)

            facility=tr.select_one('div.rt-all-facilities-hidden')
            for el in facility.select('span'):
                try:
                    room_facilities_.append(el['data-name-en'])
                except KeyError:
                    continue
            room_facilities.append(str(room_facilities_))

            text_file = open("soup_tr.html", "w")
            text_file.write(str(tr))
            text_file.close()

            tr1=room_availability_container.find('tr',{'id':re.compile("^%s"%room_id)})
            if tr1.select_one('strong.js-track-hp-rt-room-price'):
                price_=tr1.select_one('strong.js-track-hp-rt-room-price') 
                price_= price_.text.strip('\t\r\n\€xa')
                price.append(float(re.sub('[^0-9,]', "", price_).replace(",", ".")))
            elif tr1.find('span', class_='hprt-price-price-standard'):
                price_=tr1.find('span', class_='hprt-price-price-standard')
                price_= price_.text.strip('\t\r\n\€xa')
                price.append(float(re.sub('[^0-9,]', "", price_).replace(",", ".")))
            else :
                text_file = open("soup_error_price.txt", "w")
                text_file.write(str(hotel_soup))
                text_file.close()
                continue
           
            
            if tr.select_one('span.info'):
                room_size_=tr.select_one('span.info').text
                room_size.append(re.sub('[^0-9,]', "", room_size_))
            elif tr.select_one('div.info'):
                room_size_=tr.select_one('div.info').text
                room_size.append(re.sub('[^0-9,]', "", room_size_))
            else:
                room_size.append(None)
           
            for el in tr.select('ul.hp-rt__policy__list'):
                print(el)
                policy_opt_=el.text
            print(policy_opt_)
            #policy_opt.append(policy_opt_[1:])
            #breakfast_opt.append(policy_opt_[0])
           

            try :
                max_occ_=tr['data-occupancy']
    
            except KeyError:
                continue
            max_occ.append(max_occ_)
            
            try:
                room_left_=tr.select_one('span.only_x_left').text
                room_left_=re.sub('[^0-9,]', "", room_left)
            except:
                room_left_=None
            room_left.append(room_left)
            
            
        print('price: ',price)
        print('room size: ',room_size)
        print ('room type: ',room_type)
        print('breakfast options: ',breakfast_opt)
        print('policy_opt: ',policy_opt)
        print('max occ: ',max_occ)
        print('room left: ',room_left)
        print('room facilities: ',room_facilities[0])
        print("done in %0.3fs" % (time() - t0))
        return hotel_data(hotel_id,day_in,day_out,price,av_rating,room_type,room_size,breakfast_opt,policy_opt,room_left,room_facilities)
    else:
        print('no available rooms')
        return None
    
##################################################################################################

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
        n_room=len(i.price)
        for j in range(0,n_room):
            hotel_id=i.hotel_id
            day_in=i.day_in
            day_out=i.day_out
            price=i.price[j]
            av_rating=i.av_rating
            search_date=i.search_date
            room_type=i.room_type[j]
            room_size=i.room_size[j]
            breakfast_opt=i.breakfast_opt[j]
            policy_opt=i.policy_opt[j]
            room_left=i.room_left[j]
            room_facilities=i.room_facilities[j]
    
        SQL = '''BEGIN;
                 INSERT INTO hotel_data (hotel_id,day_in,day_out,price,av_rating,search_date,room_type,room_size,breakfast_opt,policy_opt,room_left,room_facilities)
	         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                 ;
                 COMMIT;'''

        data = (hotel_id,day_in,day_out,price,av_rating,search_date,room_type,room_size,breakfast_opt,policy_opt,room_left,room_facilites)

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
location='Chatillon'

day_in = datetime.datetime.strptime(day_in_str,'%Y-%m-%d')
day_out= datetime.datetime.strptime(day_out_str, '%Y-%m-%d')

if args.scraping_type == 1:
    print("type of scraping: hotel_table_update")
    hotel_table_list=crawler(type_of_location,location,day_in,day_out)
    #db updating hotel_list in webcraling postgress db
    db_hotel_list_update(hotel_table_list)

elif args.scraping_type == 2:   
    print("type of scraping: hotel_data_update ")
    hotel_data_list=crawler(type_of_location,location,day_in,day_out)
    #db updating hotel_data in webcrawling postgress db
    db_hotel_data_update(hotel_data_list)

elif args.scraping_type == 3:   
    print("type of scraping: hotel_reviews_update ")
    hotel_reviews_list=crawler(type_of_location,location,day_in,day_out)
    #db updating hotel_reviews in webcrawling postgress db
    db_hotel_reviews_update(hotel_reviews_list)

else:
    print("chose type of scraping, -h for help")

print("scraping and db updating done in %0.3fs" % (time() - start))

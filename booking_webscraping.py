#!/usr/bin/python

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
from fake_useragent import FakeUserAgentError

import requests
import re

base_url='http://www.booking.com'

def crawler(location,research_options):

#for the specific location,day_in,day_out,year gives hotel_table=[[hotel_name],[av_rating],[price]]

    class hotel_table(object):
        def __init__(self, name,av_rating,price):
            self.name = name
            self.av_rating=av_rating
            self.price = price
    

    hotel_list=[]

    #setting random user agent
    #ua = UserAgent()
    try:
        ua = UserAgent()
    except FakeUserAgentError:
        print("Connection error, please verify your connection")
        return

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

    #loop  over all the pages containing research results iterating on the URL offset parameter
    #loop  stops when it finds error message "take control of your search"

    for i in range(0,1): #set the number of visited pages 
        offset=i*30
        url='https://www.booking.com/searchresults.it.html?aid=376372&label=it-lDVzMq0mnhJhz2mtPHkScQS193330384278%3Apl%3Ata%3Ap1%3Ap21.448.000%3Aac%3Aap1t1%3Aneg%3Afi%3Atiaud-285284110726%3Akwd-65526620%3Alp20523%3Ali%3Adec%3Adm&sid=2dfa50ff5b2bd1452209c863835a0dec&checkin_month='+str(month_in)+'&checkin_monthday='+str(day_in)+'&checkin_year='+str(year_in)+'&checkout_month='+str(month_out)+'&checkout_monthday='+str(day_out)+'&checkout_year='+str(year_out)+'&class_interval=1&dest_id=-110502&dest_type=city&dtdisc=0&group_adults=2&group_children=0&inac=0&index_postcard=0&label_click=undef&no_rooms=1&postcard=0&raw_dest_type=city&room1=A%2CA&sb_price_type=total&sb_travel_purpose=leisure&src=index&src_elem=sb&ss='+location+'&ss_all=0&ssb=empty&sshis=0&ssne='+location+'&ssne_untouched='+location+'&rows=30&offset='+str(offset)
        print (url)

        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
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

            try:
                response = requests.get(url, headers=headers)
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print (e)
                return

            response = requests.get(hotel_link, headers=headers).text.strip('\t\r\n')
            soup = bs(response,'lxml')#.encode('utf-8')
            if soup.find('span', class_='sr-hotel__name'):
                hotel_name=soup.find('span', class_='sr-hotel__name').text.strip('\t\r\n')
            else:
                hotel_name= None
            av_rating=soup.find('span', class_='review-score-badge').text.strip('\t\r\n')
            #av_rating=float(re.sub('[^0-9,]', "", av_rating).replace(",", "."))
            if soup.select_one('strong.availprice'):
                price=soup.select_one('strong.availprice').select('b')
                price = price[0].text.strip('\t\r\n\€xa')
                price=float(re.sub('[^0-9,]', "", price).replace(",", "."))
            else :
                price= None
            

            #text_file = open("Output_soup.txt", "w")
            #text_file.write(str(soup))
            #text_file.close()

            #print(hotel_name)
            #print(av_rating)
            #print(price)
            hotel_list.append(hotel_table(hotel_name,av_rating,price))

            for i in hotel_list :
                print (i.name)

    return (hotel_list)


#research_options=[day_in,month_in,year_in,day_out,month_out,year_out]
research_options=[20,9,2017,21,9,2017]



crawler('aosta',research_options)


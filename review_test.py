from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import requests
import re

def find_reviews(hotel_link,headers) :
    post=[]
    hotel_link=hotel_link+'#tab-reviews'
    print(hotel_link)
    
    response = requests.get(hotel_link,headers=headers).text
    soup = bs(response,'lxml')
     
    text_file = open("Output_soup_post.html", "w")
    text_file.write(str(soup))
    text_file.close()

    for element in soup.find_all('p', class_='althotelsReview2'):
        post=element.find('span', class_=False).text
    print (post)         

def find_reviews_1(hotel_link,headers,soup):

    post_neg=[]

    link_to_rev=soup.find('a', class_='show_all_reviews_btn')['href']
    link_to_rev='http://www.booking.com'+link_to_rev
    response = requests.get(link_to_rev,headers=headers).text
    soup=bs(response,'lxml')

    for element in soup.find_all('p', class_='review_neg'):
        element=element.text #to do: remove 눇
        post_neg.append(element)
    print (post_neg)

    for element in soup.find_all('p', class_='review_pos'):
        element==element.text
        post_pos.append(element)
    print (post_pos)

    text_file = open("Output_soup_post.html", "w")
    text_file.write(str(soup))
    text_file.close()

    print (link_to_rev)

hotel_link='https://www.booking.com/hotel/it/milleluci.it.html?label=gen173nr-1FCAEoggJCAlhYSDNiBW5vcmVmaHGIAQGYARTCAQN4MTHIAQ_YAQHoAQH4AQuSAgF5qAID;sid=2dfa50ff5b2bd1452209c863835a0dec;all_sr_blocks=32225104_88456064_0_1_0;checkin=2017-09-15;checkout=2017-09-16;dest_id=-110502;dest_type=city;dist=0;group_adults=2;group_children=0;hapos=1;highlighted_blocks=32225104_88456064_0_1_0;hpos=1;no_rooms=1;room1=A%2CA;sb_price_type=total;srepoch=1505477610;srfid=3babbeb66c3a14015eb899496f5595e2ff96a3f8X1;srpvid=fed655f468950056;type=total;ucfs=1&'


#setting random user agent
ua=UserAgent()
headers={'User-Agent': ua.random}

#try to pass with http link+tab-review

#find_reviews(hotel_link,headers)


#try to find reviews link
response = requests.get(hotel_link,headers=headers).text
soup = bs(response,'lxml')
find_reviews_1(hotel_link,headers,soup)

#next step: loop over the pages of comments iterating in the link the number of page

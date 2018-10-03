# far2-webcrawling

## Purpose
The aim of this project is to provide a tool to collect hotel data found on *booking.com* website. Multiple types of data are retrieved and sorted in a structured way, using a postgres database, so that they could be easily recalled and analysed in future steps. 
## General description
*booking_dynamic_scraping.py* is a *Python3* script which can access all pages of a booking.com search, given specific location, check-in and check-out dates. It gets the link of each hotel listed on that pages, and opens it. Then, it retrieves hotel data and collects them in a dedicated postgres database. If this specific database does not exist, presumably the first the script is run, it asks for its creation: after the user enters the name of the new db, it automatically gets the current system user name as database user. All tables and columns necessary to the data collection will be initiated. After db creation, db keys will be saved in *dbkey.txt* file, which will be used for the future connections from python script to posgres db. You can change db name and user, and consequently db stored keys, at any time by running *conn_db*.<br/>
The script is structured in 4 different types of scraping, corresponding to the 4 db tabl updates. This is done with the use of argparse functions. So, when you run the program you have to specify which type of scraping you want to perform by typing:
* -t for hotel_list update
* -tt  for hotel_data update
* -ttt for hotel_ratings update
* -tttt for hotel_reviews update
<br/>
otherwise an help menu is visualized and you can restart the script using the desired scraping specification. This structure allows looking for variations in hotel data, such as prices, at the level of days scale, in hotel ratings at a month scale, to add new reviews every week or month. Locations and hotel_list data, such as hotel names, are less variable and they need only few updates.<br/>
*booking_list_scraping* consits in a modified version in wich hotel links are retrieved from a given list (initialised as csv and inserted in the db). 


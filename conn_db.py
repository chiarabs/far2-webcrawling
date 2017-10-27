#!/usr/bin/python3
import psycopg2 as ps
import subprocess
import getpass

def init_db():

    subprocess.check_output(['which psql'], shell=True)
    psql_check=subprocess.check_output(['echo $?'], shell=True)
    if psql_check==1:
        resp=input('You need postgress, do you want intall it now? (Y/N)')
        if resp== 'Y':
            subprocess.check_output['sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common']
        elif resp =='N':
            print('Please, make sure to have a postgres version installed otherwise you can not run this script')
        else: 
            Print('Command not find')
            return 0
   
    #if subprocess.check_output(['sudo service postgresql start ','sudo -su postgres'])
    sys_user_name=getpass.getuser()
    #user_name=input('set databse user: ')
    db_name=input('set database name: ')
    #command='sudo -u postgres -i createuser -d '+user_name
    command1='sudo -u postgres -i createdb '+db_name#+' -O '+user_name
    #subprocess.check_output([command],shell=True)
    subprocess.check_output([command1],shell=True)
    print('%s database correctly created') 
    print(db_name,' ',sys_user_name)

    con = ps.connect('dbname='+db_name+' user='+sys_user_name)
    cur = con.cursor()

    sql = 'CREATE TABLE locations (id_loc INTEGER PRIMARY KEY, loc_name VARCHAR(50), loc_type VARCHAR(10))'
    cur.execute(sql)
    con.commit()

    data=[(-110502,'Aosta','city'),(-116466,'Courmayeur','city'),(-112164,'Breuil-Cervinia','city'),(-115309,'Chatillon','city'),(-127084,'Saint-Vincent','city'),(-125429,'Pont-Saint-Martin','city'),(900040059,'Ayas','city'),(-115778,'Cogne','city'),(4251,'Apli italiane','region'),(3761,'Alpi austriache','region'),(3832,'Alpi svizzere','region'),(3774,'Alpi francesi','region'),(2151,'Alpi slovene','region'),(913,'Valle d Aosta','region')]
    records_list_template = ','.join(['%s'] * len(data))
    sql = 'INSERT INTO locations (id_loc,loc_name,loc_type) VALUES {}'.format(records_list_template)
    
    cur.execute(sql,data)

    sql = '''CREATE TABLE hotel_list (
hotel_name VARCHAR(80), location INTEGER, hotel_address VARCHAR(200), hotel_id INTEGER, hotel_type VARCHAR(20), hotel_star INTEGER, hotel_facilities VARCHAR(2000), loc_name VARCHAR(50), loc_type VARCHAR(10), hotel_link VARCHAR(500)
       ) 
       '''
   

    if cur.execute(sql):
        print('hotel_list table correctly created')
    else:
        print('error during hotel_list table creation')
    
    try:
        sql=  '\copy hotel_list ( hotel_link ) FROM %s WITH CSV HEADER;'
        data=('hotel_link_list.csv',)
        cur.excecute(sql,data)
    except:
        pass

    sql = '''CREATE TABLE hotel_data (
hotel_id INTEGER, day_in DATE, day_out DATE, search_date DATE, room_id INTEGER, room_type VARCHAR(100), room_size NUMERIC(4,0), price NUMERIC(8,2), breakfast_opt VARCHAR(500), policy_opt VARCHAR(500), room_left NUMERIC(2,0), room_facilities VARCHAR(1000), max_occ NUMERIC(2,0), inclusive VARCHAR(300),  non_inclusive VARCHAR (300)
       ) 
       '''
    cur.execute(sql)
    print('hotel_data table correctly created')

    sql = '''CREATE TABLE hotel_ratings (
hotel_id INTEGER, day_in DATE, day_out DATE, search_date DATE, av_rating NUMERIC(4,0),superb_score NUMERIC(4,0), good_score NUMERIC(4,0), average_score NUMERIC(4,0), poor_score NUMERIC(4,0), very_poor_score NUMERIC(4,0), breakfast_score NUMERIC(3,1), clean_score  NUMERIC(3,1), comfort_score  NUMERIC(3,1), location_score  NUMERIC(3,1), services_score  NUMERIC(3,1), staff_score  NUMERIC(3,1), value_score  NUMERIC(3,1), wifi_score  NUMERIC(3,1), n_ratings NUMERIC(6,0)
       ) 
       '''
    cur.execute(sql)
    print('hotel_ratings table correctly created')

    sql = '''CREATE TABLE hotel_reviews (
hotel_id INTEGER, post_title VARCHAR(100), positive_comment VARCHAR, negative_comment VARCHAR,post_date DATE, author_name VARCHAR(50),author_nat VARCHAR(50),author_group VARCHAR(50), score NUMERIC(4,2)
       ) 
       '''
    cur.execute(sql)
    print('hotel_reviews table correctly created')
    
    con.commit()
    cur.close()
    con.close()
    
    key={'db_name':db_name,'user_name':sys_user_name}
    text_file = open("dbkey.txt", "w")
    text_file.write(str(key))
    text_file.close()

    return 1
##################################################################################

def readingdbkey():
    with open('dbkey.txt', 'r') as f:
        s = f.read()
        key=eval(s)
        return key

#################################################################################
def db_key_mod():

    try:
        key=readingdbkey()
        db_name=key['db_name']
        user_name=key['user_name']
        p=input('Database %s with user %s: enter c to change db or user: '%(db_name,user_name))
        if p == 'c':
            db_name=input('Database name: ')
            user_name=input('User name: ')
            key={'db_name':db_name,'user_name':user_name}
            text_file = open("dbkey.txt", "w")
            text_file.write(str(key))
            text_file.close()
    except FileNotFoundError:
        p = input('No database key: create a new db (y)?  Or try to enter db name and user:')
        if p =='y':
            init_db()
        else:
            db_name=input('Database name: ')
            user_name=input('User name: ')
            key={'db_name':db_name,'user_name':user_name}
            text_file = open("dbkey.txt", "w")
            text_file.write(str(key))
            text_file.close()
    return key


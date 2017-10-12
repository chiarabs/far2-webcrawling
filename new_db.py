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
   

    sql = '''CREATE TABLE hotel_list (
hotel_name VARCHAR(50), location VARCHAR(50), hotel_address VARCHAR(200), hotel_id INTEGER, hotel_type VARCHAR(20), hotel_star INTEGER, hotel_facilities VARCHAR(2000), loc_name VARCHAR(50), loc_type VARCHAR(10)
       ) 
       '''
    if cur.execute(sql):
        print('hotel_list table correctly created')
    else:
        print('error during hotel_list table creation')

    sql = '''CREATE TABLE hotel_data (
hotel_id INTEGER, day_in DATE, day_out DATE, search_date DATE, room_id INTEGER, room_type VARCHAR(50), room_size NUMERIC(4,0), price NUMERIC(8,2), breakfast_opt VARCHAR(500), policy_opt VARCHAR(500), room_left NUMERIC(2,0), room_facilities VARCHAR(1000), max_occ NUMERIC(2,0), inclusive VARCHAR(100),  non_inclusive VARCHAR (100)
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
hotel_id INTEGER, positive_comment VARCHAR, negative_comment VARCHAR,post_date DATE, author_name VARCHAR(50),author_nat VARCHAR(50),author_group VARCHAR(50), score NUMERIC(4,2)
       ) 
       '''
    cur.execute(sql)
    print('hotel_reviews table correctly created')
    
    con.commit()
    cur.close()
    con.close()
    
    return 1

init_db()

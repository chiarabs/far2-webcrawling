from distutils.core import setup

setup(name='booking_list_scraping',
      version='3.14',
      author='Greg Ward',
      #author_email='',
      url='https://github.com/chiarabs/far2-webcrawling',
      packages=[],
      py_modules=['booking_list_scraping','conn_db'],
      data_files=[('', ['hotel_link_list.csv','search_opt.txt'])],
      requires=['BeautifulSoup','fake_useragent','requests','psycopg2','argparse']
     )





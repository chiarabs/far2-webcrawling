from setuptools import setup, find_packages
setup(
    name="booking_list_scraping",
    version="3.14",
    packages=find_packages(),
    scripts=['booking_list_scraping.py','conn_db.py'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=['BeautifulSoup','fake_useragent','requests','psycopg2','argparse'],

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['hotel_link_list.csv','search_opt.txt'],
        # And include any *.msg files found in the 'hello' package, too:
      #  'hello': ['*.msg'],
    })

    # metadata for upload to PyPI
    #author="",
    #author_email="",)
    #description="This script allows to scrape booking.com hotel data and collect them in a structured way using a psql db",

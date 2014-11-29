import click
import requests
import time
from datetime import datetime, date, timedelta

import pymongo
from pymongo import MongoClient


from xml.etree import ElementTree

MAX_AGE_DAYS = 10
TIME_SPAN_SECONDS = 10
API_URL_TEMPLATE = 'http://devapi.aljazeera.com/v1/en/stories/{region}?format={format}&apikey={key}&pagesize={pagesize}&pagenumber={pagenumber}'

PAGE_SIZE = 20

LAST_GUID_FILE_TEMPLATE = 'last_guid_{section}'

DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"


def get_stories(region, key):
    last_guid_file = LAST_GUID_FILE_TEMPLATE.format(section=region)

    current_last_guid = None
    pagenumber = 1
        

    try:
        while True:
            params = { 
                'region': region,
                'format': 'json',
                'key': key, 
                'pagesize': PAGE_SIZE,
                'pagenumber': pagenumber
            }
            try:
                with open(last_guid_file) as f:
                    last_guid = f.readline() or None
            except IOError as e:
                last_guid = None
                

            url = API_URL_TEMPLATE.format(**params)
            print 'Requesting: %s' % url
            req = requests.get(url)
            pagenumber += 1
            
            
            try:
                res = req.json()
            except Exception as e:
                import ipdb;ipdb.set_trace()
                
                continue
                
            for story in res['stories']:
                current_last_guid = current_last_guid or story['guid']

                if last_guid == story['guid']:
                    return 

                story['pub_date'] = datetime.strptime(story['pubDate'], DATE_FORMAT)
                del story['pubDate']
                
                if datetime.today() - timedelta(days=MAX_AGE_DAYS) >  story['pub_date']:
                    print 'Discarting stories prior to %s' % (datetime.today() - timedelta(days=MAX_AGE_DAYS))
                    return 

                yield story

    finally:
        with open(last_guid_file, 'w') as f:
            f.write(current_last_guid)


    
    # guid = story['guid']

    #     # guid = story['guid']
    #     # link = story['link']
    #     # title = story['title']

    #     # description = story['description']
    #     # body = story['body']
    #     # largeimage = story['largeimage']
    #     # smallimage = story['smallimage']
    #     # video = story['video']
    #     # metadata = story['metadata']
    #     # source = story['source']
    #     # pub_date = story['pubDate']
    #     # author = story['author']


    # import ipdb;ipdb.set_trace()


@click.command()
@click.option('--time_span', '-t', default=TIME_SPAN_SECONDS)
@click.option('--key', '-k', default='ROkhXK3k48LID1n1AATbwEW9sxfW0Zqn')
@click.option('--regions', default=['middle_east'], multiple=True)
@click.option('--mongo_host', default='localhost')
@click.option('--mongo_port', default=27017)
def fetcher(time_span, key, regions, mongo_host, mongo_port):
    import ipdb;ipdb.set_trace()
    
    mongo_conn = MongoClient(mongo_host, mongo_port)
    mongo_database = mongo_conn.newstogo
    mongo_collection = mongo_database.stories

    mongo_collection.ensure_index('guid', unique=True)

    while True:

        for region in regions:

            for story in get_stories(region, key):
                print 'A story: %s' % story['guid']
                mongo_collection.insert(story)


            time.sleep(TIME_SPAN_SECONDS)
            



if __name__ == '__main__':
    fetcher()



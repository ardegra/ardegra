import sys

import pymongo

from lib.config import Config
from lib.spider.ForumSpider1 import ForumSpider1
from lib.spider.NewsSpider1 import NewsSpider1
from lib.spider.NewsSpider2 import NewsSpider2

def run():
  spider_name = sys.argv[1]
  client      = pymongo.MongoClient("mongodb://{}/ardegra".format(Config.DATABASE_ADDRESS))
  try:
    db       = client["ardegra"]
    document = db.spiders.find_one({"name": spider_name})
    
    if document["type"]["name"] == "Forum Spider 1":
      spider = ForumSpider1(name=spider_name)
    elif document["type"]["name"] == "News Spider 1":
      spider = NewsSpider1(name=spider_name)
    elif document["type"]["name"] == "News Spider 2":
      spider = NewsSpider2(name=spider_name)
    spider.run()
  except Exception as err:
    print(str(err))
  finally:
    client.close()

if __name__ == "__main__":
  run()

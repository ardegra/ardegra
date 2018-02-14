import pymongo
import requests

from raven import Client
from logbook import Logger

from lib.config import Config
from lib.exceptions import DuplicateDocumentException

class NewsSpider1:
  def __init__(self, name=None, **kwargs):
    self.raven_client          = Client()
    self.logger                = Logger("NewsSpider1")
    
    self.name                  = name
    self.country               = kwargs.get("country", None)
    self.xpath                 = kwargs.get("xpath", None)
    self.index_url             = kwargs.get("indexUrl", None)
    self.index_max_page_number = kwargs.get("indexMaxPageNumber", None)
    self.ignore_domain_list    = kwargs.get("ignoreDomainList", None)
    self.entry_date_parser     = kwargs.get("entryDateParser", None)
    self.category              = "News"
    
  def prepare_data(self):
    client = pymongo.MongoClient("mongodb://{}/ardegra".format(Config.DATABASE_ADDRESS))
    try:
      db                         = client["ardegra"]
      document                   = db.spiders.find_one({"name": self.name})
      self.country               = document["country"]
      self.xpath                 = document["xpath"]
      self.index_url             = document["indexUrl"]
      self.index_max_page_number = document["indexMaxPageNumber"]
      self.ignore_domain_list    = document["ignoreDomainList"]
      self.entry_date_parser     = document["entryDateParser"]
      
      if type(self.index_url) is str:
        self.index_url = [self.index_url]
    except Exception as err:
      self.raven_client().captureException()
      self.logger.error("{}".format(str(err)))
    finally:
      client.close()

  def crawl_article_url(self, index_url):
    article_url_list = []
    for x in range(1, (self.index_max_page_number + 1)):
      real_index_url = index_url.format(page_number=x)
      self.logger.debug("Getting article_url from: {}".format(real_index_url))
      
      api_url = "{}/spider/news/extract/articleUrl".format(Config.BASE_EXTRACT_API)
      r       = requests.post(api_url, json={
        "url": real_index_url,
        "xpath": self.xpath
      })
      article_url_list.extend(r.json()["articleUrl"])
      self.logger.debug("Current article_url_list count: {}".format(len(article_url_list)))
    return article_url_list

  def crawl_article(self, article_url, continue_on_duplicate):
    self.logger.debug("article_url: {}".format(article_url))
    api_url = "{}/spider/news/extract/article".format(Config.BASE_EXTRACT_API)
    r       = requests.post(api_url, json={
      "url": article_url,
      "xpath": self.xpath
    })
    article = r.json()
    
    api_url = "{}/spider/news/save/article".format(Config.BASE_EXTRACT_API)
    r       = requests.post(api_url, json={
      "article": article,
      "permalink": article_url,
      "country": self.country,
      "crawlerName": self.name,
      "entryDateParser": self.entry_date_parser
    })
    
    self.logger.debug("continue_on_duplicate: {}".format(continue_on_duplicate))
    self.logger.debug("duplicate: {}".format(r.json()["duplicate"]))
    if r.json()["duplicate"]:
      if not continue_on_duplicate:
        raise DuplicateDocumentException("Duplicate document!")
    else:
      self.logger.debug("Saved with id: {}".format(r.json()["insertedId"]))
  
  def check_duplicate(self, article_url):
    print("[NewsSpider1] Checking duplicate: {}".format(article_url))
    api_url               = "{}/spider/news/info/isArticleDuplicate".format(Config.BASE_EXTRACT_API)
    r                     = requests.post(api_url, json={
      "url": article_url
    })
    return r.json()["duplicate"]

  def run(self):
    self.prepare_data()
    
    try:
      for index_url in self.index_url:
        self.logger.debug("index_url: {}".format(index_url))
        article_url_list      = self.crawl_article_url(index_url)
        is_duplicate          = self.check_duplicate(article_url_list[-1])
        continue_on_duplicate = False if is_duplicate else True
      
        try:
          for article_url in article_url_list:
            ignored = False
            for ignored_domain in self.ignore_domain_list:
              if ignored_domain in article_url:
                ignored = True
            if not ignored:
              self.crawl_article(article_url, continue_on_duplicate)
        except DuplicateDocumentException as err:
          self.logger.debug(str(err))
    except Exception as err:
      self.raven_client.captureException()
      self.logger.error("{}".format(str(err)))
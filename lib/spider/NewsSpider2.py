import pymongo
import requests
import arrow

from lib.config import Config

class NewsSpider2:
  def __init__(self, name=None, **kwargs):
    self.name               = name
    self.index_end_date     = kwargs.get("indexEndDate", None)
    self.index_start_date   = kwargs.get("indexStartDate", None)
    self.index_url          = kwargs.get("indexUrl", None)
    self.country            = kwargs.get("country", None)
    self.ignore_domain_list = kwargs.get("ignoreDomainList", [])
    self.xpath              = kwargs.get("xpath", None)
    self.entry_date_parser  = kwargs.get("entryDateParser", None)
    self.category           = "News"

  def prepare_date(self):
    client = pymongo.MongoClient("mongodb://{}/ardegra".format(Config.DATABASE_ADDRESS))
    try:
      db                      = client["ardegra"]
      document                = db.spiders.find_one({"name": self.name})
      self.country            = document["country"]
      self.xpath              = document["xpath"]
      self.index_url          = document["indexUrl"]
      self.index_start_date   = document["indexStartDate"]
      self.index_end_date     = document["indexEndDate"]
      self.ignore_domain_list = document["ignoreDomainList"]
      self.entry_date_parser  = document["entryDateParser"]
    except Exception as err:
      print("[NewsSpider2] {}".format(str(err)))
    finally:
      client.close()

  def crawl_article_url(self):
    start_date = arrow.get(self.index_start_date)
    end_date   = arrow.get(self.index_end_date)
    
    article_url_list = []
    last_article_url = None
    for date in arrow.Arrow.span_range("day", end_date, start_date):
      begining, ending   = date
      current_date      = begining
      current_index_url = self.index_url.format(
        month=current_date.format("MM"),
        date=current_date.format("DD"),
        year=current_date.format("YYYY")
      )
      print("[NewsSpider2] current_index_url: {}".format(current_index_url))

      api_url = "{}/spider/news/extract/articleUrl".format(Config.BASE_EXTRACT_API)
      r       = requests.post(api_url, json={"xpath": self.xpath, "url": current_index_url})
      article_url_list.extend(r.json()["articleUrl"])
      print("[NewsSpider2] article_url_list: {}".format(len(article_url_list)))
    if last_article_url is None:
      last_article_url = article_url_list[-1]
    return article_url_list, last_article_url

  def crawl_article(self, article_url, continue_on_duplicate):
    print("[NewsSpider2] Extracting article_url: {}".format(article_url))
    api_url = "{}/spider/news/extract/article".format(Config.BASE_EXTRACT_API)
    r       = requests.post(api_url, json={"xpath": self.xpath, "url": article_url})
    article = r.json()
    
    api_url = "{}/spider/news/save/article".format(Config.BASE_EXTRACT_API)
    r       = requests.post(api_url, json={
      "article": article,
      "country": self.country,
      "crawlerName": self.name,
      "entryDateParser": self.entry_date_parser,
      "permalink": article_url
    })
    
    if r.json()["duplicate"] and not continue_on_duplicate:
      raise Exception("Duplicate document!")
    else:
      print("[NewsSpider2] Saved with id: {}".format(r.json()["insertedId"]))

  def check_duplicate(self, article_url):
    api_url               = "{}/spider/news/info/isArticleDuplicate".format(Config.BASE_EXTRACT_API)
    r                     = requests.post(api_url, json={"url": article_url})
    return r.json()["duplicate"]

  def run(self):
    self.prepare_date()
    article_url_list, last_article_url = self.crawl_article_url()
    is_duplicate                       = self.check_duplicate(last_article_url)
    continue_on_duplicate              = False if is_duplicate else True
    print("[NewsSpider2] continue_on_duplicate: {}".format(continue_on_duplicate))
    
    try:
      for article_url in article_url_list:
        ignored = False
        for ignored_domain in self.ignore_domain_list:
          if ignored_domain in article_url:
            ignore = True
        if not ignored:
          self.crawl_article(article_url, continue_on_duplicate)
    except Exception as err:
      print("[NewsSpider2] {}".format(str(err)))
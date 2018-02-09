import pymongo
import requests

from lib.config import Config

class ForumSpider1:
  def __init__(self, name=None, **kwargs):
    self.fast_test     = kwargs.get("fast_test", False)
    self.name          = name
    self.country       = kwargs.get("country", None)
    self.xpath         = kwargs.get("xpath", None)
    self.category      = "Forum"
    self.category_list = kwargs.get("category_list", [])

  def prepare_data(self):
    client = pymongo.MongoClient("mongodb://{}/ardegra".format(Config.DATABASE_ADDRESS))
    try:
      db                 = client["ardegra"]
      document           = db.spiders.find_one({"name": self.name})
      self.country       = document["country"]
      self.xpath         = document["xpath"]
      self.category_list = document["categoryList"]
    except Exception as err:
      print("[ForumSpider1] {}".format(str(err)))
    finally:
      client.close()
      
  def crawl_thread_url(self, category_url):
    api_url     = "{}/spider/forum/extract/category/lastPageUrl".format(Config.BASE_EXTRACT_API)
    r           = requests.post(api_url, json={
      "url": category_url,
      "xpath": self.xpath
    })
    current_url = r.json()["lastPageUrl"] if not self.fast_test else category_url
    
    thread_url_list = []
    while current_url is not None:
      print("[ForumSpider1] current_url: {}".format(current_url))
      api_url = "{}/spider/forum/extract/threadUrl".format(Config.BASE_EXTRACT_API)
      r       = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      thread_url_list.extend(r.json()["threadList"])
      print("[ForumSpider1] Total Thread List: {}".format(len(thread_url_list)))
      
      if self.fast_test:
        break
      
      api_url     = "{}/spider/forum/extract/category/prevPageUrl".format(Config.BASE_EXTRACT_API)
      r           = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      current_url = r.json()["prevPageUrl"]
    return thread_url_list
    
  def crawl_thread(self, thread_url):
    api_url       = "{}/spider/forum/extract/thread/lastPageUrl".format(Config.BASE_EXTRACT_API)
    r             = requests.post(api_url, json={
      "url": thread_url,
      "xpath": self.xpath
    })
    last_page_url = r.json()["lastPageUrl"]
    
    if last_page_url is None:
      self.forward_crawling(thread_url)
    else:
      print("[ForumSpider1] Getting FirstPostID: {}".format(thread_url))
      api_url               = "{}/spider/forum/extract/post/firstPostId".format(Config.BASE_EXTRACT_API)
      r                     = requests.post(api_url, json={
        "url": thread_url,
        "xpath": self.xpath
      })
      continue_on_duplicate = not r.json()["duplicate"]
      self.backward_crawling(last_page_url, continue_on_duplicate)
  
  def forward_crawling(self, url):
    post_list   = []
    has_next    = True
    current_url = url
    
    while has_next:
      print("[ForumSpider1][Forward Crawling] current_url: {}".format(current_url))
      api_url = "{}/spider/forum/extract/post".format(Config.BASE_EXTRACT_API)
      r       = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      
      for post in r.json()["postList"]:
        api_url = "{}/spider/forum/save/post".format(Config.BASE_EXTRACT_API)
        r       = requests.post(api_url, json={
          "post": post,
          "crawlerName": self.name,
          "country": self.country
        })
        print("[ForumSpider1][Forward Crawling] duplicate: {}\n[ForumSpider1][Forward Crawling] permalink: {}".format(
          r.json()["duplicate"], post["permalink"]
        ))
      
      api_url     = "{}/spider/forum/extract/thread/nextPageUrl".format(Config.BASE_EXTRACT_API)
      r           = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      current_url = r.json()["nextPageUrl"]
      has_next    = True if current_url is not None else False

  def backward_crawling(self, url, continue_on_duplicate):
    current_url  = url
    post_list    = []
    has_prev     = True
    can_continue = True
    
    while has_prev and can_continue:
      print("[ForumSpider1][Backward Crawling] current_url: {}".format(current_url))
      api_url = "{}/spider/forum/extract/post".format(Config.BASE_EXTRACT_API)
      r       = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      
      for post in r.json()["postList"]:
        api_url = "{}/spider/forum/save/post".format(Config.BASE_EXTRACT_API)
        r = requests.post(api_url, json={
          "post": post,
          "crawlerName": self.name,
          "country": self.country
        })
        print("[ForumSpider1][Backward Crawling] duplicate: {}\n[ForumSpider1][Backward Crawling] permalink: {}".format(
          r.json()["duplicate"], post["permalink"]
        ))
        
        print("[ForumSpider1][Backward Crawling] continue_on_duplicate: {}".format(continue_on_duplicate))
        if r.json()["duplicate"] and not continue_on_duplicate:
          print("[ForumSpider1][Backward Crawling] Breaking! because of duplicate")
          can_continue = False
          break

      print("[ForumSpider1][Backward Crawling] Getting prevPageUrl of: {}".format(current_url))
      api_url     = "{}/spider/forum/extract/thread/prevPageUrl".format(Config.BASE_EXTRACT_API)
      r           = requests.post(api_url, json={
        "url": current_url,
        "xpath": self.xpath
      })
      current_url = r.json()["prevPageUrl"]
      has_prev    = True if current_url is not None else False
  
  def run(self):
    self.prepare_data()
    for category in self.category_list:
      thread_url_list = self.crawl_thread_url(category)
      for thread_url in thread_url_list:
        self.crawl_thread(thread_url)
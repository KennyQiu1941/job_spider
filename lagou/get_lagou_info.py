from lagou.spider_lagou import LaGouspider
import pymongo
import threading
# spider_lagou = LaGouspider(search_name='python')
'''由于编码的关系搜索为中文时候必须重写refere这个url'''

class Pylagou(LaGouspider):
    def __init__(self, search_name, city):
        super(Pylagou, self).__init__(search_name, city)
        con = pymongo.MongoClient('192.168.1.60', 27017)
        zhaopin = con.zhaopin
        self.mdb = zhaopin.python_lagou


class Spider_lagou(LaGouspider):
    def __init__(self, search_name, city):
        super(Spider_lagou, self).__init__(search_name, city)
        con = pymongo.MongoClient('192.168.1.60', 27017)
        zhaopin = con.zhaopin
        self.mdb = zhaopin.spider_lagou
        self.referer = 'https://www.lagou.com/jobs/list_%E7%88%AC%E8%99%AB'


pylagou = Pylagou(search_name='python',city='')
getid = pylagou.get_all
getdetail =pylagou.get_detail

splagou = Spider_lagou(search_name='爬虫',city='')
sgetid = splagou.get_all
sgetdetail = splagou.get_detail

threading.Thread(target=getid).start()
threading.Thread(target=sgetid).start()
print('a')
for i in range(10):
    threading.Thread(target=getdetail).start()
    threading.Thread(target=sgetdetail).start()



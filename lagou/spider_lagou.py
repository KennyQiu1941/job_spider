import requests
import redis
import pymongo
import time
import random
import math
import inspect
from lxml import etree
import json
'''写入mongodb格式{'job_id':job_id,'write_time':self.now_time(),'job_info':jsondata}}'''



class LaGouspider:
    def __init__(self,search_name,city):
        self.search_name = search_name
        self.city = city
        self.used_idname = '{}{}{}'.format(self.search_name,self.city,'used') #定义rediskeyname 用过的id去重
        self.idname = '{}{}'.format(self.search_name,self.city)  #等待用的id
        self.pagename = '{}{}{}'.format(self.search_name,self.city,'page')  #待爬取的页码
        self.json_url = 'https://www.lagou.com/jobs/positionAjax.json?city={}&needAddtionalResult=false'.format(self.city)
        self.rdb = redis.Redis(host='192.168.1.60',password='****',db=5)
        self.iprdb = redis.Redis(host='192.168.1.60',password='***',db=2)
        con = pymongo.MongoClient(host='192.168.1.60',port=27017)
        zhaopin = con.zhaopin
        self.mdb = zhaopin.mdb
        self.log = zhaopin.log
        self.headers = [{'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
                        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'},
                        {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:62.0) Gecko/20100101 Firefox/62.0'},
                        {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)'},
                        {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)'}]
        self.referer = 'https://www.lagou.com/jobs/list_{}'.format(self.search_name)

    def now_time(self):#现在的时间
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


    def get_ip(self):
        tf = True
        while tf:
            ip = self.iprdb.rpoplpush('new_ip', 'new_ip')
            # print(ip)
            if ip:
                ip = json.loads(ip)
                tf = False
                return ip


    def remove_ip(self,ip):
        jsip = json.dumps(ip)
        print('请求出错重新发请求')
        self.iprdb.lrem('new_ip', jsip)
        self.iprdb.srem('heavy_remove', jsip)


    def erro_log(self,describe,error):  #记录错误的日志进入数据库参数describe需要'xxxx{类.方法名}。。。{错误内容}....{是件}'其他地方就是特定描述
        error = describe.format('{}.{}'.format(self.__class__.__name__, inspect.stack()[1][3]), error, self.now_time())
        print(error)
        self.mdb.insert({'error': error})


    def post_data(self,page):  #翻页post数据
        if page == '1':
            return {'first':'true','pn':page,'kd':self.search_name}
        else:
            return {'first':'false','pn':page,'kd':self.search_name}


    def get_json(self,post_data): #通过请求返回json
        headers = random.choice(self.headers)
        headers['Referer'] = self.referer
        # print(headers)
        # print(post_data)
        ip = self.get_ip()
        proxies = {ip[0]: '{}://{}:{}'.format(ip[0], ip[1], ip[2])}
        # print(proxies)
        try:
            reop = requests.post(url=self.json_url,headers=headers,data=post_data,proxies=proxies)
        except:
            self.remove_ip(ip=ip)
            print('{}失败'.format(proxies))
        else:
            reop_json = json.loads(reop.text)
            print('{}{}'.format(self.search_name,reop_json))
            if 'content' in reop_json:
                return reop_json['content']
            else:
                self.remove_ip(ip=ip)
                print('{}失败'.format(proxies))
                return False


    def start_requset(self): #第一次请求获取json包含的搜索到的招聘信息数量用于翻页
        reop_jons = self.get_json(post_data=self.post_data('1'))
        if reop_jons:
            zhaopin_info = reop_jons
            page = reop_jons['positionResult']['totalCount']
            return math.ceil(page/15),zhaopin_info



    def write_data(self,data):  #写入数据库这一个动作  这里有一个去重的动作
        pageno = data['pageNo']
        if pageno == 0:
            self.rdb.delete(self.pagename)
            print(pageno)
            self.rdb.rpush(self.pagename,json.dumps('False'))
        for i in data['positionResult']['result']:
            job_id = i['positionId']
            tf = self.rdb.sadd(self.used_idname,job_id)
            if tf:
                self.rdb.lpush(self.idname,job_id)
                self.mdb.insert({'job_id':job_id,'write_time':self.now_time(),'pageNO':pageno,'job_info':i})
                print('第{}页请求成功,{}不存在数据库现写入数据库'.format(pageno,job_id))


    def get_all(self):   #将所有所有搜索json数据写入mongo数据库，并将每个招聘信息id写入redis数据库进行去重，未爬取过写入队列等待爬取
        while True:
            # n = self.search_name+'page'
            # print(n)
            nn = self.rdb.exists(self.pagename)
            # print(nn)
            if not nn: #检查数据库里有吗有断点爬取页码
                allinfo= self.start_requset()
                if allinfo:
                    page , zhaopin_info = allinfo
                    print('搜索结果共{}页'.format(page))
                    self.write_data(data=zhaopin_info)
                    for x in range(2,page+1):
                        self.rdb.rpush(self.pagename,x)  #将页码写入redis这样可以实现断点爬取
                    self.rdb.rpush(self.pagename,json.dumps('False'))
            else:
                i = json.loads(self.rdb.lpop(self.pagename))
                # print(i)
                if i == 'False':
                    print('json数据爬完暂停1小时重新爬取')
                    time.sleep(3600)
                else:
                    reop_json = self.get_json(post_data=self.post_data(page=i))
                    # print(reop_json)
                    if reop_json:
                        self.write_data(data=reop_json)
                        # time.sleep(20)
                        # print('ok')
                    else:
                        self.rdb.lpush(self.pagename,i)
                        # print('请求过于频繁等待60秒')
                        # time.sleep(60)


    def get_detail(self):
        while True:
            id = self.rdb.lpop(self.idname)
            if id:
                id = json.loads(id)
                url = 'https://www.lagou.com/jobs/{}.html'.format(id)
                headers = random.choice(self.headers)
                ip = self.get_ip()
                proxies = {ip[0]: '{}://{}:{}'.format(ip[0], ip[1], ip[2])}
                try:
                    reop = requests.get(url=url,headers=headers,proxies=proxies)
                except:
                    self.remove_ip(ip=ip)
                else:
                    # print(reop.text)
                    html = etree.HTML(reop.text)
                    data = html.xpath('//dl[@id="job_detail"]//*/text()')
                    info = [i for i in data if '\n' not in i]
                    if info:
                        a = self.mdb.update({'job_id':int(id)},{'$set':{'详情':info}})
                        print('招聘信息{}写入mdb{}'.format(id,a))
                        # time.sleep(12)
                    else:
                        self.rdb.lpush(self.idname,id)
                        self.remove_ip(ip=ip)
                        # print('id：{}请求失败等待30秒'.format(id))
            time.sleep(0.1)


import requests, os
from bs4 import BeautifulSoup
from lxml import etree
import re
from multiprocessing import Pool
import pymysql

host = 'localhost'
port = 3306
database = 'mysql'
user = 'root'
password = ‘123456’
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
}
lost_lists = []


def parse():
    d_urls = ['http://hospital.iheshan.cn/Article/ShowClass.asp?ClassID=150&page={}'.format(i) for i in range(0, 7)]
    urls_list = []
    for url in d_urls:
        response = requests.get(url, headers=headers)
        response.encoding = 'gbk'
        # soup = BeautifulSoup(response.text, 'lxml')
        html = etree.HTML(response.text)
        con_url = html.xpath('//td[@class="main_tdbg_575"]/table/tr/td[2]/a[2]/@href')
        urls_list.append(con_url)
    return urls_list

def parse_xpath(url):
    resp = requests.get(url, headers=headers)
    resp.encoding = 'gbk'
    soup = BeautifulSoup(resp.text, 'lxml')
    html = etree.HTML(resp.text)
    title = soup.title.string
    content = soup.find_all(id='Zoom')[0]
    url = url
    time_b = html.xpath('//table[@class="a"]/tr[4]/td/text()')
    publish_time = re.findall('更新时间：([\d]+/[\d]+/[\d])', str(time_b))[0]
    return str(title), str(publish_time), str(content), str(url)

def xpath_anntype(title):
    ann_type = re.findall('结果', title)
    ann_ty = re.findall('中标', title)
    ann_t = re.findall('招标', title)
    ann_y = re.findall('采购', title)
    if ann_ty or ann_type:
        return '结果公告'
    if ann_t:
        return '招标公告'
    if ann_y:
        return '采购公告'


def save_mysql(title, publish_time, ann_type, content, region, source, status, url):
    conn = pymysql.connect(host, user, password, database, port, charset='utf8')
    cursor = conn.cursor()
    sql_insert = 'INSERT INTO anndatas_copy(title, publish_time, ann_type, content, region, source, status, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
    # sinsert = 'INSERT INTO anndatas_copy(title, content) VALUES (%s, %s);'
    # sql_select = 'select title from anndatas_copy'
    try:
        #元祖元祖元祖，mysql插入要元祖，元祖元祖元祖
        # cursor.execute(sql_select, ('123','123','123','132','13','13',1 ,'123'))#元祖元祖元祖，mysql插入要元祖，元祖元祖元祖
        a = cursor.execute(sql_insert, (title, publish_time, ann_type, content, '广东鹤山', '本站原创', 1, url))
        conn.commit()
        print('成功插入 {} 数据'.format(a))
    except Exception:
        conn.rollback()
        lost_lists.append(url)

def main():
    num = 1
    try:
        urls_list = parse()
        for u in urls_list:
            for i in u:
                a = parse_xpath(i)
                b = xpath_anntype(a[0])
                print('这是第{}条数据'.format(num))
                print(a+'\n'+b)
                save_mysql(a[0], a[1], b, a[2], '广东省鹤山人民医院', '本站原创', 1, a[3])
                num += 1
    except:
        pass


if __name__=='__main__':
    main()
    print(os.getpid())
    p = Pool(10)
    for i in range(10):
        p.apply_async(parse)#异步异步异步，一定要异步，快
    p.close()
    p.join()

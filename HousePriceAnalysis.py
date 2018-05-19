#coding:utf8
import codecs
import requests
from bs4 import BeautifulSoup
import common.MySQLHelper
import common.CommonHelper
import time
import traceback
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
DOWNLOAD_URL = 'https://sz.zu.anjuke.com/'
def DownloadPage(url):	
	html = requests.get(url, headers={
		'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
	}).content
	return html

def ParseHtml(html):
	soup = BeautifulSoup(html,'html.parser')
	house_list_soup = soup.find('div', attrs={'class': 'list-content'}).find_all('div', attrs={'class':'zu-itemmod'})
	house_info_list = []
	for house_item in house_list_soup:
		rent_month = house_item.find('strong').getText()
		address_detail = house_item.find('address')
		#print(address_detail)
		address = address_detail.find('a')
		if address:
			address = address.getText().strip()
		else:
			address = address_detail.getText().strip()
		other_detail = house_item.find('p',attrs={'class':'details-item tag'}).getText().strip()		
		index = other_detail.index('层')
		house_type = other_detail[:index+1]
		house_info_list.append({'address':address, 'house_type':house_type, 'rent_month':rent_month})
	next_page = soup.find('a',attrs={'class':'aNxt'})
	if next_page:
		return house_info_list, next_page['href']
	else:
		return house_info_list, None
def GetHouseInfo(url):
	house_info_list = []
	index = 1
	while url:
		print(f'{index}:{url}')
		html = DownloadPage(url)
		house_page_list, url = ParseHtml(html)
		house_info_list.extend(house_page_list)
		index += 1
	return house_info_list
def Write2File(house_data):
	fp = codecs.open('data/house_info', 'wb', encoding='utf-8')
	for line in house_data:
		address = line['address']
		house_type = line['house_type']
		rent_month = line['rent_month']
		fp.write(f'{address} {house_type} {rent_month}\n');
def Main():
	url = DOWNLOAD_URL
	house_info_list = GetHouseInfo(url)
	Write2File(house_info_list)

'''
Update city_info table
'''
ANJUKE_CITY_INFO_URL = 'https://www.anjuke.com/sy-city.html'
CITY_INFO_TABLE = 'city_info'

def UpdateCityInfoFromAnjuke(DB, Logger, url = ANJUKE_CITY_INFO_URL):
	try:
		city_homepage_url_dict = GetAnjukeCityUrl(url)
		city_rent_url_dict = GetAnjukeCityZuUrl(city_homepage_url_dict)
	except Exception as err:
		trace = traceback.format_exc()
		msg = "[Exception] %s\n%s" % (str(err), trace);
		print(f'Error Get city_info!')
		Logger.error(msg)
		return
				
	for city, rent_url in city_rent_url_dict.items():
		fields = {}
		fields['city_name'] = city
		fields['anjuke_homepage_url'] = city_homepage_url_dict[city]
		fields['anjuke_rent_url'] = rent_url		
		sql_string = f"SELECT city_id FROM city_info where city_name = '{city}'"        
		if not DB.query(sql_string):
			Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
			continue
		rows = DB.fetch_all()		
		if len(rows) <= 0: 			
			if DB.insert(CITY_INFO_TABLE, fields):                
				Logger.info("insert table[%s] city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] ok!" % (CITY_INFO_TABLE, fields["city_name"], fields['anjuke_homepage_url'], fields['anjuke_rent_url']));            
			else:            
				Logger.error("insert table[%s] city_info city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] failed! error[%s]" \
				% (CITY_INFO_TABLE, fields["city_name"], fields['anjuke_homepage_url'], fields['anjuke_rent_url'], DB.get_last_error()));
				continue
		else:
			city_id = rows[0]['city_id']
			if DB.update(CITY_INFO_TABLE, fields, "city_id = '%s'" % city_id ):
				if DB.rowcount() == 1:
					Logger.info("update table[%s] successfully!" % CITY_INFO_TABLE);
			else:
				Logger.error("update table[%s] error! sqlerror[%s]" % (CITY_INFO_TABLE, DB.get_last_error()));
	pass 
	
def GetAnjukeCityUrl(url = ANJUKE_CITY_INFO_URL):
	html = DownloadPage(url)
	soup = BeautifulSoup(html,'html.parser')
	letter_list_soup = soup.find('ul')
	citys_url_dict = dict()
	for city_list_soup in letter_list_soup.find_all('div',attrs={'class':'city_list'}):
		for city_item_soup in city_list_soup.find_all('a'):
			city = city_item_soup.getText().strip()
			c_url = city_item_soup['href']
			citys_url_dict[city] = c_url
			
	return citys_url_dict

def GetAnjukeCityZuUrl(city_url_dict):
	city_zuurl_dict = dict()
	for city, c_url in city_url_dict.items():
		time.sleep(0.1)
		html = DownloadPage(c_url)		
		soup = BeautifulSoup(html,'html.parser')
		ul_soup=soup.find('ul', attrs={'class':'L_tabsnew'})
		if ul_soup is None:
			city_zuurl_dict[city] = ''
			print(f'{city}-{c_url}-null')
			continue
		list_soup = ul_soup.find_all('li')
		zu_soup= list_soup[3].find('a',attrs={'class': 'a_navnew'})
		if zu_soup is None:
			city_zuurl_dict[city] = ''			
			print(f'{city}-{c_url}-null')
			continue
		zu_url = zu_soup['href']		
		city_zuurl_dict[city] = zu_url
		print(f'{city}-{c_url}-{zu_url}')
	return city_zuurl_dict
	
'''
Update district_info table'
'''
DISTRICT_INFO_TABLE = 'district_info'
def UpdateDistrictInfoFromAnjuke(DB, Logger):
	sql_string = 'SELECT city_id,city_name,anjuke_rent_url FROM city_info'
	if not DB.query(sql_string):
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return False
	city_zuurl_list = DB.fetch_all()		
	for row in city_zuurl_list:
		print(row)
		city_id = row['city_id']
		zu_url = row['anjuke_rent_url']
		html = DownloadPage(zu_url)	
		soup = BeautifulSoup(html,'html.parser')
		list_soup = soup.find('div', attrs={'class':'sub-items sub-level1'})
		if list_soup is None:
			list_soup = soup.find_all('div', attrs={'class':'items'})[0]
		a_soup_list = list_soup.find_all('a')
		a_soup_list.remove(a_soup_list[0])
		for a_soup in a_soup_list:
			district_name = a_soup.getText().strip()
			fields = {
				'city_id':city_id,
				'district_name': district_name
			}
			print(fields)
			sql_string = f"SELECT district_id FROM district_info where district_name = '{district_name}'"        
			if not DB.query(sql_string):
				Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
				continue
			rows = DB.fetch_all()		
			if len(rows) <= 0: 			
				if DB.insert(DISTRICT_INFO_TABLE, fields):                
					Logger.info("insert table[%s] district_name[%s], district_name[%s] ok!" \
					% (DISTRICT_INFO_TABLE, fields["district_name"], fields['district_name']));            
				else:            
					Logger.error("insert table[%s] district_name[%s], district_name[%s] error!" \
					% (DISTRICT_INFO_TABLE, fields["district_name"], fields['district_name'])); 
					continue
			else:
				district_id = rows[0]['district_id']
				if DB.update(DISTRICT_INFO_TABLE, fields, "district_id = '%s'" % district_id ):
					if DB.rowcount() == 1:
						Logger.info("update table[%s] successfully!" % DISTRICT_INFO_TABLE);
				else:
					Logger.error("update table[%s] error! sqlerror[%s]" % (DISTRICT_INFO_TABLE, DB.get_last_error()));		
	pass
'''
Update rent housing information
'''


if __name__ == '__main__':
	Logger = common.CommonHelper.GetLogger('log/%s.log' % (time.strftime("%Y%m%d")));
	DB = common.MySQLHelper.MySQL('127.0.0.1', 'root','', 3306, 'house_info')
	DB.query('SET NAMES UTF8')	
	#UpdateCityInfoFromAnjuke(DB, Logger, ANJUKE_CITY_INFO_URL)
	UpdateDistrictInfoFromAnjuke(DB, Logger)
	
	
	
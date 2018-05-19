import codecs
import requests
from bs4 import BeautifulSoup
import common.MySQLHelper
import common.CommonHelper
import time

DOWNLOAD_URL = 'https://sz.zu.anjuke.com/'
def DownloadPage(url):	
	time.sleep(0.010)
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

def UpdateTable_city_info(DB, Logger, url = ANJUKE_CITY_INFO_URL):
	city_homepage_url_dict = GetAnjukeCityUrl(url)
	city_rent_url_dict = GetAnjukeCityZuUrl(city_homepage_url_dict)
	for city, homepage_url in city_homepage_url_dict:
		fields = {}
		fields['city_name'] = city
		fields['anjuke_homepage_url'] = homepage_url
		fields['anjuke_rent_url'] = city_rent_url_dict[city]
		if DB.insert(CITY_INFO_TABLE, fields):                
			Logger.info("Insert table[%s] city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] ok!" % (CITY_INFO_TABLE, fields["city_name"], homepage_url, fields['anjuke_rent_url']));            
		else:            
			Logger.error("Insert table[%s] city_info city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] failed! error[%s]" \
                           % (CITY_INFO_TABLE, fields["city_name"], homepage_url, fields['anjuke_rent_url'], DB.get_last_error()));
		
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
	
if __name__ == '__main__':
	Logger = common.CommonHelper.GetLogger('log/%s.log' % (time.strftime("%Y%m%d")));
	DB = common.MySQLHelper.MySQL(
		'127.0.0.1', 
		'root', 
		'', 
		3306, 
		'house_info'
	)
	UpdateTable_city_info(DB, Logger, ANJUKE_CITY_INFO_URL)
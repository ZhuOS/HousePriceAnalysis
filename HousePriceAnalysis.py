#coding=utf8
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
TESTING = True
CFG_PATH='.\conf\configure.ini'
ANJUKE_CITY_INFO_URL = 'https://www.anjuke.com/sy-city.html'
SHENZHEN_HOMEPAGE_URL='https://shenzhen.anjuke.com'
CITY_INFO_TABLE = 'city_info'
DISTRICT_INFO_TABLE = 'district_info'
COMMUNITY_INFO_TABLE = 'community_info'
RENT_INFO_TABLE = 'rent_info'
def DownloadPage(Logger, url):
	count = 0
	while count < 3:
		try:
			if count > 0:
				print('Try again ...')
			html = requests.get(url, headers={
				'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
			}).content				
			return html
		except Exception as err:
			count += 1
			trace = traceback.format_exc()
			msg = "[Exception] url[%s] %s\n%s" % (url, str(err), trace)
			print(f'Error: url[{url}] download page failed!\n')
			Logger.error(msg)
	return False
def StringSplit(str,char,type = 0):
	if type == 0:
		idx = str.find(char)
		if idx == -1:
			return str
		return str[:idx]	
	else:
		idx = str.rfind(char)
		if idx < 0 or idx >= len(str)-1:
			return None
		return str[idx+1:]
	pass
def Write2File(house_data):
	fp = codecs.open('data/house_info', 'wb', encoding='utf-8')
	for line in house_data:
		address = line['address']
		house_type = line['house_type']
		rent_month = line['rent_month']
		fp.write(f'{address} {house_type} {rent_month}\n');
'''
#Update city_info table
'''
def UpdateCityInfoFromAnjuke(DB, Logger, url = ANJUKE_CITY_INFO_URL):
	try:
		city_homepage_url_dict = GetAnjukeCityUrl(Logger, url)
		city_rent_url_dict = GetAnjukeCityZuUrl(Logger, city_homepage_url_dict)
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
				if TESTING:
					print(f'insert {CITY_INFO_TABLE} city_name[{city}] successfully')
				Logger.info("insert table[%s] city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] ok!" % (CITY_INFO_TABLE, fields["city_name"], fields['anjuke_homepage_url'], fields['anjuke_rent_url']));            
			else:
				if TESTING:
					print(f'insert {CITY_INFO_TABLE} city_name[{city}] failed')            
				Logger.error("insert table[%s] city_info city_name[%s], anjuke_homepage_url[%s] anjuke_rent_url[%s] failed! error[%s]" \
				% (CITY_INFO_TABLE, fields["city_name"], fields['anjuke_homepage_url'], fields['anjuke_rent_url'], DB.get_last_error()));
				continue
		else:
			city_id = rows[0]['city_id']
			if DB.update(CITY_INFO_TABLE, fields, f"city_id={city_id}"):				
				if DB.rowcount() == 1:
					Logger.info("update table[%s] successfully!" % CITY_INFO_TABLE);
				if TESTING:
					print(f'update {CITY_INFO_TABLE} city_name[{city}] successfully')            
				
			else:
				if TESTING:
					print(f'update {CITY_INFO_TABLE} city_name[{city}] failed')   
				Logger.error("update table[%s] error! sqlerror[%s]" % (CITY_INFO_TABLE, DB.get_last_error()));
	pass 
	
def GetAnjukeCityUrl(Logger, url = ANJUKE_CITY_INFO_URL):
	html = DownloadPage(Logger, url)
	if not html:
		return None
	soup = BeautifulSoup(html,'html.parser')
	letter_list_soup = soup.find('ul')
	citys_url_dict = dict()
	for letter_info_soup in letter_list_soup.find_all('li'):
		city_letter = letter_info_soup.find('label').getText().strip()
		if city_letter == '其他':
			continue
		city_list_soup = letter_info_soup.find('div', attrs={'class':'city_list'})
		for city_item_soup in city_list_soup.find_all('a'):
			city = city_item_soup.getText().strip()
			c_url = city_item_soup['href']
			citys_url_dict[city] = c_url
			if TESTING:
				print(f'{city}:{c_url}')
	return citys_url_dict

def GetAnjukeCityZuUrl(Logger, city_url_dict):
	city_zuurl_dict = dict()
	for city, c_url in city_url_dict.items():
		time.sleep(0.1)
		html = DownloadPage(Logger, c_url)		
		if nothtml:
			continue
		soup = BeautifulSoup(html,'html.parser')
		ul_soup=soup.find('ul', attrs={'class':'L_tabsnew'})
		zu_url = ''
		if ul_soup is None:
			city_zuurl_dict[city] = ''
			print(f'{city}|{c_url}|null')
			continue
		list_soup = ul_soup.find_all('li')
		
		for item_soup in list_soup:
			menu_soup = item_soup.find('a',attrs={'class':'a_navnew'})
			menu_name = menu_soup.getText().strip().replace(' ','')
			if menu_name == '租房':
				zu_url = menu_soup['href']
				break
		if zu_url == '':		
			print(f'{city}|{c_url}|null')
			continue
		city_zuurl_dict[city] = zu_url
		if TESTING:
			print(f'{city}|{c_url}|{zu_url}')
	return city_zuurl_dict
	
'''
#Update district_info table'
#Only for districts having rentint house
'''
'''
def UpdateDistrictInfoFromAnjuke(DB, Logger):
	sql_string = "SELECT city_id,city_name,anjuke_rent_url FROM city_info WHERE anjuke_rent_url!=''"
	if not DB.query(sql_string):
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return False
	city_zuurl_list = DB.fetch_all()		
	for row in city_zuurl_list:
		if TESTING:
			print(f"{row['city_name']}:")
		city_id = row['city_id']
		zu_url = row['anjuke_rent_url']
		html = DownloadPage(zu_url)	
		soup = BeautifulSoup(html,'html.parser')
		list_soup = soup.find('div', attrs={'class':'sub-items sub-level1'})	
		if list_soup is None:
			list_soup = soup.find('span', attrs={'class':'elems-l'})
			if list_soup is None:
				print(zu_url)
				continue
		a_soup_list = list_soup.find_all('a')
		a_soup_list.remove(a_soup_list[0])
		for a_soup in a_soup_list:
			district_name = a_soup.getText().strip()
			fields = {
				'city_id':city_id,
				'district_name': district_name
			}
			if TESTING:
				print(f'	{district_name}',)   
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
'''
#Update community information
'''
def GetAnjukeComInfo(DB, Logger):
	sql_string = f'SELECT city_id,city_name,anjuke_homepage_url FROM {CITY_INFO_TABLE}'
	if not DB.query(sql_string):
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return
	city_zuurl_list = DB.fetch_all()
	if len(city_zuurl_list) <=0:
		return
	for row in city_zuurl_list:
		city_id = row['city_id']
		city_name = row['city_name']
		if TESTING:
			print(f"{city_name}:")
		homepage_url = row['anjuke_homepage_url']
		info_list = GetCityAllComInfo(Logger, homepage_url)
		UpdateCityComInfo(DB, Logger, city_id, info_list)
		#break
	pass
# Update table community_info
def UpdateCityComInfo(DB, Logger, city_id, info_list):
	if not info_list:
		return None
	for fields in info_list:
		fields['city_id'] = city_id
		cm_name = fields['cm_name']		
		# update
		sql_string = f"SELECT cm_id FROM {COMMUNITY_INFO_TABLE} WHERE \
			cm_name='{fields['cm_name']}' AND city_id={fields['city_id']} AND district='{fields['district']}'"
		if not DB.query(sql_string):
			Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
			continue
		rows = DB.fetch_all()
		if len(rows) <= 0: 			
			if DB.insert(COMMUNITY_INFO_TABLE, fields):                
				Logger.info("insert table[%s] city_id[%s], district[%s] cm_name[%s] ok!" \
					% (COMMUNITY_INFO_TABLE, fields["city_id"], fields['district'], fields['cm_name']));            
				if TESTING:
					print(f'insert {COMMUNITY_INFO_TABLE} cm_name[{cm_name}] successfully')				
			else:            
				Logger.error("insert table[%s] city_id[%s], district[%s] cm_name[%s] failed! error[%s]" %\
					(COMMUNITY_INFO_TABLE, fields["city_id"], fields['district'], fields['cm_name'], DB.get_last_error()));
				if TESTING:
					print(f'insert {COMMUNITY_INFO_TABLE} cm_name[{cm_name}] failed')				
			continue
		cm_id = rows[0]['cm_id']
		if DB.update(COMMUNITY_INFO_TABLE, fields, f"cm_id={cm_id}"):
			if DB.rowcount() == 1:
				Logger.info("update table[%s] successfully!" % COMMUNITY_INFO_TABLE);
			if TESTING:
				print(f'update {COMMUNITY_INFO_TABLE} cm_name[{cm_name}] successfully')				
		else:
			Logger.error("update table[%s] error! sqlerror[%s]" \
				% (COMMUNITY_INFO_TABLE, DB.get_last_error()));
			if TESTING:
				print(f'update {COMMUNITY_INFO_TABLE} cm_name[{cm_name}] failed')				
	pass	
# Get all community informations
def GetCityAllComInfo(Logger, city_homepage_url):
	if not city_homepage_url:
		return None
	com_url = f'{city_homepage_url}/community'
	info_list = []
	#if TESTING:
	#	count = 0
	while com_url:
		if TESTING:
			print(com_url)
		com_info_list, com_url = GetCityPageComInfo(Logger, com_url)
		if com_info_list is not None:
			info_list.extend(com_info_list)
		#if TESTING:
		#	count += 1
		#	if count == 2:
		#		break
	return info_list
# Get current page information	
def GetCityPageComInfo(Logger, com_url):
	html = DownloadPage(Logger, com_url)
	if not html:
		return None, None
	soup = BeautifulSoup(html,'html.parser')
	list_soup = soup.find('div', attrs={'class':'list-content'})
	com_info_list = []
	for item_soup in list_soup.find_all('div', attrs={'class':'li-itemmod'}):
		#parse community informations
		com_info_url = item_soup['link'].strip()
		cmmid = StringSplit(com_info_url,"/",1)
		if cmmid == None:
			msg = f"Url[{com_url}],error[cann't find cmmid form {com_info_url}]!"
			Logger.error(msg)
			if TESTING:
				print(f'Error:{msg}')
			continue
		if TESTING:
			print(f"{cmmid}:",end='')
		aver_price_soup = item_soup.find('strong')
		aver_price = -1
		if aver_price_soup:
			aver_price_str = aver_price_soup.getText().strip()			
			if aver_price_str != '暂无数据' and aver_price_str a!= '暂无' and aver_price_str !='':
				aver_price = float(aver_price_str)
		else:
			msg = f"Url[{com_url}],error[cann't find aver_price form {com_info_url}]!"
			Logger.error(msg)
			if TESTING:
				print(f'Error:{msg}')
			
		
		com_infos = ParseComHtml(Logger, com_info_url)
		if com_infos is not None:
			com_infos['anjuke_cmmid'] = cmmid
			com_infos['aver_price'] = aver_price
			com_info_list.append(com_infos)
		
	next_page = soup.find('a',attrs={'class':'aNxt'})
	if next_page:
		return com_info_list, next_page['href']
	else:
		return com_info_list, None
# Parse current page information
def ParseComHtml(Logger, url):
	com_infos = {}
	try:
		html = DownloadPage(Logger, url)
		if not html:
			return None
		soup = BeautifulSoup(html,'html.parser')
		position_soup = soup.find('h1')
		com_infos['cm_name'] = position_soup.getText().strip().split()[0]
		posi_details = position_soup.find('span').getText().strip()
		posi_details = posi_details.split('-',3)
		com_infos['district'] = posi_details[0]
		com_infos['block'] = posi_details[1]
		com_infos['street'] = posi_details[2]
		detail_soup = soup.find('div', attrs={'class':'basic-infos-box'})
		
		params_soup = detail_soup.find('dl', attrs={'class':'basic-parms-mod'})
		params_list = params_soup.find_all('dd')
		if len(params_list) < 10:
			print(f'Error:{url}')
			return None
		property_type = params_list[0].getText().strip()
		if property_type != '暂无数据':
			com_infos['property_type'] = property_type
		pf_string = params_list[1].getText().strip()
		if pf_string != '暂无数据':
			com_infos['property_fee'] = float(StringSplit(pf_string, '元'))
		area_string = params_list[2].getText().strip()
		if area_string != '暂无数据':
			com_infos['area_total'] = int(StringSplit(area_string, 'm'))
		hn_string = params_list[3].getText().strip()
		if hn_string != '暂无数据':
			com_infos['house_num'] = int(StringSplit(hn_string, '户'))	
		year_string = params_list[4].getText().strip()
		if year_string != '暂无数据':
			com_infos['built_year'] = StringSplit(year_string, '年')
		parking_space_num = params_list[5].getText().strip()
		if parking_space_num != '暂无数据':
			com_infos['parking_space_num'] = parking_space_num
		plot_ratio = params_list[6].getText().strip()
		if plot_ratio != '暂无数据':
			com_infos['plot_ratio'] = float(plot_ratio)
		green_string = params_list[7].getText().strip()
		if green_string != '暂无数据':
			com_infos['greening_rate'] = float(StringSplit(green_string,'%')) / 100
		developer = params_list[8].getText().strip()
		if developer != '暂无数据':
			com_infos['developer'] = developer
		property_company = params_list[9].getText().strip()
		if property_company != '暂无数据':
			com_infos['property_company'] = property_company
		'''
		related_school = params_list[10].getText().strip()
		if related_school != '暂无数据':
			com_infos['related_school'] = related_school
		'''
	except Exception as err:
		trace = traceback.format_exc()
		msg = "[Exception] %s\n%s" % (str(err), trace);
		print(f'Error[{url}]:{msg}')
		Logger.error(msg)
		return None
	if TESTING:
		print(f"	{com_infos['cm_name']}")
	return com_infos
'''
#Update house-renting informations
'''
def GetAnjukeRentInfo(DB, Logger):
	try:
		sql_string = f'SELECT city_id,city_name,anjuke_rent_url FROM {CITY_INFO_TABLE}'
		if not DB.query(sql_string):
			Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
			return
		city_list = DB.fetch_all()
		for row in city_list:
			if row['anjuke_rent_url'] == '':
				continue		
			if TESTING:
				print(row['city_name'])
			GetRentHouseInfo(DB, Logger, row['city_name'])
	except Exception as err:
		trace = traceback.format_exc()
		msg = "[Exception] %s\n%s" % (str(err), trace)		
		Logger.error(msg)
		if TESTING:
			print(f'Error[Get anjuke rent information]:{msg}')
		return None
	pass	
def GetRentHouseInfo(DB, Logger, city_name):
	sql_string = f"SELECT city_id, anjuke_rent_url FROM {CITY_INFO_TABLE} WHERE city_name='{city_name}'"
	if not DB.query(sql_string):
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return None
	rows = DB.fetch_all()
	if len(rows) == 0:
		Logger.error("sql[%s], error[no data]", sql_string)
		return None
	city_id = rows[0]['city_id']
	rent_url = f"{rows[0]['anjuke_rent_url']}fangyuan/px7"
	#rent_url = 'https://cd.zu.anjuke.com/fangyuan/p4-px7/'
	house_info_list = []
	index = 1
	while rent_url:
		print(f'{index}:{rent_url}')		
		house_page_list, rent_url = ParseRentHtml(DB, Logger, city_id, rent_url)
		if house_page_list != None:
			house_info_list.extend(house_page_list)
		index += 1
	return house_info_list		
	pass
def ParseRentHtml(DB, Logger, city_id, rent_url):
	html = DownloadPage(Logger, rent_url)
	if not html:
		return None, None
	soup = BeautifulSoup(html,'html.parser')
	house_list_soup = soup.find('div', attrs={'class': 'list-content'})
	house_info_list = []
	for house_item in house_list_soup.find_all('div', attrs={'class':'zu-itemmod'}):
		rent_price = int(house_item.find('strong').getText())
		address_detail = house_item.find('address')
		# get community id
		cmmid_soup = address_detail.find('a')
		cmmid = ''
		if cmmid_soup:
			cmmid = StringSplit(cmmid_soup['href'].strip(), "/",1)
		community = ''
		district = ''
		block = ''
		street = ''
		address_str = address_detail.getText().strip()
		address_arr = address_str.split()		
		if len(address_arr) >= 1:
			community = address_arr[0]
		if len(address_arr) < 2 or len(address_arr[1].strip().split('-')) !=2:
			street = address_str
			msg = f'Error parse rent url[{rent_url}] address_arr[{address_arr}]'
			Logger.error(msg)
			print(msg)
		else:
			district_block = address_arr[1].strip().split('-')
			district = district_block[0]
			block = district_block[1]
		if len(address_arr) >= 3:
			street = address_arr[2].strip()		
		other_detail = house_item.find('p',attrs={'class':'details-item tag'}).getText().strip().split('\ue147')
		if len(other_detail) == 2:
			contact = other_detail[1].strip()
		else:
			contact = ''		
		other_parm = other_detail[0].split('|')
		if len(other_parm) != 3:
			room_num = -1
			hall_num = -1
			area = -1
			floor = 0
			msg = f'Error parse rent url[{rent_url}] other_parm[{other_parm}]'
			Logger.error(msg)
			print(msg)			
		else:
			room_num = int(StringSplit(other_parm[0],'室'))
			hall_num = int(StringSplit(other_parm[0].split('室')[1],'厅'))
			area = float(StringSplit(other_parm[1], '平'))
			floor = int(StringSplit(other_parm[2], '/'))
		rent_type = house_item.find('span', attrs={'class':'cls-1'}).getText()
		orientation = house_item.find('span', attrs={'class':'cls-2'}).getText()
		subway_soup = house_item.find('span', attrs={'class':'cls-3'})
		subway = ''
		if subway_soup:
			subway = subway_soup.getText()		
		remark_soup = house_item.find('h3').find('a')
		remark = remark_soup['title'].strip()
		house_url = remark_soup['href'].strip()
		house_id = StringSplit(house_url, "/",1)
		if TESTING:
			print(f"{house_id}:{community}")
		if house_id == None:
			msg = f"Url[{rent_url}],error[cann't find house_id form {house_url}]!"
			Logger.error(msg)
			if TESTING:
				print(f'Error:{msg}')
			continue
			
		fields = {}
		fields['city_id'] = city_id
		fields['h_rent'] = rent_price
		fields['cm_name'] = community
		fields['district'] = district
		fields['block'] = block
		fields['street'] = street
		fields['contact'] = contact
		fields['room_num'] = room_num
		fields['hall_num'] = hall_num
		fields['h_area'] = area
		fields['floor'] = floor
		fields['rent_type'] = rent_type
		fields['orientation'] = orientation
		fields['subway'] = subway
		fields['remark'] = remark
		fields['anjuke_hid'] = house_id
		fields['anjuke_cmmid'] = cmmid
		house_info_list.append(fields)		
		UpdateRentInfo(DB, Logger, fields)		
	next_page = soup.find('a',attrs={'class':'aNxt'})
	if next_page:
		return house_info_list, next_page['href']
	else:
		return house_info_list, None		
RENT_TYPE = {'整租':0, '合租':1}
ORIENTATION = {	'朝东':1,'朝西':2,'朝南':3,'朝北':4,\
				'东南':13,'西南':23,'东北':14,'西北':24,\
				'东西':12,'南北':34,'不知道朝向':-1}
def QueryCmidFromID(DB, Logger,rent_info):
	anjuke_cmmid = rent_info['anjuke_cmmid']
	if anjuke_cmmid == '':
		return None	
	sql_string = f"SELECT cm_id FROM {COMMUNITY_INFO_TABLE} WHERE anjuke_cmmid='{anjuke_cmmid}'"
	if not DB.query(sql_string):		
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return None
	rows = DB.fetch_all()
	if len(rows) > 0:
		return rows[0]['cm_id']
	
	cm_fields = {}
	cm_fields['anjuke_cmmid'] = anjuke_cmmid
	cm_fields['cm_name'] = rent_info['cm_name']
	cm_fields['city_id'] = rent_info['city_id']
	cm_fields['district'] = rent_info['district']
	cm_fields['block'] = rent_info['block']
	cm_fields['street'] = rent_info['street']
	cm_fields['subway'] = rent_info['subway']
	if DB.insert(COMMUNITY_INFO_TABLE, cm_fields):   
		msg = f"insert table[{COMMUNITY_INFO_TABLE}] city_id[{cm_fields['city_id']}] cm_name[{cm_fields['cm_name']}] ok!"
		if TESTING:
			print(msg)
		Logger.info(msg)           
	else:
		msg = f'Error: insert table[{COMMUNITY_INFO_TABLE}] cm_name[{cm_name}]  error[{DB.get_last_error}]'
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	sql_string = f"SELECT cm_id FROM {COMMUNITY_INFO_TABLE} WHERE anjuke_cmmid='{anjuke_cmmid}'"
	if not DB.query(sql_string):
		msg = f"Error: sql[{sql_string}], error[{DB.get_last_error()}]"
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	rows = DB.fetch_all()
	if len(rows) <= 0:
		msg = f"Error: sql[{sql_string}], error[return None]"
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	return rows[0]['cm_id']		
		
def QueryCmidFromName(DB, Logger, rent_info):
	cm_name = rent_info['cm_name']
	if cm_name == '' or cm_name == '暂无小区':
		return None
	sql_string = f"SELECT cm_id FROM {COMMUNITY_INFO_TABLE} WHERE \
		cm_name='{cm_name}' and city_id={rent_info['city_id']} and district='{rent_info['district']}'"
	if not DB.query(sql_string):		
		Logger.error("sql[%s], error[%s]", sql_string, DB.get_last_error())
		return None
	rows = DB.fetch_all()
	if len(rows) > 0:
		return rows[0]['cm_id']
	cm_fields = {}
	cm_fields['cm_name'] = cm_name
	cm_fields['city_id'] = rent_info['city_id']
	cm_fields['district'] = rent_info['district']
	cm_fields['block'] = rent_info['block']
	cm_fields['street'] = rent_info['street']
	cm_fields['subway'] = rent_info['subway']	
	if not DB.insert(COMMUNITY_INFO_TABLE, cm_fields):   
		msg = f'Error: insert table[{COMMUNITY_INFO_TABLE}] cm_name[{cm_name}]  error[{DB.get_last_error}]'
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	msg = f"insert table[{COMMUNITY_INFO_TABLE}] city_id[{cm_fields['city_id']}] cm_name[{cm_fields['cm_name']}] ok!"
	if TESTING:
		print(msg)
	Logger.info(msg)
		
	sql_string = f"SELECT cm_id FROM {COMMUNITY_INFO_TABLE} WHERE cm_name='{cm_name}'"
	if not DB.query(sql_string):
		msg = f"Error: sql[{sql_string}], error[{DB.get_last_error()}]"
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	rows = DB.fetch_all()
	if len(rows) <= 0:
		msg = f"Error: sql[{sql_string}], error[return None]"
		if TESTING:
			print(msg)
		Logger.error(msg)
		return None
	return rows[0]['cm_id']
	
def UpdateRentInfo(DB, Logger, rent_info):
	'''
	Insert data to database
	'''
	fields = {}
	cm_id = QueryCmidFromID(DB, Logger, rent_info)
	if cm_id == None:
		cm_id = QueryCmidFromName(DB, Logger, rent_info)
	if cm_id != None:
		fields['cm_id'] = cm_id
	fields['h_rent'] = rent_info['h_rent']
	fields['h_area'] = rent_info['h_area']
	fields['floor'] = rent_info['floor']
	fields['room_num'] = rent_info['room_num']
	fields['hall_num'] = rent_info['hall_num']
	fields['contact'] = rent_info['contact']
	if rent_info['rent_type'] in RENT_TYPE:
		fields['rent_type'] = RENT_TYPE[rent_info['rent_type']]
	else:
		fields['rent_type'] = -1
	if rent_info['orientation'] in ORIENTATION:
		fields['h_orientation'] = ORIENTATION[rent_info['orientation']]
	else:
		fields['h_orientation'] = -1
	fields['subway'] = rent_info['subway']
	fields['remark'] = rent_info['remark']
	fields['anjuke_hid'] = rent_info['anjuke_hid']
	sql_string = f"SELECT h_id FROM {RENT_INFO_TABLE} WHERE anjuke_hid='{rent_info['anjuke_hid']}'"
	if not DB.query(sql_string):
		msg = f"sql[{sql_string}, error[{DB.get_last_error()}]]"
		if TESTING:
			print(f"Error:{msg}")
		Logger.error(msg)
		return False
	rows = DB.fetch_all()
	if len(rows) <= 0: 	
		if DB.insert(RENT_INFO_TABLE, fields): 
			msg = f"Insert {RENT_INFO_TABLE} community[{rent_info['cm_name']}] remark[{rent_info['remark']}] ok!"
			Logger.info(msg);            
			if TESTING:
				print(msg)
			return True
		Logger.error("Insert table[%s] community[%s], remark[%s] failed! error[%s]" \
			% (RENT_INFO_TABLE, rent_info['cm_name'], rent_info['remark'], DB.get_last_error()));
		if TESTING:
			print(f"Insert {RENT_INFO_TABLE} community[{rent_info['cm_name']}] remark[{rent_info['remark']}] failed!")	
		return False
	h_id = rows[0]['h_id']
	if DB.update(RENT_INFO_TABLE, fields, f"h_id={h_id}"):
		msg = f"Update {RENT_INFO_TABLE} community[{rent_info['cm_name']}] remark[{rent_info['remark']}] ok!"
		if DB.rowcount() == 1:
			Logger.info(msg);
		if TESTING:
			print(msg)
		return True
	Logger.error("update table[%s] error! sqlerror[%s]" \
		% (RENT_INFO_TABLE, DB.get_last_error()));
	if TESTING:
		print(f"update {RENT_INFO_TABLE} community[{rent_info['cm_name']}] failed")
	return False

'''
1.更新中断继续更新功能
'''	
if __name__ == '__main__':
	cfgs = common.CommonHelper.ReadIniFile(CFG_PATH)
	Logger = common.CommonHelper.GetLogger('%s/%s.log' % (cfgs['log']['path'], time.strftime("%Y%m%d")));
	DB = common.MySQLHelper.MySQL('127.0.0.1', 'root','', 3306, 'house_info')
	DB.query('SET NAMES UTF8')
	
	print('Update City Informations')
	#UpdateCityInfoFromAnjuke(DB, Logger, ANJUKE_CITY_INFO_URL)
	#print('Update District Informations')
	#UpdateDistrictInfoFromAnjuke(DB, Logger)
	print('Update Community Informations')
	#GetAnjukeComInfo(DB, Logger)
	print('Update Anjuke Rent Informations for all cities')
	GetAnjukeRentInfo(DB, Logger)
	
	
	
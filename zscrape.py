from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
import glob
import tqdm
import html
import unicodedata
import re

cities = ['Sugar Land']
home_types = ['For Sale', 'Sold']
col_list = ['zpid']

home_list = []
file_list = []

for city in cities:
    for home_type in home_types:
        files = glob.glob(f'Homes\\{city}\\{home_type}\\*.html')
        file_list.extend(files)

# for f in tqdm.tqdm(file_list):
#     print(f)

html_file = file_list[0]

with open(html_file, encoding='utf8') as f:
    page = f.read()
zpid_idx = page.find('_zpid')
zpid = page[zpid_idx-8:zpid_idx]
current_home = { 'zpid' : zpid }
soup = bs(page, 'html.parser')

current_home['value'] = float(soup.find_all('span', class_='ds-value')[0].text.replace(',','').replace('$',''))

details = soup.find_all('h3', class_='ds-bed-bath-living-area-container')[0].find_all(class_='ds-bed-bath-living-area')
current_home['bed'] = float(details[0].text.replace(' bd',''))
current_home['bath'] = float(details[1].text.replace(' ba',''))
current_home['sqft'] = float(details[2].text.replace(' sqft','').replace(',',''))

address_card = soup.find_all('h1', class_='ds-address-container')[0]
address_text = unicodedata.normalize('NFKD', address_card.text).split(',')
current_home['address'] = address_text[0].strip()
current_home['city'] = address_text[1].strip()
state_zip = address_text[2].strip().split(' ')
current_home['state'] = state_zip[0].strip()
current_home['zip'] = state_zip[1].strip()

current_home['status'] = soup.find_all('span', class_='ds-status-details')[0].text

overview_stats = soup.find('ul', class_='ds-overview-stats').find_all('li')
current_home['time on zillow'] = float(overview_stats[0].text.replace('Time on Zillow','').replace(' days',''))
current_home['views'] = float(overview_stats[1].text.replace('Views','').replace(',',''))
current_home['saves'] = float(overview_stats[2].text.replace('Saves','').replace(',',''))

current_home['description'] = unicodedata.normalize('NFKD', soup.find_all('div', class_='Text-aiai24-0')[0].text)

home_facts = soup.find('ul', class_='ds-home-fact-list').find_all('li')
for fact in home_facts:
    facts = fact.text.split(':')
    stripped_fact = re.sub('[a-zA-Z$/,]+', '', facts[1]).strip()
    if stripped_fact == '':
        current_home[facts[0].lower()] = facts[1]
    else:
        current_home[facts[0].lower()] = float(stripped_fact)


pass

# TODO: add lat long from address (to display on map)
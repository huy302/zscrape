from bs4 import BeautifulSoup as bs
import numpy as np
import pandas as pd
import glob
import tqdm
import html
import unicodedata
import re
import time
from joblib import Parallel, delayed
import requests
import xmltodict

zwsid = 'X1-ZWz1hc66db5h57_39zrb'
# zwsid = 'X1-ZWz1horrl3bksr_6jziz'

def strip_content(input_str):
    input_str = unicodedata.normalize('NFKD', input_str)
    stripped_str = re.sub('[a-zA-Z$:/, ®]+', '', input_str).replace('\\','')
    if stripped_str == '':
        return input_str.strip()
    else:
        return float(stripped_str)

def scrape_file(html_file, home_type, region):
    if home_type == 'For Sale':
        try:
            return scrape_for_sale(html_file, home_type, region)
        except:
            return {}
    else:
        try:
            return scrape_sold(html_file, home_type, region)
        except:
            try:
                return scrape_for_rent(html_file, 'For Rent', region)
            except:
                return {}

def scrape_sold(html_file, home_type, region):
    with open(html_file, encoding='utf8') as f:
        page = f.read()
    zpid_idx = page.find('_zpid')
    zpid = page[zpid_idx-8:zpid_idx]
    current_home = { 'zpid' : zpid, 'home type' : home_type, 'region' : region }
    soup = bs(page, 'html.parser')

    current_home['value'] = strip_content(soup.find('div', class_='status').text)
    current_home['date sold'] = soup.find('div', class_='date-sold').text.replace('Sold on ', '')
    current_home['home value'] = strip_content(soup.find('div', class_='zestimate').text)

    details = soup.find('h3', class_='edit-facts-light').find_all('span')
    current_home['bed'] = strip_content(details[1].text)
    current_home['bath'] = strip_content(details[3].text)
    current_home['sqft'] = strip_content(details[5].text)

    address_cards = soup.find('h1', class_='zsg-h1').find_all('div')
    current_home['address full'] = unicodedata.normalize('NFKD', address_cards[0].text + ', ' + address_cards[1].text)
    current_home['address'] = address_cards[0].text.strip()
    city_state_zip = address_cards[1].text.split(', ')
    current_home['city'] = city_state_zip[0].strip()
    state_zip = city_state_zip[1].split(' ')
    current_home['state'] = state_zip[0].strip()
    current_home['zip'] = state_zip[1].strip()

    current_home['status'] = 'Sold'
    current_home['description'] = unicodedata.normalize('NFKD', soup.find('div', id='home-description-container').text)
    
    home_facts = soup.find('div', class_='home-facts-at-a-glance-section').find_all('div', class_='fact-group')
    for fact in home_facts:
        fact_label = strip_content(fact.find('div', class_='fact-label').text).lower()
        fact_value = strip_content(fact.find('div', class_='fact-value').text)
        current_home[fact_label] = fact_value

    # # extract Property details
    # property_details_card = soup.find_all('span', class_='Text-aiai24-0')
    # details_keys = ['Stories', 'Garage spaces', 'Attached garage', 'Lot size'
    #     'Home type', 'Year built', 'Major remodel year', 'Tax assessed value'
    #     'Annual tax amount', ]
    # for card in property_details_card:
    #     for key in details_keys:
    #         if key in card.text:
    #             current_home[key.lower()] = strip_content(card.text.replace(key, '').replace(':', ''))
    #     if 'Estimated sales range' in card.text:
    #         sales_range = card.text.replace('Estimated sales range', '').strip().split(' - ')
    #         current_home['zestimate low'] = strip_content(sales_range[0])
    #         current_home['zestimate high'] = strip_content(sales_range[1])

    # extract school ratings
    school_cards = soup.find_all('span', class_='gs-rating-number')
    current_home['school 1'] = strip_content(school_cards[0].text)
    current_home['school 2'] = strip_content(school_cards[1].text)
    current_home['school 3'] = strip_content(school_cards[2].text)

    # address place holder
    current_home['lat'] = None
    current_home['lon'] = None
    return current_home

def scrape_for_rent(html_file, home_type, region):
    with open(html_file, encoding='utf8') as f:
        page = f.read()
    zpid_idx = page.find('_zpid')
    zpid = page[zpid_idx-8:zpid_idx]
    current_home = { 'zpid' : zpid, 'home type' : home_type, 'region' : region }
    soup = bs(page, 'html.parser')

    current_home['rent value'] = float(soup.find('span', class_='ds-value').text.replace(',','').replace('$',''))

    details = soup.find('h3', class_='ds-bed-bath-living-area-container').find_all(class_='ds-bed-bath-living-area')
    current_home['bed'] = strip_content(details[0].text)
    current_home['bath'] = strip_content(details[1].text)
    current_home['sqft'] = strip_content(details[2].text)

    address_card = soup.find('h1', class_='ds-address-container')
    address_text = unicodedata.normalize('NFKD', address_card.text)
    address_text_splits = address_text.split(',')
    current_home['address full'] = address_text
    current_home['address'] = address_text_splits[0].strip()
    current_home['city'] = address_text_splits[1].strip()
    state_zip = address_text_splits[2].strip().split(' ')
    current_home['state'] = state_zip[0].strip()
    current_home['zip'] = state_zip[1].strip()

    current_home['status'] = soup.find('span', class_='ds-status-details').text

    overview_stats = soup.find('ul', class_='ds-overview-stats').find_all('li')
    current_home['time on zillow'] = strip_content(overview_stats[0].text)
    current_home['views'] = strip_content(overview_stats[1].text)

    current_home['description'] = unicodedata.normalize('NFKD', soup.find('div', class_='Text-aiai24-0').text)

    home_facts = soup.find('ul', class_='ds-home-fact-list').find_all('li')
    for fact in home_facts:
        facts = fact.text.split(':')
        current_home[facts[0].lower()] = strip_content(facts[1])

    # extract Property details
    property_details_card = soup.find_all('span', class_='Text-aiai24-0')
    details_keys = ['Stories', 'Garage spaces', 'Attached garage', 'Lot size'
        'Home type', 'Year built', 'Major remodel year', 'Tax assessed value'
        'Annual tax amount', ]
    for card in property_details_card:
        for key in details_keys:
            if key in card.text:
                current_home[key.lower()] = strip_content(card.text.replace(key, '').replace(':', ''))
        # if 'Estimated sales range' in card.text:
        #     sales_range = card.text.replace('Estimated sales range', '').strip().split(' - ')
        #     current_home['zestimate low'] = strip_content(sales_range[0])
        #     current_home['zestimate high'] = strip_content(sales_range[1])

    # # extract zestimate and rent zestimate
    # zestimate_texts = ['Home value', 'Rental value']
    # for text in zestimate_texts:
    #     value_card = None
    #     for card in soup.find_all('h4', class_='Text-aiai24-0'):
    #         if text in card.text:
    #             value_card = card
    #             break
    #     if value_card == None:
    #         print(f'Cannot find {text} - {address_text}')
    #     else:
    #         zestimate_card = value_card.parent.find('p', class_='Text-aiai24-0')
    #         if zestimate_card == None:
    #             print(f'Cannot find {text} - {address_text}')
    #         else:
    #             current_home[text.lower()] = strip_content(zestimate_card.text)

    # extract school ratings
    school_cards = soup.find('div', class_='ds-nearby-schools-list').find_all('span', class_='ds-schools-display-rating')
    current_home['school 1'] = strip_content(school_cards[0].text)
    current_home['school 2'] = strip_content(school_cards[1].text)
    current_home['school 3'] = strip_content(school_cards[2].text)

    # address place holder
    current_home['lat'] = None
    current_home['lon'] = None
    return current_home

def scrape_for_sale(html_file, home_type, region):
    with open(html_file, encoding='utf8') as f:
        page = f.read()
    zpid_idx = page.find('_zpid')
    zpid = page[zpid_idx-8:zpid_idx]
    current_home = { 'zpid' : zpid, 'home type' : home_type, 'region' : region }
    soup = bs(page, 'html.parser')

    current_home['value'] = float(soup.find('span', class_='ds-value').text.replace(',','').replace('$',''))

    details = soup.find('h3', class_='ds-bed-bath-living-area-container').find_all(class_='ds-bed-bath-living-area')
    current_home['bed'] = strip_content(details[0].text)
    current_home['bath'] = strip_content(details[1].text)
    current_home['sqft'] = strip_content(details[2].text)

    address_card = soup.find('h1', class_='ds-address-container')
    address_text = unicodedata.normalize('NFKD', address_card.text)
    address_text_splits = address_text.split(',')
    current_home['address full'] = address_text
    current_home['address'] = address_text_splits[0].strip()
    current_home['city'] = address_text_splits[1].strip()
    state_zip = address_text_splits[2].strip().split(' ')
    current_home['state'] = state_zip[0].strip()
    current_home['zip'] = state_zip[1].strip()

    current_home['status'] = soup.find('span', class_='ds-status-details').text

    overview_stats = soup.find('ul', class_='ds-overview-stats').find_all('li')
    current_home['time on zillow'] = strip_content(overview_stats[0].text)
    current_home['views'] = strip_content(overview_stats[1].text)
    current_home['saves'] = strip_content(overview_stats[2].text)

    current_home['description'] = unicodedata.normalize('NFKD', soup.find('div', class_='Text-aiai24-0').text)

    home_facts = soup.find('ul', class_='ds-home-fact-list').find_all('li')
    for fact in home_facts:
        facts = fact.text.split(':')
        current_home[facts[0].lower()] = strip_content(facts[1])

    # extract Property details
    property_details_card = soup.find_all('span', class_='Text-aiai24-0')
    details_keys = ['Stories', 'Garage spaces', 'Attached garage', 'Lot size'
        'Home type', 'Year built', 'Major remodel year', 'Tax assessed value'
        'Annual tax amount', ]
    for card in property_details_card:
        for key in details_keys:
            if key in card.text:
                current_home[key.lower()] = strip_content(card.text.replace(key, '').replace(':', ''))
        # if 'Estimated sales range' in card.text:
        #     sales_range = card.text.replace('Estimated sales range', '').strip().split(' - ')
        #     current_home['zestimate low'] = strip_content(sales_range[0])
        #     current_home['zestimate high'] = strip_content(sales_range[1])

    # # extract zestimate and rent zestimate
    # zestimate_texts = ['Home value', 'Rental value']
    # for text in zestimate_texts:
    #     value_card = None
    #     for card in soup.find_all('h4', class_='Text-aiai24-0'):
    #         if text in card.text:
    #             value_card = card
    #             break
    #     if value_card == None:
    #         print(f'Cannot find {text} - {address_text}')
    #     else:
    #         zestimate_card = value_card.parent.find('p', class_='Text-aiai24-0')
    #         if zestimate_card == None:
    #             print(f'Cannot find {text} - {address_text}')
    #         else:
    #             current_home[text.lower()] = strip_content(zestimate_card.text)

    current_home['monthly cost'] = strip_content(soup.find('h4', class_='Text-sc-1vuq29o-0').text)

    # extract school ratings
    school_cards = soup.find('div', class_='ds-nearby-schools-list').find_all('span', class_='ds-schools-display-rating')
    current_home['school 1'] = strip_content(school_cards[0].text)
    current_home['school 2'] = strip_content(school_cards[1].text)
    current_home['school 3'] = strip_content(school_cards[2].text)

    # address place holder
    current_home['lat'] = None
    current_home['lon'] = None
    return current_home

def add_zillow_api_data(current_home):
    if 'zpid' not in current_home.keys():
        return current_home
    
    target_url = ('http://www.zillow.com/webservice/GetSearchResults.htm?zws-id={0}&address={1}&citystatezip={2}%2C+{3}&rentzestimate=true').format(zwsid, current_home['address'].replace("#", "APT"), current_home['city'], current_home['state'])
    response_text = requests.get(target_url).text
    zillow_data = xmltodict.parse(response_text)
    result_data = zillow_data['SearchResults:searchresults']['response']['results']['result']
    if type(result_data) is list: # set result = first result if there are multiple returned results
        result_data = result_data[0]

    current_home['api lat'] = result_data['address']['latitude']
    current_home['api lon'] = result_data['address']['longitude']
    current_home['api link'] = result_data['links']['homedetails']
    if 'zestimate' in result_data.keys():
        if '#text' in result_data['zestimate']['amount'].keys():
            current_home['api zestimate'] = result_data['zestimate']['amount']['#text']
        if '#text' in result_data['zestimate']['valuationRange']['low'].keys():
            current_home['api zestimate low'] = result_data['zestimate']['valuationRange']['low']['#text']
        if '#text' in result_data['zestimate']['valuationRange']['high'].keys():
            current_home['api zestimate high'] = result_data['zestimate']['valuationRange']['high']['#text']
    if 'rentzestimate' in result_data.keys():
        if '#text' in result_data['rentzestimate']['amount'].keys():
            current_home['api rent zestimate'] = result_data['rentzestimate']['amount']['#text']
        if '#text' in result_data['rentzestimate']['valuationRange']['low'].keys():
            current_home['api rent zestimate low'] = result_data['rentzestimate']['valuationRange']['low']['#text']
        if '#text' in result_data['rentzestimate']['valuationRange']['high'].keys():
            current_home['api rent zestimate high'] = result_data['rentzestimate']['valuationRange']['high']['#text']

    # this part is so fragile, need over defending
    if 'localRealEstate' in result_data.keys() and result_data['localRealEstate'] != None:
        if 'region' in result_data['localRealEstate'].keys():
            if 'zindexValue' in result_data['localRealEstate']['region'].keys():
                current_home['api neighborhood index'] = strip_content(result_data['localRealEstate']['region']['zindexValue'])

    return current_home

if __name__ == '__main__':
    
    regions = ['Sugar Land', 'Med Center', 'Missouri City']
    home_types = ['For Sale', 'Sold']
    # home_types = ['Sold']
    col_list = ['zpid']

    file_list = []

    for region in regions:
        for home_type in home_types:
            files = glob.glob(f'Homes\\{region}\\{home_type}\\*.html')
            file_tuples = ((f, home_type, region) for f in files)
            file_list.extend(file_tuples)

    # debug code
    # home_list = [scrape_file(file_list[43][0], file_list[43][1])]

    # parallel code
    home_list = Parallel(n_jobs=-1)(delayed(scrape_file)(html_file, home_type, region) for (html_file, home_type, region) in tqdm.tqdm(file_list))
    # add zillow api data, cannot be execute in parallel due to rate limiting
    print("===== Adding data from zillow api =====")
    for i in tqdm.tqdm(range(len(home_list))):
        time.sleep(0.5)
        try:
            home_list[i] = add_zillow_api_data(home_list[i])
        except: # ignore error in pulling data from api, if any
            continue
    # home_list = [add_zillow_api_data(home) for home in tqdm.tqdm(home_list)]

    df = pd.DataFrame(home_list)
    df.dropna(subset=['zpid'], inplace=True)
    print(f'Successfully extracted {df.shape[0]}/{len(home_list)}')
    df = df.sort_values(by=['zpid'])
    df.to_csv('data.csv', index=False)
    print(df)

    pass
    # TODO: 
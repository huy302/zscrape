from splinter import Browser
from splinter.exceptions import ElementDoesNotExist
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random

def init_browser():
    executable_path = {'executable_path': 'chromedriver.exe'}
    return Browser('chrome', **executable_path, headless=False)

def scrape_info():
    browser = init_browser()

    data = []

    for i in range(1, 13):
        url_1 = url_2 = ''
        if i != 1:
            url_1 = f'{i}_p'
            url_2 = f'%22currentPage%22:{i}'

        url = r"https://www.zillow.com/houston-tx/" + url_1 + r"?searchQueryState={%22pagination%22:{" + url_2 + r"},%22mapBounds%22:{%22west%22:-95.36660958368361,%22east%22:-95.2069645031172,%22south%22:29.806609167549286,%22north%22:29.932989374839877},%22regionSelection%22:[{%22regionId%22:39051,%22regionType%22:6}],%22isMapVisible%22:true,%22mapZoom%22:13,%22filterState%22:{},%22isListVisible%22:true}"
        browser.visit(url)

        # random sleep to avoid detection by zillow
        time.sleep(random.randrange(15, 25)) 

        # Scrape page into Soup
        html = browser.html
        soup = bs(html, "html.parser")
        all_cards = soup.find_all('article', class_="list-card")
        for card in all_cards:
            try:
                id = card['id']
                link = card.find('a', class_="list-card-link")['href']
                price = float(card.find('div', class_="list-card-price").text.replace(",","").replace("$",""))
                details = card.find('ul', class_="list-card-details").find_all('li')
                bed = float(details[0].text.replace(" bds",""))
                bath = float(details[1].text.replace(" ba",""))
                sqft = float(details[2].text.replace(" sqft","").replace(",",""))
                print(f'{id}, {link}, {price}, {bed}, {bath}, {sqft}')
                data.append([id, link, price, bed, bath, sqft])
            except Exception as e:
                print(e)

    browser.quit()

    return data


# ================== main code ==================
if __name__ == "__main__":
    data = scrape_info()
    df = pd.DataFrame(data, columns =['code', 'link', 'price', 'bed', 'bath', 'sqft'])
    df.to_csv("zillow.csv")
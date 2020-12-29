import requests
from lxml import html
import random

def make_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{:02d}.0.{:04d}.{} Safari/537.36'.format(
            random.randint(63, 84), random.randint(0, 9999), random.randint(98, 132)),
    }
    return headers


class MainScraper():
    def __init__(self):
        self.start_url = "https://www.clinicaltrialsregister.eu/ctr-search/search?query=&phase=phase-one&phase=phase-two&phase=phase-three"

    def start_scraping(self):
        for i in range(1506):
            self.get_details(page=i+1)

    def get_details(self, page):
        if  page >= 2:
            url = self.start_url + "&page={}".format(page)
        else:
            url = self.start_url

        r = requests.get(url)

if __name__ == '__main__':
    app = MainScraper()
    app.start_scraping()

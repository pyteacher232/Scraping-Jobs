import requests
from lxml import html
import random
import csv
import os
import time

def make_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{:02d}.0.{:04d}.{} Safari/537.36'.format(
            random.randint(63, 84), random.randint(0, 9999), random.randint(98, 132)),
    }
    return headers


class MainScraper():
    def __init__(self):
        self.start_url = "https://www.clinicaltrials.gov/ct2/results/download_fields?down_count=10000&down_flds=all&down_fmt=xml&recrs=abdefm&phase=0124&fund=2&strd_s=01%2F01%2F2019&flds=a&flds=b&flds=y"

        self.total_cnt = 0
        self.sess = requests.Session()

    def start_scraping(self):
        self.get_details(self.start_url)

    def get_details(self, url):
        print(f"[get_details] url: {url}")

        r = self.sess.get(url, headers=make_headers(), verify=False)
        tree = html.fromstring(r.text)
        rows = tree.xpath('//table[@class="result"]')
        for i, row in enumerate(rows):
            try:
                EudraCT_num = "".join(row.xpath('.//span[text()="EudraCT Number:"]/../text()')).strip()
            except:
                EudraCT_num = ""
            try:
                start_date = "".join(row.xpath('.//span[text()="Start Date"]/../text()')).strip()
            except:
                start_date = ""
            try:
                sponsor_name = "".join(row.xpath('.//span[text()="Sponsor Name:"]/../text()')).strip()
            except:
                sponsor_name = ""
            result_row = [
                EudraCT_num, start_date, sponsor_name
            ]
            self.total_cnt += 1
            self.insert_row(result_row)
            print(f"\t[Result {self.total_cnt}] {result_row}")


if __name__ == '__main__':
    app = MainScraper()
    app.start_scraping()

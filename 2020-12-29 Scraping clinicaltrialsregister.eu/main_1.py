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
        self.start_url = "https://www.clinicaltrialsregister.eu/ctr-search/search?query=&phase=phase-one&phase=phase-two&phase=phase-three"

        self.result_fname = os.path.join("Result.csv")
        self.create_result_file()

        heading = [
            "EudraCT_num", "start_date", "sponsor_name"
        ]
        if os.path.getsize(self.result_fname) == 0:
            self.insert_row(result_row=heading)

        self.total_cnt = 0
        self.sess = requests.Session()

    def start_scraping(self):
        for i in range(1506):
            while 1:
                try:
                    self.get_details(page=i + 1)
                    break
                except:
                    time.sleep(2)

    def get_details(self, page):
        print(f"[get_details] page: {page}")

        if page >= 2:
            url = self.start_url + "&page={}".format(page)
        else:
            url = self.start_url

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

    def create_result_file(self):
        self.result_fp = open(self.result_fname, 'w', encoding='utf-8', newline='')
        self.result_writer = csv.writer(self.result_fp)

    def insert_row(self, result_row):
        result_row = [str(elm) for elm in result_row]
        self.result_writer.writerow(result_row)
        self.result_fp.flush()


if __name__ == '__main__':
    app = MainScraper()
    app.start_scraping()

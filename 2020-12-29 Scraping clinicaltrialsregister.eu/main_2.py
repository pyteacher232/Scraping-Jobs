import requests
from lxml import html
import random
import csv
import os
import time
import xml.etree.ElementTree as ET

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

        self.result_fname = os.path.join("Result.csv")
        self.create_result_file()

        heading = [
            "nct_id", "sponsor", "collaborator", "start_date"
        ]
        if os.path.getsize(self.result_fname) == 0:
            self.insert_row(result_row=heading)

        self.total_cnt = 0
        self.sess = requests.Session()

    def start_scraping(self):
        self.get_details(xml_fname="SearchResults.xml")

    def get_details(self, xml_fname):
        print(f"[get_details] xml_fname: {xml_fname}")

        root = ET.parse(xml_fname).getroot()

        for study in root.findall('study'):
            nct_id = study.find('nct_id').text.strip()
            start_date = study.find('start_date').text.strip()
            sponsors = [e.text.strip() for e in study.findall('sponsors/lead_sponsor')]
            collaborators = [e.text.strip() for e in study.findall('sponsors/collaborator')]

            for sponsor in sponsors:
                for collaborator in collaborators:
                    result_row = [
                        nct_id, sponsor, collaborator, start_date
                    ]
                    self.total_cnt += 1
                    self.insert_row(result_row)
                    print(f"[Result {self.total_cnt}] {result_row}")


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

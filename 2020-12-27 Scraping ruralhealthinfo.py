import random
import os
import csv
import scrapy
from scrapy.http import FormRequest
from scrapy import signals
import xlrd
from lxml import html


def make_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{:02d}.0.{:04d}.{} Safari/537.36'.format(
            random.randint(63, 84), random.randint(0, 9999), random.randint(98, 132)),
    }
    return headers


timeout = 100
conn_limit = 300

proxy_file_name = 'proxy_http_ip.txt'
PROXIES = []
with open(proxy_file_name, 'rb') as text:
    PROXIES = ["http://" + x.decode("utf-8").strip() for x in text.readlines()]


class MainScraper(scrapy.Spider):
    name = "arrt_scrapy"
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': conn_limit,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': conn_limit,
        # 'AUTOTHROTTLE_START_DELAY ': 1,
        # 'AUTOTHROTTLE_MAX_DELAY ': 360,
        'AUTOTHROTTLE_DEBUG': True,
        # 'DOWNLOAD_DELAY': 1,
        # 'dont_filter': True,
        'RETRY_ENABLED': False,
        # 'COOKIES_ENABLED ': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': conn_limit,
        'CONCURRENT_REQUESTS_PER_IP': conn_limit,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_IGNORE_HTTP_CODES': [301, 302, 403, 404, 429, 500, 502, 503],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage',
        'HTTPCACHE_POLICY': 'scrapy.extensions.httpcache.DummyPolicy',
        # 'LOG_ENABLED': False,
        'DOWNLOAD_TIMEOUT': timeout,
        'URLLENGTH_LIMIT': 99999,
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MainScraper, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self):
        self.result_dir = os.path.join(os.getcwd(), "Result")
        if not os.path.exists(self.result_dir):
            os.makedirs(self.result_dir)

        self.result_fname = os.path.join(self.result_dir, "Result.csv")
        self.create_result_file()

        heading = [
            "id", "address", "geocoded_address", "latitude", "longitude", "", "census_tract", "county", "rhc", "forhp",
            "c2010_pct_rural", "cbsa_type", "cbsa_name", "cbsa_id", "ruca_code", "rucc_code", "uic", "ua",
            "shortage_primary_care", "shortage_dental_care", "shortage_mental_health", "mua", "mup", "mua_ge", "mup_ge",
            "web_link"
        ]
        if os.path.getsize(self.result_fname) == 0:
            self.insert_row(result_row=heading)

        self.total_cnt = 0
        self.total_result = []
        self.total_links = []

        self.input_dt = []
        input_fname = 'address_list.xlsx'
        input_xls = xlrd.open_workbook(input_fname)
        sheet = input_xls.sheet_by_index(0)
        for row_index in range(0, sheet.nrows):
            row = [sheet.cell(row_index, col_index).value for col_index in range(sheet.ncols)]
            self.input_dt.append(row)

        self.main_url = "https://www.ruralhealthinfo.org/am-i-rural/report?lat={}&lng={}"
        self.shortage_details_url = "https://www.ruralhealthinfo.org/am-i-rural/report/shortage-designations?lat={}&lng={}"

    def start_requests(self):
        for i, row in enumerate(self.input_dt):
            if i == 0:
                continue
            lat, lng = row[3], row[4]
            url = self.main_url.format(lat, lng)
            pxy = random.choice(PROXIES)
            request = FormRequest(
                url=url,
                method='GET',
                headers=make_headers(),
                callback=self.get_main_info,
                errback=self.fail_main_info,
                dont_filter=True,
                meta={
                    'url': url,
                    'row': row,
                    'proxy': pxy
                    # 'handle_httpstatus_all': True,
                    # 'dont_redirect': True,
                }
            )
            yield request

    def get_main_info(self, response):
        url = response.meta['url']
        row = response.meta['row']

        try:
            census_tract = "".join(response.xpath('//strong[text()="Census Tract:"]/../text()').extract()).strip()
        except:
            census_tract = ""
        try:
            county = "".join(response.xpath('//strong[text()="County:"]/../text()').extract()).strip()
        except:
            county = ""
        try:
            rhc = response.xpath(
                '//table//strong[contains(text(), "CMS - Rural Health Clinics (RHC) Program")]/../following-sibling::td[1]/text()').extract_first().strip()
        except:
            rhc = ""
        try:
            forhp = response.xpath(
                '//table//strong[contains(text(), "FORHP - Grant Programs")]/../following-sibling::td[1]/text()').extract_first().strip()
        except:
            forhp = ""
        try:
            c2010_pct_rural = response.xpath(
                '//table//strong[contains(text(), "Census 2010, Percent Rural")]/../following-sibling::td/ul/li/text()').extract_first().split(
                ":")[-1].strip()
        except:
            c2010_pct_rural = ""
        try:
            cbsa_type = response.xpath('//li[contains(text(), "CBSA Type:")]/text()').extract_first().split(":")[
                -1].strip()
        except:
            cbsa_type = ""
        try:
            cbsa_name = response.xpath('//li[contains(text(), "CBSA Name:")]/text()').extract_first().split(":")[
                -1].strip()
        except:
            cbsa_name = ""
        try:
            cbsa_id = response.xpath('//li[contains(text(), "CBSA ID:")]/text()').extract_first().split(":")[-1].strip()
        except:
            cbsa_id = ""
        try:
            ruca_code = response.xpath('//li[contains(text(), "RUCA Code:")]/text()').extract_first().split(":")[
                -1].strip()
        except:
            ruca_code = ""
        try:
            rucc_code = response.xpath('//li[contains(text(), "RUCC Code:")]/text()').extract_first().split(":")[
                -1].strip()
        except:
            rucc_code = ""
        try:
            uic = response.xpath('//li[contains(text(), "Urban Influence Code:")]/text()').extract_first().split(":")[
                -1].strip()
        except:
            uic = ""
        try:
            ua = response.xpath('//li[contains(text(), "UA/UC Number:")]/text()').extract_first().split(":")[-1].strip()
        except:
            ua = ""

        result_row = [
            census_tract, county, rhc, forhp, c2010_pct_rural, cbsa_type, cbsa_name, cbsa_id, ruca_code,
            rucc_code, uic, ua
        ]

        lat, lng = row[3], row[4]
        url = self.shortage_details_url.format(lat, lng)
        pxy = random.choice(PROXIES)
        request = FormRequest(
            url=url,
            method='GET',
            headers=make_headers(),
            callback=self.get_shortage_info,
            errback=self.fail_shortage_info,
            dont_filter=True,
            meta={
                'url': url,
                'row': row,
                'result_row': result_row,
                'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def fail_main_info(self, failure):
        pxy = random.choice(PROXIES)
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_main_info,
            errback=self.fail_main_info,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                'row': failure.request.meta['row'],
                'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_shortage_info(self, response):
        url = response.meta['url']
        row = response.meta['row']
        result_row = response.meta['result_row']

        tree = html.fromstring(response.text)
        try:
            shortage_primary_care = response.xpath(
                '//strong[text()= "Health Professional Shortage Areas"]/../../../..//td[contains(text(), "Primary Care")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            shortage_primary_care = ""
        try:
            shortage_dental_care = response.xpath(
                '//strong[text()= "Health Professional Shortage Areas"]/../../../..//td[contains(text(), "Dental Care")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            shortage_dental_care = ""
        try:
            shortage_mental_health = response.xpath(
                '//strong[text()= "Health Professional Shortage Areas"]/../../../..//td[contains(text(), "Mental Health")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            shortage_mental_health = ""
        try:
            mua = response.xpath(
                '//strong[text()= "Medically Underserved Areas/Populations"]/../../../..//td[contains(text(), "Medically Underserved Area (MUA)")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            mua = ""
        try:
            mup = response.xpath(
                '//strong[text()= "Medically Underserved Areas/Populations"]/../../../..//td[contains(text(), "Medically Underserved Population (MUP)")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            mup = ""
        try:
            mua_ge = response.xpath(
                '//strong[text()= "Medically Underserved Areas/Populations"]/../../../..//td[contains(text(), "Medically Underserved Area - Governor\'s Exception (MUA-GE)")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            mua_ge = ""
        try:
            mup_ge = response.xpath(
                '//strong[text()= "Medically Underserved Areas/Populations"]/../../../..//td[contains(text(), "Medically Underserved Population - Governor\'s Exception (MUP-GE)")]/following-sibling::td[1]/text()').extract_first().strip()
        except:
            mup_ge = ""

        lat, lng = row[3], row[4]
        web_link = self.shortage_details_url.format(lat, lng)

        final_result = row[:6] + result_row + [shortage_primary_care, shortage_dental_care, shortage_mental_health, mua,
                                               mup, mua_ge, mup_ge, web_link]

        self.total_cnt += 1
        self.insert_row(final_result)
        print(f"[Result {self.total_cnt}] {final_result}")


    def fail_shortage_info(self, failure):
        pxy = random.choice(PROXIES)
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_shortage_info,
            errback=self.fail_shortage_info,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                'row': failure.request.meta['row'],
                'result_row': failure.request.meta['result_row'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def create_result_file(self):
        self.result_fp = open(self.result_fname, 'w', encoding='utf-8', newline='')
        self.result_writer = csv.writer(self.result_fp)

    def insert_row(self, result_row):
        result_row = [str(elm) for elm in result_row]
        self.result_writer.writerow(result_row)
        self.result_fp.flush()

    def spider_closed(self, spider):
        pass


if __name__ == '__main__':
    from scrapy.utils.project import get_project_settings
    from scrapy.crawler import CrawlerProcess

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(MainScraper)
    process.start()

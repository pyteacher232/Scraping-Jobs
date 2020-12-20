import random
import os
import csv
import scrapy
from scrapy.http import FormRequest
from scrapy import signals
from lxml import html
import time
import itertools


def make_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{:02d}.0.{:04d}.{} Safari/537.36'.format(
            random.randint(63, 84), random.randint(0, 9999), random.randint(98, 132)),
    }
    return headers


timeout = 100
conn_limit = 200


class MainScraper(scrapy.Spider):
    name = "arrt_scrapy"
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': conn_limit,
        # 'AUTOTHROTTLE_ENABLED': True,
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
        # 'HTTPCACHE_ENABLED': True,
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
            "Jaar", "Kwartaal", "Regio", "Beroepsgroep", "Spanning", "Typering"
        ]
        if os.path.getsize(self.result_fname) == 0:
            self.insert_row(result_row=heading)

        self.total_cnt = 0
        self.total_result = []
        self.total_links = []

        self.start_url = "https://www.arbeidsmarktcijfers.nl/Report/2"

    def start_requests(self):
        request = FormRequest(
            url=self.start_url,
            method='GET',
            headers=make_headers(),
            callback=self.get_first_page,
            errback=self.fail_first_page,
            dont_filter=True,
            meta={
                'url': self.start_url,
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_first_page(self, response):
        __VIEWSTATE = response.xpath('//input[@name="__VIEWSTATE"]/@value').extract_first()
        __VIEWSTATEGENERATOR = response.xpath('//input[@name="__VIEWSTATEGENERATOR"]/@value').extract_first()
        __EVENTVALIDATION = response.xpath('//input[@name="__EVENTVALIDATION"]/@value').extract_first()

        Jaar_list = list([str(e) for e in range(2020, 2016, -1)])
        Kwartaal_list = response.xpath(
            '//div[@id="ctl00_ContentPlaceHolder1_ReportViewer1_ctl09_ctl05"]/select/option/@value').extract()[1:]
        Arbeidsmarktregio_list = [(str(i - 1), " ".join(row.xpath('.//label/text()').extract_first().strip().split()))
                                  for
                                  i, row in enumerate(response.xpath(
                '//div[@id="ctl00_ContentPlaceHolder1_ReportViewer1_ctl09_ctl07_divDropDown"]/table/tr/td[1]'))][2:]

        self.search_list = list(itertools.product(Jaar_list, Kwartaal_list, Arbeidsmarktregio_list))

        for Jaar, Kwartaal, (HiddenIndices, Regio) in self.search_list:
            formdata = {
                "ctl00$ContentPlaceHolder1$ctl00": "ctl00$ContentPlaceHolder1$ctl00|ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl05$ddValue",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl00": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl01": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$isReportViewerInVs": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl15": "ltr",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl16": "standards",
                "ctl00$ContentPlaceHolder1$ReportViewer1$AsyncWait$HiddenCancelField": "False",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl03$ddValue": Jaar,
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl05$ddValue": Kwartaal,
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$txtValue": "Totaal",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$txtValue": "Totaal",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$divDropDown$ctl01$HiddenIndices": "0",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$divDropDown$ctl01$HiddenIndices": "0",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$store": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$collapse": "false",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl10$ctl00$CurrentPage": "1",
                "null": "100",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl10$ctl03$ctl00": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl13$ClientClickedId": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$store": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$collapse": "false",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$VisibilityState$ctl00": "ReportPage",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ScrollPosition": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl02": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl03": "",
                "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl04": "100",
                "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl05$ddValue",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "__ASYNCPOST": "true",
            }

            headers = make_headers()
            request = FormRequest(
                url=self.start_url,
                method='POST',
                formdata=formdata,
                headers=headers,
                callback=self.get_Kwartaal_tokens,
                errback=self.fail_Kwartaal_tokens,
                dont_filter=True,
                meta={
                    'url': self.start_url,
                    'Jaar': Jaar,
                    'Kwartaal': Kwartaal,
                    'Regio': Regio,
                    'HiddenIndices': HiddenIndices,
                    '__VIEWSTATE': __VIEWSTATE,
                    '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                    '__EVENTVALIDATION': __EVENTVALIDATION,
                    # 'proxy': pxy
                    # 'handle_httpstatus_all': True,
                    # 'dont_redirect': True,
                }
            )
            yield request

    def fail_first_page(self, failure):
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_first_page,
            errback=self.fail_first_page,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_Kwartaal_tokens(self, response):
        Jaar = response.meta['Jaar']
        Kwartaal = response.meta['Kwartaal']
        Regio = response.meta['Regio']
        HiddenIndices = response.meta['HiddenIndices']
        __VIEWSTATE = response.meta['__VIEWSTATE']
        __VIEWSTATEGENERATOR = response.meta['__VIEWSTATEGENERATOR']
        __EVENTVALIDATION = response.meta['__EVENTVALIDATION']

        split_info = response.text.split('|')
        try:
            __VIEWSTATE = split_info[split_info.index('__VIEWSTATE') + 1]
            __VIEWSTATEGENERATOR = split_info[split_info.index('__VIEWSTATEGENERATOR') + 1]
            __EVENTVALIDATION = split_info[split_info.index('__EVENTVALIDATION') + 1]
        except Exception as e:
            # print([Jaar, Kwartaal, HiddenIndices, Regio])
            return

        formdata = {
            "ctl00$ContentPlaceHolder1$ctl00": "ctl00$ContentPlaceHolder1$ctl00|ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl00": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl01": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$isReportViewerInVs": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl15": "ltr",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl16": "standards",
            "ctl00$ContentPlaceHolder1$ReportViewer1$AsyncWait$HiddenCancelField": "False",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl03$ddValue": Jaar,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl05$ddValue": Kwartaal,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$txtValue": Regio,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$txtValue": "Totaal",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$divDropDown$ctl01$HiddenIndices": HiddenIndices,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$divDropDown$ctl01$HiddenIndices": "0",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$store": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$collapse": "false",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl13$ClientClickedId": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$store": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$collapse": "false",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$VisibilityState$ctl00": "None",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ScrollPosition": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl02": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl03": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl04": "100",
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": __EVENTVALIDATION,
            "__ASYNCPOST": "true",
        }

        headers = make_headers()
        request = FormRequest(
            url=self.start_url,
            method='POST',
            formdata=formdata,
            headers=headers,
            callback=self.get_Arbeidsmarktregio_tokens,
            errback=self.fail_Arbeidsmarktregio_tokens,
            dont_filter=True,
            meta={
                'url': self.start_url,
                'Jaar': Jaar,
                'Kwartaal': Kwartaal,
                'Regio': Regio,
                'HiddenIndices': HiddenIndices,
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def fail_Kwartaal_tokens(self, failure):
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_Kwartaal_tokens,
            errback=self.fail_Kwartaal_tokens,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                'Jaar': failure.request.meta['Jaar'],
                'Kwartaal': failure.request.meta['Kwartaal'],
                'Regio': failure.request.meta['Regio'],
                'HiddenIndices': failure.request.meta['HiddenIndices'],
                '__VIEWSTATE': failure.request.meta['__VIEWSTATE'],
                '__VIEWSTATEGENERATOR': failure.request.meta['__VIEWSTATEGENERATOR'],
                '__EVENTVALIDATION': failure.request.meta['__EVENTVALIDATION'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_Arbeidsmarktregio_tokens(self, response):
        Jaar = response.meta['Jaar']
        Kwartaal = response.meta['Kwartaal']
        Regio = response.meta['Regio']
        HiddenIndices = response.meta['HiddenIndices']
        __VIEWSTATE = response.meta['__VIEWSTATE']
        __VIEWSTATEGENERATOR = response.meta['__VIEWSTATEGENERATOR']
        __EVENTVALIDATION = response.meta['__EVENTVALIDATION']

        split_info = response.text.split('|')
        try:
            __VIEWSTATE = split_info[split_info.index('__VIEWSTATE') + 1]
            __VIEWSTATEGENERATOR = split_info[split_info.index('__VIEWSTATEGENERATOR') + 1]
            __EVENTVALIDATION = split_info[split_info.index('__EVENTVALIDATION') + 1]
        except Exception as e:
            # print([Jaar, Kwartaal, HiddenIndices, Regio])
            return

        formdata = {
            "ctl00$ContentPlaceHolder1$ctl00": "ctl00$ContentPlaceHolder1$ctl00|ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$Reserved_AsyncLoadTarget",
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$Reserved_AsyncLoadTarget",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__EVENTVALIDATION": __EVENTVALIDATION,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl00": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl03$ctl01": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$isReportViewerInVs": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl15": "ltr",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl16": "standards",
            "ctl00$ContentPlaceHolder1$ReportViewer1$AsyncWait$HiddenCancelField": "False",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl03$ddValue": Jaar,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl05$ddValue": Kwartaal,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$txtValue": Regio,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$txtValue": "Totaal",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl07$divDropDown$ctl01$HiddenIndices": HiddenIndices,
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl09$divDropDown$ctl01$HiddenIndices": "0",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$store": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ToggleParam$collapse": "false",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl10$ctl00$CurrentPage": "",
            "null": "100",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl10$ctl03$ctl00": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl13$ClientClickedId": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$store": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl12$collapse": "false",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$VisibilityState$ctl00": "None",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ScrollPosition": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl02": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl03": "",
            "ctl00$ContentPlaceHolder1$ReportViewer1$ctl14$ReportControl$ctl04": "100",
            "__ASYNCPOST": "true",
            # "ctl00$ContentPlaceHolder1$ReportViewer1$ctl09$ctl00": "View Report"
        }

        headers = make_headers()
        # headers['Cookie'] = response.headers[b'Set-Cookie'].decode()
        request = FormRequest(
            url=self.start_url,
            method='POST',
            formdata=formdata,
            headers=headers,
            callback=self.get_details,
            errback=self.fail_details,
            dont_filter=True,
            meta={
                'url': self.start_url,
                'Jaar': Jaar,
                'Kwartaal': Kwartaal,
                'Regio': Regio,
                'HiddenIndices': HiddenIndices,
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def fail_Arbeidsmarktregio_tokens(self, failure):
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_Arbeidsmarktregio_tokens,
            errback=self.fail_Arbeidsmarktregio_tokens,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                'Jaar': failure.request.meta['Jaar'],
                'Kwartaal': failure.request.meta['Kwartaal'],
                'Regio': failure.request.meta['Regio'],
                'HiddenIndices': failure.request.meta['HiddenIndices'],
                '__VIEWSTATE': failure.request.meta['__VIEWSTATE'],
                '__VIEWSTATEGENERATOR': failure.request.meta['__VIEWSTATEGENERATOR'],
                '__EVENTVALIDATION': failure.request.meta['__EVENTVALIDATION'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_details(self, response):
        Jaar = response.meta['Jaar']
        Kwartaal = response.meta['Kwartaal']
        Regio = response.meta['Regio']
        HiddenIndices = response.meta['HiddenIndices']
        split_info = response.text.split('|')

        try:
            tree = html.fromstring(split_info[11])
            rows = tree.xpath('//table[@id]/tr[not(@id)]')
            for i, row in enumerate(rows):
                result_row = [td.xpath('.//text()')[0].strip() if td.xpath('.//text()') else "" for td in
                              row.xpath('./td')]
                if i == 0:
                    result_row = result_row[2:]
                else:
                    result_row = result_row[1:]
                result_row = [Jaar, Kwartaal, Regio] + result_row
                self.insert_row(result_row)
                print(result_row)
        except Exception as e:
            print([Jaar, Kwartaal, HiddenIndices, Regio])
            # print(e)
            pass

    def fail_details(self, failure):
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_details,
            errback=self.fail_details,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                'Jaar': failure.request.meta['Jaar'],
                'Kwartaal': failure.request.meta['Kwartaal'],
                'Regio': failure.request.meta['Regio'],
                'HiddenIndices': failure.request.meta['HiddenIndices'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def create_result_file(self):
        self.result_fp = open(self.result_fname, 'w', encoding='utf-8-sig', newline='')
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

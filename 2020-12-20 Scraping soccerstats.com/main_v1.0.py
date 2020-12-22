import random
import os
import csv
import scrapy
from scrapy.http import FormRequest
from scrapy import signals
import json
from urllib.parse import urljoin


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

        # self.result_fname = os.path.join(self.result_dir, "Result.csv")
        # self.create_result_file()
        #
        # heading = [
        #     "", "", "", "", "", "", "", ""
        # ]
        # if os.path.getsize(self.result_fname) == 0:
        #     self.insert_row(result_row=heading)

        self.total_cnt = 0
        self.total_result = {'leagues': {}}
        self.total_links = []

        self.start_url = "https://www.soccerstats.com/"

    def start_requests(self):
        request = FormRequest(
            url=self.start_url,
            method='GET',
            headers=make_headers(),
            callback=self.get_leagues,
            errback=self.fail_leagues,
            dont_filter=True,
            meta={
                'url': self.start_url,
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_leagues(self, response):
        url = response.meta['url']

        rows = response.xpath('(//table[@bgcolor="#b0b0b0"]//tr[@class="trow2"])[2]/td/span')
        for i, row in enumerate(rows):
            leagueName = row.xpath('./@title').extract_first()
            leagueURL = urljoin("https://www.soccerstats.com/", row.xpath('./a/@href').extract_first())

            request = FormRequest(
                url=leagueURL,
                method='GET',
                headers=make_headers(),
                callback=self.get_teams,
                errback=self.fail_teams,
                dont_filter=True,
                meta={
                    'leagueURL': leagueURL,
                    'leagueName': leagueName,
                    # 'proxy': pxy
                    # 'handle_httpstatus_all': True,
                    # 'dont_redirect': True,
                }
            )
            yield request

    def fail_leagues(self, failure):
        request = FormRequest(
            url=failure.request.meta['url'],
            method='GET',
            headers=make_headers(),
            callback=self.get_leagues,
            errback=self.fail_leagues,
            dont_filter=True,
            meta={
                'url': failure.request.meta['url'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_teams(self, response):
        leagueURL = response.meta['leagueURL']
        leagueName = response.meta['leagueName']

        if leagueName not in self.total_result['leagues']:
            self.total_result['leagues'][leagueName] = {}

        self.total_result['leagues'][leagueName]['leagueURL'] = leagueURL
        self.total_result['leagues'][leagueName]['teams'] = {}

        rows = response.xpath('//div[@class="eight columns"]/table[4]//tr[@class="odd"]')
        for i, row in enumerate(rows):
            ranking = row.xpath('./td[1]/b/text()').extract_first()
            teamName = row.xpath('./td[2]/a/text()').extract_first()
            teamLink = urljoin("https://www.soccerstats.com/", row.xpath('./td[2]/a/@href').extract_first())
            pointsPerGame = row.xpath('./td[12]//text()').extract_first()
            pointsPerGameLast8 = row.xpath('./td[13]//text()').extract_first()
            cleanSheet = row.xpath('./td[14]//text()').extract_first()
            failedToScore = row.xpath('./td[15]//text()').extract_first()

            if teamName not in self.total_result['leagues'][leagueName]['teams']:
                self.total_result['leagues'][leagueName]['teams'][teamName] = {}

            self.total_result['leagues'][leagueName]['teams'][teamName] = {
                'teamLink': teamLink,
                'ranking': ranking,
            }
            self.total_result['leagues'][leagueName]['teams'][teamName]['stats'] = {
                'pointsPerGame': pointsPerGame,
                'pointsPerGameLast8': pointsPerGameLast8,
                'cleanSheet': cleanSheet,
                'failedToScore': failedToScore,
            }

            request = FormRequest(
                url=teamLink,
                method='GET',
                headers=make_headers(),
                callback=self.get_more_stats,
                errback=self.fail_more_stats,
                dont_filter=True,
                meta={
                    'teamLink': teamLink,
                    'leagueURL': leagueURL,
                    'leagueName': leagueName,
                    'teamName': teamName,
                    # 'proxy': pxy
                    # 'handle_httpstatus_all': True,
                    # 'dont_redirect': True,
                }
            )
            yield request

    def fail_teams(self, failure):
        request = FormRequest(
            url=failure.request.meta['leagueURL'],
            method='GET',
            headers=make_headers(),
            callback=self.get_teams,
            errback=self.fail_teams,
            dont_filter=True,
            meta={
                'leagueURL': failure.request.meta['leagueURL'],
                'leagueName': failure.request.meta['leagueName'],
                # 'proxy': pxy
                # 'handle_httpstatus_all': True,
                # 'dont_redirect': True,
            }
        )
        yield request

    def get_more_stats(self, response):
        teamLink = response.meta['teamLink']
        leagueURL = response.meta['leagueURL']
        leagueName = response.meta['leagueName']
        teamName = response.meta['teamName']

        rows = response.xpath(
            f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr[contains(@class,"trow")]')

        try:
            scoring = {
                'goalsScored': {
                    "home": rows[1].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[1].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[1].xpath('./td[4]//text()').extract_first().strip(),
                },
                'goalsScoredPerMatch': {
                    "home": rows[2].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[2].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[2].xpath('./td[4]//text()').extract_first().strip(),
                },
                'goalsConceded': {
                    "home": rows[3].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[3].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[3].xpath('./td[4]//text()').extract_first().strip(),
                },
                'goalsConcededPerMatch': {
                    "home": rows[4].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[4].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[4].xpath('./td[4]//text()').extract_first().strip(),
                },
                'scoredConcPerMatch': {
                    "home": rows[5].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[5].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[5].xpath('./td[4]//text()').extract_first().strip(),
                },
                'matchesOver1.5Goals': {
                    "home": rows[6].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[6].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[6].xpath('./td[4]//text()').extract_first().strip(),
                },
                'matchesOver2.5Goals': {
                    "home": rows[7].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[7].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[7].xpath('./td[4]//text()').extract_first().strip(),
                },
                'matchesOver3.5Goals': {
                    "home": rows[8].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[8].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[8].xpath('./td[4]//text()').extract_first().strip(),
                },
                'matchesOver4.5Goals': {
                    "home": rows[9].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[9].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[9].xpath('./td[4]//text()').extract_first().strip(),
                },
                'matchesOver5.5Goals': {
                    "home": rows[10].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[10].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[10].xpath('./td[4]//text()').extract_first().strip(),
                },
                'over0.5gAtHalftime': {
                    "home": rows[11].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[11].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[11].xpath('./td[4]//text()').extract_first().strip(),
                },
                'over1.5gAtHalftime': {
                    "home": rows[12].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[12].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[12].xpath('./td[4]//text()').extract_first().strip(),
                },
                'over2.5gAtHalftime': {
                    "home": rows[13].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[13].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[13].xpath('./td[4]//text()').extract_first().strip(),
                },
            }

            self.total_result['leagues'][leagueName]['teams'][teamName]['stats']['scoring'] = scoring

            rows = response.xpath(
                '//h2[contains(text(),"Scoring patterns")]/../../../tr[contains(@class,"trow")]')

            goalNoGoal = {
                'cleanSheets': {
                    "home": rows[1].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[1].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[1].xpath('./td[4]//text()').extract_first().strip(),
                },
                'wonToNil': {
                    "home": rows[2].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[2].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[2].xpath('./td[4]//text()').extract_first().strip(),
                },
                'scoredInBothHalves': {
                    "home": rows[3].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[3].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[3].xpath('./td[4]//text()').extract_first().strip(),
                },
                'bothTeamsScored': {
                    "home": rows[4].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[4].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[4].xpath('./td[4]//text()').extract_first().strip(),
                },
                'failedToScore': {
                    "home": rows[5].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[5].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[5].xpath('./td[4]//text()').extract_first().strip(),
                },
                'lostToNil': {
                    "home": rows[6].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[6].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[6].xpath('./td[4]//text()').extract_first().strip(),
                },
                'concededInBothHalves': {
                    "home": rows[7].xpath('./td[2]//text()').extract_first().strip(),
                    "away": rows[7].xpath('./td[3]//text()').extract_first().strip(),
                    "all": rows[7].xpath('./td[4]//text()').extract_first().strip(),
                }
            }
        except:
            print('')

        self.total_result['leagues'][leagueName]['teams'][teamName]['stats']['goalNoGoal'] = goalNoGoal

    def fail_more_stats(self, failure):
        request = FormRequest(
            url=failure.request.meta['teamLink'],
            method='GET',
            headers=make_headers(),
            callback=self.get_more_stats,
            errback=self.fail_more_stats,
            dont_filter=True,
            meta={
                'teamLink': failure.request.meta['teamLink'],
                'leagueURL': failure.request.meta['leagueURL'],
                'leagueName': failure.request.meta['leagueName'],
                'teamName': failure.request.meta['teamName'],
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
        print(json.dumps(self.total_result, indent=3))


if __name__ == '__main__':
    from scrapy.utils.project import get_project_settings
    from scrapy.crawler import CrawlerProcess

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(MainScraper)
    process.start()

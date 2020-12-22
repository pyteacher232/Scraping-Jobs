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

        self.result_fname = os.path.join(self.result_dir, "Stats.json")
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

        scoring = {
            'goalsScored': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored") and not(contains(text(),"Goals scored per match"))]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored") and not(contains(text(),"Goals scored per match"))]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored") and not(contains(text(),"Goals scored per match"))]/following-sibling::td[3]//text()').extract_first(),
            },
            'goalsScoredPerMatch': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored per match")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored per match")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals scored per match")]/following-sibling::td[3]//text()').extract_first(),
            },
            'goalsConceded': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded") and not(contains(text(),"Goals conceded per match"))]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded") and not(contains(text(),"Goals conceded per match"))]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded") and not(contains(text(),"Goals conceded per match"))]/following-sibling::td[3]//text()').extract_first(),
            },
            'goalsConcededPerMatch': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded per match")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded per match")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Goals conceded per match")]/following-sibling::td[3]//text()').extract_first(),
            },
            'scoredConcPerMatch': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Scored+conc. per match")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Scored+conc. per match")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Scored+conc. per match")]/following-sibling::td[3]//text()').extract_first(),
            },
            'matchesOver1.5Goals': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 1.5 goals")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 1.5 goals")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 1.5 goals")]/following-sibling::td[3]//text()').extract_first(),
            },
            'matchesOver2.5Goals': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 2.5 goals")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 2.5 goals")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 2.5 goals")]/following-sibling::td[3]//text()').extract_first(),
            },
            'matchesOver3.5Goals': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 3.5 goals")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 3.5 goals")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 3.5 goals")]/following-sibling::td[3]//text()').extract_first(),
            },
            'matchesOver4.5Goals': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 4.5 goals")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 4.5 goals")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 4.5 goals")]/following-sibling::td[3]//text()').extract_first(),
            },
            'matchesOver5.5Goals': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 5.5 goals")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 5.5 goals")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Matches over 5.5 goals")]/following-sibling::td[3]//text()').extract_first(),
            },
            'over0.5gAtHalftime': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 0.5 g. at halftime")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 0.5 g. at halftime")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 0.5 g. at halftime")]/following-sibling::td[3]//text()').extract_first(),
            },
            'over1.5gAtHalftime': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 1.5 g. at halftime")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 1.5 g. at halftime")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 1.5 g. at halftime")]/following-sibling::td[3]//text()').extract_first(),
            },
            'over2.5gAtHalftime': {
                "home": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 2.5 g. at halftime")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 2.5 g. at halftime")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"statistics") and contains(text(), "{teamName}")]/../../../tr/td[contains(text(),"Over 2.5 g. at halftime")]/following-sibling::td[3]//text()').extract_first(),
            },
        }

        self.total_result['leagues'][leagueName]['teams'][teamName]['stats']['scoring'] = scoring

        goalNoGoal = {
            'cleanSheets': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Clean sheets")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Clean sheets")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Clean sheets")]/following-sibling::td[3]//text()').extract_first(),
            },
            'wonToNil': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Won-to-nil")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Won-to-nil")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Won-to-nil")]/following-sibling::td[3]//text()').extract_first(),
            },
            'scoredInBothHalves': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Scored in both halves")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Scored in both halves")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Scored in both halves")]/following-sibling::td[3]//text()').extract_first(),
            },
            'bothTeamsScored': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Both teams scored")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Both teams scored")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Both teams scored")]/following-sibling::td[3]//text()').extract_first(),
            },
            'failedToScore': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Failed to score")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Failed to score")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Failed to score")]/following-sibling::td[3]//text()').extract_first(),
            },
            'lostToNil': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Lost-to-nil")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Lost-to-nil")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Lost-to-nil")]/following-sibling::td[3]//text()').extract_first(),
            },
            'concededInBothHalves': {
                "home": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Conceded in both halves")]/following-sibling::td[1]//text()').extract_first(),
                "away": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Conceded in both halves")]/following-sibling::td[2]//text()').extract_first(),
                "all": response.xpath(
                    f'//h2[contains(text(),"Scoring patterns")]/../../../tr/td[contains(text(),"Conceded in both halves")]/following-sibling::td[3]//text()').extract_first(),
            }
        }

        self.total_result['leagues'][leagueName]['teams'][teamName]['stats']['goalNoGoal'] = goalNoGoal

        corners = {
            "teamCorners": {
                "homeMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "awayMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "homeAndAway": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Team corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[6]//text()').extract_first(),
                },
            },
            "opponentsCorners": {
                "homeMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "awayMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "homeAndAway": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Opponents corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[6]//text()').extract_first(),
                },
            },
            "totalMatchCorners": {
                "homeMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "awayMatches": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Away matches")]/following-sibling::td[6]//text()').extract_first(),
                },
                "homeAndAway": {
                    "average": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[1]//text()').extract_first(),
                    "2.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[2]//text()').extract_first(),
                    "3.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[3]//text()').extract_first(),
                    "4.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[4]//text()').extract_first(),
                    "5.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[5]//text()').extract_first(),
                    "6.5+": response.xpath(
                        '//h2[contains(text(),"Total match corners")]/../../../tr/td[contains(text(),"Home and Away")]/following-sibling::td[6]//text()').extract_first(),
                },
            }
        }
        self.total_result['leagues'][leagueName]['teams'][teamName]['stats']['corners'] = corners

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
        with open(self.result_fname, "w", encoding='utf-8-sig') as fp:
            result_str = json.dumps(self.total_result, indent=3)
            fp.write(result_str)
            fp.flush()
            print(result_str)


if __name__ == '__main__':
    from scrapy.utils.project import get_project_settings
    from scrapy.crawler import CrawlerProcess

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(MainScraper)
    process.start()

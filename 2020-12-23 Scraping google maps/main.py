from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import json


first_driver.get(first_link)
WebDriverWait(first_driver, 10).until(
    EC.presence_of_all_elements_located(
        (By.XPATH,
         '//script[contains(text(), "(function(){window.APP_OPTIONS")]'))
)

javascript = first_driver.execute_script('return window.APP_INITIALIZATION_STATE[3][2];').replace(")]}'", '').strip()
json_info = json.loads(javascript)
rows = json_info[0][1]

for i, row in enumerate(rows):
    if len(row) == 15:
        try:
            lat = str(row[14][9][2])
        except:
            lat = ''
        try:
            lon = str(row[14][9][3])
        except:
            lon = ''
        try:
            name = str(row[14][11])
        except:
            name = ''
        try:
            address = str(row[14][2][0])
        except:
            address = ''
        try:
            city = str(row[14][2][1])
        except:
            city = ''
        try:
            zip_code = str(row[14][2][2])
        except:
            zip_code = ''
        try:
            country = str(row[14][2][3])
        except:
            country = ""
        try:
            phone = str(row[14][3][0])
        except:
            phone = ''
        try:
            website = str(row[14][7][1])
        except:
            website = ''
        try:
            opening_hours = "\n".join(["{}: {}".format(line[0], line[1][0]) for line in row[14][34][1]])
        except:
            opening_hours = ''
        try:
            reviews_cnt = str(row[14][4][8])
        except:
            reviews_cnt = ''
        try:
            reviews_rating = str(row[14][4][7])
        except:
            reviews_rating = ''
        try:
            image = row[14][37][0][0][6][0]
        except:
            image = ""
        image = 'https:' + image if 'http' not in image else image

        result_row = [
            name, lat, lon, address, city, zip_code, country, phone, website, opening_hours, reviews_cnt,
            reviews_rating,
            image
        ]
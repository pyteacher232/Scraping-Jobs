import requests
from lxml import html

url = "https://en.wikipedia.org/wiki/Fields_Medal"

r = requests.get(url)

tree = html.fromstring(r.text)

rows = tree.xpath('//table[@class="wikitable sortable"]/tbody/tr')
winners = []

for i, row in enumerate(rows):
    if i==0:
        continue

    cols = [r.xpath('.//text()') for r in row.xpath('./td')]
    try:
        _1 = [e.strip() for e in cols[1] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _1 = []
    try:
        _2 = [e.strip() for e in cols[2] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _2 = []
    try:
        _3 = [e.strip() for e in cols[3] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _3 = []
    try:
        _4 = [e.strip() for e in cols[4] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _4 = []
    try:
        _5 = [e.strip() for e in cols[5] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _5 = []
    try:
        _6 = [e.strip() for e in cols[6] if e.strip() and e.strip() not in [';', '(prize not established)', '—', 'None', '[a]']]
    except:
        _6 = []
    physics.extend(_1)
    chemistry.extend(_2)
    medicine.extend(_3)
    literature.extend(_4)
    peace.extend(_5)
    economics.extend(_6)

print(f"physics: {len(physics)}")
print(f"chemistry: {len(chemistry)}")
print(f"medicine: {len(medicine)}")
print(f"literature: {len(literature)}")
print(f"peace: {len(peace)}")
print(f"economics: {len(economics)}")
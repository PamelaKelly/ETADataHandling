import lxml
from lxml import html
import requests
import json
# we have to use page.content rather than page.text because html.fromstring
# implicitly expects bytes as input

def scrape_timetable(page):
    tree = html.fromstring(page.content)

    # Tree now contains the whole HTML file in a nice tree structure which
    # we can go over two different ways: XPath and CSSSelect. We'll use the former.

    # XPath = XML Path Language
    line_id = tree.xpath('//div[@id="timetable_top_left"]/text()')
    line_id = line_id[0].strip()

    route_description = tree.xpath('//div[@id="route_description"]/text()')
    route_description = route_description[0].strip()
    terminus1 = ""
    terminus2 = ""
    route_breakdown = route_description.split()

    flag = False
    for word in route_breakdown:
        if word == 'From':
            continue
        elif word != 'Towards' and flag == False:
            terminus1 += (word + " ")
        elif word == 'Towards':
            flag = True
            continue
        else:
            terminus2 += (word + " ")

    print("T1: ", terminus1)
    print("T2: ", terminus2)
    direction1 = "From " + terminus1 + " to " + terminus2
    direction0 = "From " + terminus2 + " to " + terminus1
    print("d1: ", direction1)
    print("d0: ", direction0)

    route_variations = tree.xpath("//div[contains(.,'Route Variations')]/p/text()")
    route_vars = []
    route_vars_full = {}
    for route in route_variations:
        if len(route) > 3:
            code = route[:1]
            desc = route[2:]
            route_vars.append(code)
            route_vars_full[code] = desc

    timetable = {
        "direction1": {
            "description": direction1,
            "weekday": [],
            "saturday": [],
            "sunday": [],
            "variations": {}
        },
        "direction0": {
            "description": direction0,
            "weekday": [],
            "saturday": [],
            "sunday": [],
            "variations": {}
        },
    }


    timetables = tree.xpath("//div[@class='timetable_horiz_display']//text()")
    for t in timetables:
        t = t.strip()

    nums = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    flag = ""
    half_way_point = int(len(timetables)/2)
    direction_1 = timetables[:half_way_point]
    direction_0 = timetables[half_way_point:]
    last_entry = ""

    ts = [direction_0, direction_1]


    for i in range(len(ts)):
        direction = "direction0" if i == 0 else "direction1"
        for t in ts[i]:
            t = t.strip(' \n\t')
            if t.startswith("Monday"):
                flag = "M"
            elif t.startswith("Saturday"):
                flag = "ST"
                continue
            elif t.startswith("Sunday"):
                flag = "SN"
                continue
            elif t == "":
                continue
            else:
                if t[0] in nums:
                    if flag == "M":
                        timetable[direction]["weekday"].append(t)
                        last_entry = "weekday"
                    elif flag == "ST":
                        timetable[direction]["saturday"].append(t)
                        last_entry = "saturday"
                    elif flag == "SN":
                        timetable[direction]["sunday"].append(t)
                        last_entry = "sunday"
                elif t in route_vars:
                    if t in timetable[direction]["variations"].keys():
                        pass
                    else:
                        timetable[direction]["variations"][t] = {"description": "", "weekday": [], "saturday": [], "sunday": []}
                        timetable[direction]["variations"][t]["description"] = route_vars_full[t]
                        last_time = timetable[direction][last_entry].pop()
                        if flag == "M":
                            timetable[direction]["variations"][t]["weekday"].append(last_time)
                        elif flag == "ST":
                            timetable[direction]["variations"][t]["saturday"].append(last_time)
                        elif flag == "SN":
                            timetable[direction]["variations"][t]["sunday"].append(last_time)

    for t in timetable:
        print(timetable[t])
    return timetable, line_id

page = requests.get('http://web.archive.org/web/20121015112815/http://dublinbus.ie/en/Your-Journey1/Timetables/All-Timetables/1/')
home_page = requests.get('http://web.archive.org/web/20121004001856/http://dublinbus.ie/Your-Journey1/Timetables/')
tree_ = html.fromstring(home_page.content)
line_ids_tree = tree_.xpath('//div[@class="timetable_listing"]//text()')

line_ids = []
nums = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
for line in line_ids_tree:
    if line[0] in nums:
        line_ids.append(line)

print(line_ids)
links = []

timetable_links = tree_.xpath("/html/body//a[contains(@href, 'All-Timetables')]/@href")
for link in timetable_links:
    print(link)
    starter = "http://web.archive.org"
    starter += link
    links.append(starter)
    print(starter)

all_timetables = {}

for l in links:
    try:
        page = requests.get(l)
        print(page)
        t_all = scrape_timetable(page)
        line_id = t_all[1]
        t = t_all[0]
        all_timetables[line_id] = t
    except Exception as e:
        print("Error type: ", type(e))
        print("Error details: ", e)

# at the moment trying to use a pattern to generate the urls however the urls don't follow a pattern
# therefore I will need to extract the url from the href in the main list of timetables page

print(len(all_timetables))
with open('../datasets/output_files/timetables.txt', 'w') as f:
    json.dump(all_timetables, f)

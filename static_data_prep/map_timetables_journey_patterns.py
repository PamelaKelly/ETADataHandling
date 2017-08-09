import json
import re

with open('../datasets/output_files/timetables.txt', 'r') as f:
    timetables = json.load(f)

with open('../datasets/output_files/routes.txt', 'r') as f:
    routes = json.load(f)

with open('../datasets/input_files/2012_stops.txt', 'r') as f:
    stops = json.load(f)


print(len(timetables.keys()))
# we want to add journey_pattern to the timetables listing
# so essentially timetables[line_id][direction]["journey_pattern"] = journey_pattern
def match():
    print(timetables)
    count = 0
    error_count = 0
    for route in routes:
        try:
            print("############################################################")
            count += 1
            line_id = routes[route].pop("line_id")

            # print details
            if route.endswith('1'):
                print("Journey Pattern: ", route)
                print("Line Id: ", line_id)
                print("Count: ", count)

                # get the first and last stop on the route
                first_stop = min(routes[route].items(), key=lambda x: x[1])[0]
                last_stop = max(routes[route].items(), key=lambda x: x[1])[0]

                # use that to get the addresses of the stops
                first_stop_address = stops[str(first_stop)]["stop_address"]

                # breaking down the stop address to try to match the directions
                stop_address_parts = first_stop_address.split(',')
                print(stop_address_parts)
                stop_address_keywords = stop_address_parts[0].split()
                for i in range(len(stop_address_keywords)):
                    stop_address_keywords[i] = stop_address_keywords[i].lower().strip('()')
                key_address = stop_address_keywords[0] + " " + stop_address_keywords[1]
                print("Precise Stop Address: ", key_address)
                area = stop_address_parts[1][0].lower().strip()
                print("Area: ", area)
                # cross reference this address with the route description scraped with the timetables
                direction1_start = timetables[line_id]["direction1"]["description"].lower().split()[1]
                direction2_start = timetables[line_id]["direction0"]["description"].lower().split()[1]
                print("Direction 0 start: ", direction2_start)
                print("Direction 1 Start: ", direction1_start)
                if direction1_start.lower().find(area) != -1:
                    print("This journey pattern matches Direction 1")
                elif direction2_start.lower().find(area) != -1:
                    print("This journey pattern matches Direction 0")
                else:
                    print("This is a variation of the main route")
            else:
                print("Journey Pattern ID: ", route, " is a variation")
                print("Journey Pattern: ", route)
                print("Line Id: ", line_id)
                print("Count: ", count)

                # get the first and last stop on the route
                first_stop = min(routes[route].items(), key=lambda x: x[1])[0]
                last_stop = max(routes[route].items(), key=lambda x: x[1])[0]

                # use that to get the addresses of the stops
                first_stop_address = stops[str(first_stop)]["stop_address"]

        except Exception as e:
            print("Error with this line_id: ", line_id)
            error_count += 1
            print("Error count: ", error_count)
            print("Error: ", e)
            continue


match()









import pandas as pd
import json

with open('../datasets/output_files/timetables.txt', 'r') as f:
    timetables = json.load(f)

with open('../datasets/output_files/routes.txt', 'r') as f:
    routes = json.load(f)

print(type(routes))
print(routes)
for route in routes:
    print(type(routes[route]))
    minimum = min(routes[route].items(), key=lambda x: x[1])[0]
    print(minimum)





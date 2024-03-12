import csv

source_file = open("2020citibiketrips.csv", "r")

citibike_reader = csv.DictReader(source_file)

print(citibike_reader.fieldnames)
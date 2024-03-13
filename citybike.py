import csv

source_file = open("/Users/sydneymfuniselwa/sweetapp/2020-citibike-tripdata/9_September/202009-citibike-tripdata_2.csv", "r")

citibike_reader = csv.DictReader(source_file)

#print(citibike_reader.fieldnames)

subscriber_count = 0
customer_count = 0
other_user_count = 0

for a_row in citibike_reader:
    if a_row["usertype"] == "Subscriber":
        subscriber_count = subscriber_count +1
    elif a_row["usertype"] == "Customer":
        customer_count = customer_count +1
    else:
        other_user_count = other_user_count + 1

print("Number of subscribers:")
print(subscriber_count)
print("Number of customers:") 
print(customer_count) 
print("Number of 'other' users:") 
print(other_user_count)

    


import json
import pandas as pd

INPUT_FILE = "data/yelp_academic_dataset_business.json"
OUTPUT_FILE = "output/yelp_business_flat.csv"

#create an empty python list

#each business record in the JSON file will be turned into one flat dictionary, thne we will store each of those dictionaries in the "rows" list
#in that way, each JSON object becomes one Python dictionary
#all dictionaries go into "rows"
#then pandas library will be used to turn these "rows" into a dataframe
rows = []

#with opens the file, let us read it and close it automatically when we are done

with open(INPUT_FILE, "r", encoding="utf-8") as f: #opens the JSON file, "r" read mode, encoding to ensure that it reads the text correctly, f gives the file name
    for line in f:
        item = json.loads(line)

        attributes = item.get("attributes", {}) or {} # this command tries to get the attributes field from the JSON object, if the attribute exists but is None, still replace it with empty dictionary
        hours = item.get("hours", {}) or {} # same idea as the attributes

        row = {# creating the new flat dictionary
            "business_id": item.get("business_id"),
            "name": item.get("name"),
            "city": item.get("city"),
            "state": item.get("state"),
            "postal_code": item.get("postal_code"),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
            "stars": item.get("stars"),
            "review_count": item.get("review_count"),
            "is_open": item.get("is_open"),
            "categories": item.get("categories"),
            "restaurants_price_range": attributes.get("RestaurantsPriceRange2"),
            "bike_parking": attributes.get("BikeParking"),
            "business_accept_credit_cards": attributes.get("BusinessAcceptCreditsCards"),
            "good_for_kids": attributes.get("GoodForKids"),
            "restaurants_take_out": attributes.get("RestaurantsTakeOut"),
            "restaurants_delivery": attributes.get("RestaurantsDelivery"),
            "wheelchair_access": attributes.get("WheelchairAccesible"),
            "outdoor_seating": attributes.get("OutdoorSeating"),
            "monday_hours": hours.get("Monday"),
            "tuesday_hours": hours.get("Tuesday"),
            "wednesday_hours": hours.get("Wednesday"),
            "thursady_hours": hours.get("Thursday"),
            "saturday_hours": hours.get("Saturday"),
            "Sunday_hours": hours.get("Sunday"),

        }
        rows.append(row)

df = pd.DataFrame(rows) #use pandas function (pd.Dataframe to convert the flat dictionary to python dataframe)       

print(df.head()) # ask to print the five first rows to get a quick overview

df.to_csv(OUTPUT_FILE, index=False, encoding= "utf-8")
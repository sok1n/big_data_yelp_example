import json
import pandas as pd

def flatten_json(data, parent_key="", seperator=""):

    #Recursively flattern a nested JSON dictionary.

    # Paarameters (inputs to be function)
    #data (dict): The JSON dictionary to be flatten
    #parent_key(str): the prefix for nested keys
    #seperator(str): the symbol used to join parent and child keys

    # Return: flattened dictionary

    items={} # store flattened key-value pairs

    #Loop through each key-value in the JSON dictionary object
    for key, value in data.items():
        #build the new key name
        #example: attribute + "_" + BikeParking --> attribute_BikeParking
        new_key = parent_key + seperator + key if parent_key else key

        # if the key is another dictionary, flatten recursively
        if isinstance(value, dict):
            items.update(
                flatten_json(value, new_key, seperator)
            )
            #if the value is a list, convert it to string
        elif isinstance(value, list):
                items[new_key]= str(value)
            
            #otherwise, if it is  a normal value, strech the value and store it
        else:
                items[new_key] = value
    return items

def auto_flatten_json(input_file, output_file):
    #Read a JSON file , flatten each JSON object, and save the result as a CSV file.

    rows = []

    with open(input_file, "r",encoding="utf-8") as f:
        
        #process the file line by line
        for line in f:

            # covert the JSON text to a python dictionary
            data=json.loads(line)

            #flatten the dictionary automatically
            flat_record = flatten_json(data)

            #add to the list of rows
            rows.append(flat_record)
    df = pd.DataFrame(rows)

    print(df.columns)

    #save to a csv
    df.to_csv(output_file, index=False, encoding="utf-8")
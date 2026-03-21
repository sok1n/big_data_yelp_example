import os # this library is Python's standard library
        # work with files, navigate threw folders and files,
        # create or remove dictionaries
        # manipulate paths
        # read environment variables
from flatteners.auto_flatten_json import auto_flatten_json

# list all files in the data folder
files = os.listdir("data")

print(files)

# Loop through all files
for file in files:

    # only process JSON files
    if file.endswith(".json"):

        #build input path
        input_path = os.path.join("data", file)

        #create output csv
        output_name = file.replace(".json", "_flat.csv")

        #build output path
        output_path = os.path.join("output", output_name)

        # run auto flattener
        auto_flatten_json(input_path, output_path)
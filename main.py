import boto3
import json
import time


# This file is used to compare s3 files against a json file
# we set any matches to private so users cannot access them

# check if s3 object key (s3 path) is in our json file
def check_s3_object_key_in_json(s3_object_key, json_objects_file):
    with open(json_objects_file, 'r') as file:
        json_data = json.load(file)

    for obj in json_data:
        if obj['guid'] in s3_object_key:
            return True

    return False


# Set the s3 objects to private,
def set_s3_objects_acl_private(bucket_name, object_keys):
    s3 = boto3.client('s3')
    # Set the ACL of the S3 object to private
    for key in object_keys:
        response = s3.put_object_acl(ACL='private', Bucket=bucket_name, Key=key)


# Write a confirmed s3 object key to a new file
def write_s3_object_keys_to_file(s3_object_keys, output_file, page):
    data = {
        str(page): s3_object_keys
    }
    with open(output_file, 'a') as file:
        json.dump(data, file)
        file.write('\n')


def compare_s3_objects_with_json(bucket_name, prefix, json_objects, output_file):
    # track the time it takes to run
    start_time = time.time()
    # Init s3 client
    s3 = boto3.client('s3')

    # Setup our paginator
    paginator = s3.get_paginator('list_objects_v2')

    for year in range(2010, 2016):
        prefix = "test_delete/" + str(year)
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        count = 0
        matched_objs = []
        # Paginate through our results
        for page in page_iterator:
            count += 1
            # Iterate over the S3 objects and compare with JSON objects
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_object_key = obj['Key']
                    # Check if the S3 object key is in our json file
                    if check_s3_object_key_in_json(s3_object_key, json_objects):
                         # Write the s3 object key to our array
                        matched_objs.append(s3_object_key)
                        print('Setting to private: ' + s3_object_key)
                # change ACL to private
                set_s3_objects_acl_private(bucket_name, matched_objs)
                # at the end, write our privatized object keys to a new file
                # write_s3_object_keys_to_file(matched_objs, output_file, count)

    end_time = time.time()
    total_time = end_time - start_time

    # Write the total time to the output file
    with open(output_file, 'a') as file:
        json.dump({'total_time': total_time}, file)
        file.write('\n')

    return matched_objs


json_objects = 'export.json'  # Load JSON objects
bucket_name = 'static.hiphopdx.com'  # Set bucket name
bucket_prefix = 'test_delete'  # set bucket prefix (not required)
output_file = 'updated_files.json'  # file to output changes to

# Compare s3 objects with JSON objects
compare_s3_objects_with_json(bucket_name, bucket_prefix, json_objects, output_file)
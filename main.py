import boto3
import json
import time


# This file is used to compare s3 files against a json file
# we set any matches to private so users cannot access them

# check if s3 object key (s3 path) is in our json file
def check_object_keys(s3_object_key, json_objects_file, matched_objs):
    with open(json_objects_file, 'r') as file:
        json_data = json.load(file)

    # if any of the objects in `json_data` have a `guid` value that is in `s3_object_key`
    if any(obj['guid'] in s3_object_key for obj in json_data):
        matched_objs.append(s3_object_key)

    return matched_objs


# Set the s3 objects to private,
def set_s3_objects_acl_private(bucket_name, object_keys):
    s3 = boto3.client('s3')

    # Initialize a counter to keep track of the number of objects whose ACL is set to private
    count = 0

    # Iterate over the object keys
    for key in object_keys:
        # Get the ACL (Access Control List) of the object
        response = s3.get_object_acl(Bucket=bucket_name, Key=key)
        grants = response['Grants']

        # Iterate over the grants in the ACL
        for grant in grants:
            grantee = grant.get('Grantee', {})
            permission = grant.get('Permission')
            # Check if the grantee is 'Everyone' and the permission is 'READ'
            if (
                grantee.get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' and
                permission == 'READ'
            ):
                # Set the ACL to private
                response = s3.put_object_acl(
                    ACL='private',
                    Bucket=bucket_name,
                    Key=key
                )
                count += 1

    # We're done looping through the s3 object grants. return the count
    return count


def compare_s3_objects_with_json(bucket_name, json_objects, output_file):
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
            if 'Contents' not in page:
                continue

            # Iterate over the S3 objects and compare with JSON objects
            for obj in page['Contents']:
                s3_object_key = obj['Key']
                # Check if the S3 object key is in our json file
                # We also check for any additional files with the same name. a lot of files have different sizes.
                if s3_object_key.endswith(('jpg', 'png')):
                    # append new object keys. @TODO: Should we append here instead of in check_object_keys?
                    matched_objs = check_object_keys(s3_object_key, json_objects, matched_objs)
                    count += 1

            # change ACL to private
            if len(matched_objs) > 0:
                private_files = set_s3_objects_acl_private(bucket_name, matched_objs)

            # at the end, write our privatized object keys to a new file
            print("year: ", year)
            print("file count: ", count)
            print("files set to private: ", private_files)

    end_time = time.time()
    total_time = end_time - start_time

    # Write the total time to the output file
    with open(output_file, 'a') as file:
        json.dump({'total_time': total_time}, file)
        file.write('\n')

    return matched_objs


json_objects = 'test_export.json'  # Load JSON objects
bucket_name = 'static.hiphopdx.com'  # Set bucket name
bucket_prefix = 'test_delete'  # set bucket prefix (not required)
output_file = 'updated_files.json'  # file to output changes to

# Compare s3 objects with JSON objects
compare_s3_objects_with_json(bucket_name, json_objects, output_file)
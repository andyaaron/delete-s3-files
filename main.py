import boto3
import json
import time


# This file is used to compare s3 files against a json file
# we set any matches to private so users cannot access them

# check if s3 object key (s3 path
def check_s3_object_key_in_json(s3_object_key, json_objects_file):
    with open(json_objects_file, 'r') as file:
        json_data = json.load(file)

    for obj in json_data:
        if obj['guid'] in s3_object_key:
            return True

    return False


# Set the s3 object to private, if already private, ignore
def set_s3_object_acl_private(bucket_name, s3_object_key):
    s3 = boto3.client('s3')

    # Get the current ACL of the s3 object
    response = s3.get_object_acl(Bucket=bucket_name, Key=s3_object_key)
    current_acl = response['Grants']

    # Check if the ACL is already private
    for grant in current_acl:
        if grant['Permission'] == 'READ' and grant['Grantee']['URI'] == 'http://acs.amazonaws.com/groups/global/AllUsers':
            # Set the ACL of the S3 object to private
            s3.put_object_acl(ACL='private', Bucket=bucket_name, Key=s3_object_key)
        return


# Write a confirmed s3 object key to a new file
def write_s3_object_key_to_file(s3_object_key, output_file):
    with open(output_file, 'a') as file:
        json.dump({'key': s3_object_key}, file)
        file.write('\n')


def compare_s3_objects_with_json(bucket_name, prefix, json_objects, output_file):
    # lets track the time it takes to run
    start_time = time.time()
    s3 = boto3.client('s3')

    # TODO: add pgination to retrieve all objects
    # Iterate over the S3 objects and compare with JSON objects
    matched_objs = []
    count = 0
    for obj in s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']:
        s3_object_key = obj['Key']

        # Check if the S3 object key is in our json file
        if check_s3_object_key_in_json(s3_object_key, json_objects):
            # key found, add to our array
            matched_objs.append(s3_object_key)

            # change ACL to private
            # set_s3_object_acl_private(bucket_name, s3_object_key)

            # Write the s3 object key to an output file
            with open(output_file, 'a') as file:
                json.dump({'key': s3_object_key}, file)
                file.write(',\n')

    end_time = time.time()
    total_time = end_time - start_time

    # Write the s3 object key to an output file
    with open(output_file, 'a') as file:
        json.dump({'total_time': total_time}, file)
        file.write('\n')

    return matched_objs


json_objects = 'export.json'  # Load JSON objects
bucket_name = 'static.hiphopdx.com'  # Set bucket name
bucket_prefix = 'test_delete'  # set bucket prefix (not required)
output_file = 'updated_files.json'  # file to output changes to

# Compare s3 objects with JSON objects
matched_objects = compare_s3_objects_with_json(bucket_name, bucket_prefix, json_objects, output_file)

for obj in matched_objects:
    print(obj)

import boto3
import json


def load_json_file(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    json_objs = set(obj['guid'] for obj in data)
    return json_objs  # or dictionary if additional metadata is required


def compare_s3_objects_with_json(bucket_name, prefix, json_objects, output_file):
    s3 = boto3.client('s3')

    # TODO: add pgination to retrieve all objects

    # Iterate over the S3 objects and compare with JSON objects
    matched_objs = []
    for obj in s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']:
        s3_object_key = obj['Key']

        # Check if the S3 object key starts with s3_object_key and ends with .jpg
        if s3_object_key.startswith(s3_object_key) and s3_object_key.endswith('.jpg') in json_objects:
            matched_objs.append(s3_object_key)

            # we need to change the ACL here!
            s3.put_object_acl(
                ACL='private',
                Bucket=bucket_name,
                Key=s3_object_key
            )

            # Writ e the s3 object key to an output file
            with open(output_file, 'a') as file:
                json.dump({'key': s3_object_key}, file)
                file.write('\n')

    return matched_objs


# Load JSON objects
json_objects = load_json_file('6-22-dev-guids-to-delete.json')

bucket_name = 'static.hiphopdx.com'
bucket_prefix = 'test_delete'
output_file = 'updated_files.json'
# Compare s3 objects with JSON objects
matched_objects = compare_s3_objects_with_json(bucket_name, bucket_prefix, json_objects, output_file)

for obj in matched_objects:
    print(obj)

# image_filenames = get_image_filenames(bucket_name)

# for filename in image_filenames:
#     print(filename)

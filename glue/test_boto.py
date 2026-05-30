import boto3

try:
    sts = boto3.client("sts")
    print(sts.get_caller_identity())
    print("SUCCESS")
except Exception as e:
    print("FAILED")
    print(e)
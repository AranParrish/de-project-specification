def lambda_handler(event, context):
    print("Hello terraform :)")
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }

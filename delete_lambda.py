import boto3
from boto3.dynamodb.conditions import Key

dynamoDb = boto3.resource('dynamodb')
table = dynamoDb.Table('Image')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    table.delete_item(
        Key={
            'url': event['url']
        }
    )
    s3.delete_object(
        Bucket='image-tag-bucket',
        Key=event['url']
        )


if __name__ == '__main__':
    lambda_handler(
        {
            'url':"https://image-tag-bucket.s3.amazonaws.com/ffd3773acc9d4225b5bfc453dc2b8dd5.jpg"
        }, None)

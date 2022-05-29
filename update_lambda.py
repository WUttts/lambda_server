import boto3
from boto3.dynamodb.conditions import Key

dynamoDb = boto3.resource('dynamodb')
table = dynamoDb.Table('Image')


def lambda_handler(event, context):
    keyCondition = Key('url').eq(event['url'])
    response = table.query(
        KeyConditionExpression=keyCondition,
    )
    tags_set = set(response['Items'][0]['tags'])
    if(event['type'] == 1):
        print(event['tags'])
        item = {
            "url": event['url'],
            "tags": list(tags_set.union(set(event['tags'])))
        }
        res = table.put_item(Item=item)
        print(res)
    elif(event['type'] == 2):
        item = response['Items'][0]
        item['tags'] = list(tags_set.difference(set(event['tags'])))
        table.put_item(Item=item)


# if __name__ == '__main__':
#     lambda_handler(
#         {
#             'type': 1,
#             'url': "https://image-tag-bucket.s3.amazonaws.com/test.jpg",
#             'tags': ['person', 'car', 'cat']
#         }, None)

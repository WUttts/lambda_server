import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamoDb = boto3.resource('dynamodb')
table = dynamoDb.Table('Image')


def lambda_handler(event, context):
    res = []
    if event['type'] == 'tag':
        condtion = {}
        filterExpression = '';
        i = 1
        for tag in event['data']:
            key = ':tag' + str(i)
            condtion[key] = tag;
            filterExpression += 'contains(tags, ' + key + ') and ' ;
            i += 1
        filterExpression = filterExpression[:-5]
        print(condtion)
        print(filterExpression)
        response = table.scan(
            FilterExpression= filterExpression,
            ExpressionAttributeValues=condtion
        )
        for item in response['Items']:
            res.append(item)

    elif event['type'] == 'url':
        keyCondition = Key('url').eq(event['data'])
        response = table.query(
            KeyConditionExpression=keyCondition,
        )
        for item in response['Items']:
            res.append(item)

    return res


# if __name__ == '__main__':
#     res = lambda_handler(
#         {
#             'type': 'tag',
#             'data': ['person']
#         },
#         None)
#     print(res)

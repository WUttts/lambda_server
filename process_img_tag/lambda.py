import boto3
import cv2
import numpy as np
import json


def lambda_handler(event, context):

    s3 = boto3.client("s3")
    table_name = 'Image'
    dynamoDB = boto3.resource('dynamodb').Table(table_name)

    bucket_name = 'image-tag-bucket'

    labelsPath = "./process_img_tag/coco.names"

    Labels = open(labelsPath).read().strip().split("\n")

    # get image filename from s3
    print("Received event: " + json.dumps(event, indent=2))
    file_object = event["Records"][0]
    file_name = str(file_object['s3']['object']['key'])
    print('filename: ', file_name)

    # Get url from of the images from the s3 bucket
    s3_url = s3.generate_presigned_url(
        'get_object', Params={'Bucket': bucket_name, 'Key': file_name})
    s3_url_final = (s3_url.split("?"))
    print(s3_url_final)

    # converts the image file to numpy array
    fileObj = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = fileObj["Body"].read()
    np_array = np.fromstring(file_content, np.uint8)
    image_np = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
    print("Type is: ", type(image_np))

    # Loads the Yolo algorithm
    net_obj = cv2.dnn.readNet(
        './process_img_tag/yolov3-tiny.weights', './process_img_tag/yolov3-tiny.cfg')

    # Object detection block
    class_ids = []
    accuracy = []

    lnames = net_obj.getLayerNames()
    layers = [lnames[i - 1] for i in net_obj.getUnconnectedOutLayers()]

    blob = cv2.dnn.blobFromImage(
        image_np, 0.00392, (320, 320), (0, 0, 0), True, crop=False)

    net_obj.setInput(blob)

    result_det = net_obj.forward(layers)

    for each_result in result_det:
        for each in each_result:
            scores = each[5:]
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            if confidence > 0.5:
                accuracy.append(float(confidence))
                class_ids.append(class_id)

    # Convert the object list to set to get unique object values
    objects = set()
    length = len(class_ids)
    for i in range(length):
        objects.add(str(Labels[class_ids[i]]))

    # Put the detected objects to the DynamoDB
    data = []

    print(data)
    try:
        item = {
            "url": s3_url_final[0],
            "tags": list(objects)
        }
        dynamoDB.put_item(Item=item)
        return item
    except Exception as e:
        print(e)
        raise

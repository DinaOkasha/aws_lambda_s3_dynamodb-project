import boto3

def lambda_handler(event, context):
    """
    Lambda function to fetch data from S3 CSV file and put it in DynamoDB table.

    Args:
        event (dict): Event data passed to the Lambda function.
        context (object): Lambda context object.

    Returns:
        str: Message indicating success or failure.
    """

    # Replace with your S3 bucket name
    s3_bucket_name = "csv-project-dina"

    # Replace with your DynamoDB table name
    dynamodb_table_name = "Customer"

    try:
        # Get S3 client and DynamoDB resource
        s3_client = boto3.client('s3')
        dynamodb = boto3.resource('dynamodb')

        # Extract bucket name and key (filename) from event (if applicable)
        # Adjust this logic based on your event structure
        bucket_name = event.get('Records', [])[0].get('s3', {}).get('bucket', {}).get('name')
        key = event.get('Records', [])[0].get('s3', {}).get('object', {}).get('key')

        # If bucket and key not provided in event, use configured values
        if not bucket_name:
            bucket_name = s3_bucket_name
        if not key:
            raise ValueError("Missing S3 object key in event")

        # Get the CSV file object from S3
        csv_object = s3_client.get_object(Bucket=bucket_name, Key=key)

        # Read the CSV file content and decode it
        file_content = csv_object['Body'].read().decode("utf-8")

        # Split the content into lines (assuming each line represents a record)
        lines = file_content.splitlines()

        # Filter out empty lines
        lines = list(filter(None, lines))

        # Get the DynamoDB table
        table = dynamodb.Table(dynamodb_table_name)

        # Process each line (record) in the CSV file
        for line in lines:
            # Split the line into a list based on comma separators
            # Adjust this based on your CSV data structure
            data = line.split(",")

            # Create a dictionary with key-value pairs for DynamoDB item
            item = {
                "order_id": data[0],  # Assuming "id" is in the first column (index 0)
                "product": data[1],  # Assuming "product" is in the second column (index 1)
                "quantity": data[2],  # Assuming "quantity" is in the third column (index 2)
                # Add more key-value pairs based on your data structure
            }

            # Put the item into the DynamoDB table
            table.put_item(Item=item)

        return "Data uploaded to DynamoDB table successfully!"

    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return f"Failed to upload data to DynamoDB: {e}"

# Replace these with your actual S3 bucket and DynamoDB table names before deploying the Lambda function


from utils.aws_wrapper import AWSDynamoDB
'''
def test_dynamodb_list_tables():
    aws_object = AWSDynamoDB()
    tables = aws_object.dynamodb.list_tables()
    # printing the list of table to DEBUG only
    print( tables )
    assert len(tables) > 0

def test_dynamodb_table_exist_success():
    aws_object = AWSDynamoDB()
    table_name = 'ddm_client_trigger_config'
    response = aws_object.table_exists(table_name)
    assert response == True

def test_dynamodb_table_exist_error():
    aws_object = AWSDynamoDB()
    table_name = 'table_does_not_exists'
    response = aws_object.table_exists(table_name)
    assert response == False

def test_dynamodb_table_info():
    aws_object = AWSDynamoDB()
    table_name = 'ddm_client_trigger_config'
    response = aws_object.table_info(table_name)
    print(response)
    assert table_name == response['Table']['TableName']
'''
def test_dynamodb_waterfall_query():
    ddb = AWSDynamoDB()
    table_name = 'ddm_client_trigger_waterfall'
    config_id = 'dollar-consumer_trigger_E2E_Tx'
    campaign_wednesday ='20250622'
    response = ddb.get_query_logs(
        table_name,
        'config_id',
        config_id,
        'run_dt',
        campaign_wednesday
    )

    cols = ['config_id', 'detail', 'qty', 'run_dt', 'timestamp', 'is_active', 'high', 'waterfall_trace_id']
    col_format = "{:<30} {:<30} {:>10} {:^8} {:^30} {:^8} {:>10} {:>25}"
    result = ['\n']
    result.append(col_format.format(*cols))
    for o in response:
        o['qty'] = str(o['qty']).replace("Decimal(", "").replace(")", "")
        o['detail'] = 'None' if o['detail'] is None else o['detail']
        values = [v for v in o.values()]
        result.append(col_format.format(*values))

    print('\n\t'.join(result))
    assert response is not None

def test_dynamodb_steps_query():
    ddb = AWSDynamoDB()
    table_name = 'ddm_client_trigger_step'
    config_id = 'dollar-consumer_trigger_E2E_Tx'
    campaign_wednesday ='20250622'
    response = ddb.get_query_logs(
        table_name,
        'config_id',
        config_id,
        'run_dt',
        campaign_wednesday
    )

    result = ['\n']
    cols = ['message_type', 'config_id', 'message', 'step_trace_id', 'run_dt', 'timestamp']
    col_format = "{:<15}  {:<30}  {:<80}  {:<40}  {:^8} {:<30}"
    result.append(col_format.format(*cols))
    for o in response:
        values = [v for v in o.values()]
        result.append(col_format.format(*values))

    print('\n'.join(result))

    assert response is not None
#


'''
Sample Table Response from DynamoDB API
=======================================
Table
 - AttributeDefinitions
 - TableName
 - KeySchema: [{'AttributeName': 'config_id', 'KeyType': 'HASH'}]
 - TableStatus
 - CreationDateTime
 - ProvisionedThroughput
 - TableSizeBytes
 - ItemCount
 - TableArn # arn:aws:dynamodb:us-east-1:590407490654:table/ddm_client_trigger_config
 - TableId  # 1c9b8ebd-f786-48b6-9195-a18f1d5981f0
'''

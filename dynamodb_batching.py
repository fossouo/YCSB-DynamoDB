# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose
Shows how to use the AWS SDK for Python (Boto3) to write and retrieve Amazon DynamoDB
data using batch functions.
Boto3 features a `batch_writer` function that handles all of the necessary intricacies
of the Amazon DynamoDB batch writing API on your behalf. This includes buffering,
removing duplicates, and retrying unprocessed items.
"""

import decimal
import json
import logging
import os
import pprint
import time
import boto3
import timeit
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
dynamodb = boto3.resource('dynamodb')

MAX_GET_SIZE = 100  # Amazon DynamoDB rejects a get batch larger than 100 items.


def create_table(table_name, schema):
    """
    Creates an Amazon DynamoDB table with the specified schema.
    :param table_name: The name of the table.
    :param schema: The schema of the table. The schema defines the format
                   of the keys that identify items in the table.
    :return: The newly created table.
    """
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{
                'AttributeName': item['name'], 'KeyType': item['key_type']
            } for item in schema],
            AttributeDefinitions=[{
                'AttributeName': item['name'], 'AttributeType': item['type']
            } for item in schema],
            ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
        )
        table.wait_until_exists()
        logger.info("Created table %s.", table.name)
    except ClientError:
        logger.exception("Couldn't create TimeSeries table.")
        raise
    else:
        return table


def do_batch_get(batch_keys):
    """
    Gets a batch of items from Amazon DynamoDB. Batches can contain keys from
    more than one table.
    When Amazon DynamoDB cannot process all items in a batch, a set of unprocessed
    keys is returned. This function uses an exponential backoff algorithm to retry
    getting the unprocessed keys until all are retrieved or the specified
    number of tries is reached.
    :param batch_keys: The set of keys to retrieve. A batch can contain at most 100
                       keys. Otherwise, Amazon DynamoDB returns an error.
    :return: The dictionary of retrieved items grouped under their respective
             table names.
    """
    tries = 0
    max_tries = 5
    sleepy_time = 1  # Start with 1 second of sleep, then exponentially increase.
    retrieved = {key: [] for key in batch_keys}
    while tries < max_tries:
        response = dynamodb.batch_get_item(RequestItems=batch_keys)
        # Collect any retrieved items and retry unprocessed keys.
        for key in response.get('Responses', []):
            retrieved[key] += response['Responses'][key]
        unprocessed = response['UnprocessedKeys']
        if len(unprocessed) > 0:
            batch_keys = unprocessed
            unprocessed_count = sum(
                [len(batch_key['Keys']) for batch_key in batch_keys.values()])
            logger.info(
                "%s unprocessed keys returned. Sleep, then retry.",
                unprocessed_count)
            tries += 1
            if tries < max_tries:
                logger.info("Sleeping for %s seconds.", sleepy_time)
                time.sleep(sleepy_time)
                sleepy_time = min(sleepy_time * 2, 32)
        else:
            break

    return retrieved


def fill_table(table, table_data):
    """
    Fills an Amazon DynamoDB table with the specified data, using the Boto3
    Table.batch_writer() function to put the items in the table.
    Inside the context manager, Table.batch_writer builds a list of
    requests. On exiting the context manager, Table.batch_writer starts sending
    batches of write requests to Amazon DynamoDB and automatically
    handles chunking, buffering, and retrying.
    :param table: The table to fill.
    :param table_data: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
    """
    print(table_data)
    print("******** Table schema Begin****************")
    print(table)
    print("******** Table schema End****************")
    print(table.name)
    try:
        with table.batch_writer() as writer:
            for item in table_data:
                writer.put_item(Item=item)
                print(item)
        logger.info("Loaded data into table %s.", table.name)
    except ClientError:
        logger.exception("Couldn't load data into table %s.", table.name)
        raise


def get_batch_data(trade_table, trade_list, entity_table, entity_list, aspect_table, aspect_list):
    """
    Gets data from the specified trade and trade tables. Data is retrieved in batches.
    :param trade_table: The table from which to retrieve trade data.
    :param trade_list: A list of keys that identify trades to retrieve.
    :param entity_table: The table from which to retrieve entity data.
    :param entity_list: A list of keys that identify entity to retrieve.

    :return: The dictionary of retrieved items grouped under the respective
             trade and actor table names.
    """
    batch_keys = {
        trade_table.name: {
            'Keys': [{'notional': trade[5]} for trade in trade_list]
        },
        entity_table.name: {
            'Keys': [{'float_value': entity[6]} for entity in entity_list]
        },
        aspect_table.name: {
            'Keys': [{'float_value': aspect[6]} for aspect in aspect_list]
        }

    }
    try:
        retrieved = do_batch_get(batch_keys)
        for response_table, response_items in retrieved.items():
            logger.info("Got %s items from %s.", len(response_items), response_table)
    except ClientError:
        logger.exception(
            "Couldn't get items from %s and %s.", trade_table.name, entity_table.name, aspect_table.name)
        raise
    else:
        return retrieved


def archive_trades(trade_table, trade_data):
    """
    Archives a list of trades to a newly created archive table and then deletes the
    trades from the original table.
    Uses the Boto3 Table.batch_writer() function to handle putting items into the
    archive table and deleting them from the original table. Shows how to configure
    the batch_writer to ensure there are no duplicates in the batch. If a batch
    contains duplicates, Amazon DynamoDB rejects the request and returns a
    ValidationException.
    :param trade_table: The table that contains trade data.
    :param trade_data: The list of keys that identify the trades to archive.
    :return: The newly created archive table.
    """
    try:
        # Copy the schema and attribute definition from the original trade table to
        # create the archive table.
        archive_table = dynamodb.create_table(
            TableName=f'{trade_table.name}-archive',
            KeySchema=trade_table.key_schema,
            AttributeDefinitions=trade_table.attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits':
                    trade_table.provisioned_throughput['ReadCapacityUnits'],
                'WriteCapacityUnits':
                    trade_table.provisioned_throughput['WriteCapacityUnits']
            })
        logger.info("Table %s created, wait until exists.", archive_table.name)
        archive_table.wait_until_exists()
    except ClientError:
        logger.exception("Couldn't create archive table for %s.", trade_table.name)
        raise

    try:
        # When the list of items in the batch contains duplicates, Amazon DynamoDB
        # rejects the request. By default, the batch_writer keeps duplicates.
        with archive_table.batch_writer() as archive_writer:
            for item in trade_data:
                archive_writer.put_item(Item=item)
        logger.info("Put trades into %s.", archive_table.name)
    except ClientError as error:
        if error.response['Error']['Code'] == 'ValidationException':
            logger.info(
                "Got expected exception when trying to put duplicate records into the "
                "archive table.")
        else:
            logger.exception(
                "Got unexpected exception when trying to put duplicate records into "
                "the archive table.")
            raise

    try:
        # When `overwrite_by_pkeys` is specified, the batch_writer overwrites any
        # duplicate in the batch with the new item.
        with archive_table.batch_writer() as archive_writer:
            for item in trade_data:
                archive_writer.put_item(Item=item)
        logger.info("Put trades into %s.", archive_table.name)
    except ClientError:
        logger.exception(
            "Couldn't put trades into %s.", archive_table.name)
        raise

    try:
        with trade_table.batch_writer() as trade_writer:
            for item in trade_data:
                trade_writer.delete_item(
                    Key={'notional': item['notional']})
        logger.info("Deleted trades from %s.", trade_table.name)
    except ClientError:
        logger.exception(
            "Couldn't delete trades from %s.", trade_table.name)
        raise

    return archive_table


def usage_demo():
    """
    Shows how to use the Amazon DynamoDB batch functions.
    """
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print('-'*88)
    print("Welcome to the Amazon DynamoDB batch usage demo.")
    print('-'*88)

    trades_file_name = 'ead.json'
    print(f"Getting trade data from {trades_file_name}.")
    try:
        with open(trades_file_name) as json_file:

            trade_data = json.load(json_file, parse_float=decimal.Decimal)

            trade_entity_data = trade_data["vegas"]

            trade_entity_data = trade_entity_data[:500]  # Only use the first 500 trades for the demo.

            #print(trade_entity_data)
            #print(trade_entity_data[0])

    except FileNotFoundError:
        print(f"The file tradedata.json was not found in the current working directory "
              f"{os.getcwd()}.\n"
              f"1. Download the zip file from https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/tradedata.zip.\n"
              f"2. Extract '{trades_file_name}' to {os.getcwd()}.\n"
              f"3. Run the usage demo again.")
        return

    # Build a second table centered around actors.

    deltas_schema = [
        {'name': 'address_hash', 'key_type': 'HASH', 'type': 'S'},
        {'name': 'valid_time', 'key_type': 'RANGE', 'type': 'S'}
    ]
    gammas_schema = [
        {'name': 'address_hash', 'key_type': 'HASH', 'type': 'S'},
        {'name': 'valid_time', 'key_type': 'RANGE', 'type': 'S'}
    ]
    thetas_schema = [
        {'name': 'address_hash', 'key_type': 'HASH', 'type': 'S'},
        {'name': 'valid_time', 'key_type': 'RANGE', 'type': 'S'}
    ]
    trades_schema = [
        {'name': 'address_hash', 'key_type': 'HASH', 'type': 'S'},
        {'name': 'valid_time', 'key_type': 'RANGE', 'type': 'S'}
    ]
    vegas_schema = [
        {'name': 'address_hash', 'key_type': 'HASH', 'type': 'S'},
        {'name': 'valid_time', 'key_type': 'RANGE', 'type': 'S'}
    ]

    trade_schema = [
        {'name': 'notional', 'key_type': 'HASH', 'type': 'S'}
    ]
    entity_schema = [
        {'name': 'float_value', 'key_type': 'HASH', 'type': 'S'}
    ]
    aspect_schema = [
        {'name': 'float_value', 'key_type': 'HASH', 'type': 'S'}
    ]

    print(f"Creating trade and entity tables and waiting until they exist...")
    
    deltas_table = create_table(f'demo-batch-deltas-{time.time_ns()}', deltas_schema)
    gammas_table = create_table(f'demo-batch-gammas-{time.time_ns()}', gammas_schema)
    vegas_table = create_table(f'demo-batch-vegas-{time.time_ns()}', vegas_schema)
    thetas_table = create_table(f'demo-batch-theta-{time.time_ns()}', thetas_schema)
    trades_table = create_table(f'demo-batch-trades-{time.time_ns()}', trades_schema)

    trade_table = create_table(f'demo-batch-trades-{time.time_ns()}', trade_schema)
    entity_table = create_table(f'demo-batch-entity-{time.time_ns()}', entity_schema)
    aspect_table = create_table(f'demo-batch-entity-{time.time_ns()}', aspect_schema)

    print(f"Created {trade_table.name} and {entity_table.name}.")

    print(f"Putting {len(trade_data)} trades into {trade_table.name}.")

    print("Inserting vegas")

    deltas_data = trade_data["deltas"]
    vegas_data = trade_data["vegas"]
    trades_data = trade_data["trades"]
    thetas_data = trade_data["thetas"]
    gammas_data = trade_data["gammas"]

    entity_data = trade_data["vegas"]
    aspect_data = trade_data["thetas"]
    
    ##fill_table(trade_table, trade_data["trades"])
    print("end vegas insert")

    print(f"Putting {len(entity_data)} entities into {entity_table.name}.")

    start = timeit.default_timer()

    fill_table(deltas_table, deltas_data)
    fill_table(vegas_table, vegas_data)
    fill_table(trades_table, trades_data)
    fill_table(thetas_table, thetas_data)
    fill_table(gammas_table, gammas_data)

    
    #fill_table(aspect_table, aspect_data)



    stop = timeit.default_timer()
    execution_time = stop - start

    print("Program Executed in "+str(execution_time)) # It returns time in seconds


    trade_list = [(trade['notional'])
                  for trade in trade_data[0:int(MAX_GET_SIZE/2)]]
    entity_list = [(entity['float_value'])
                  for entity in entity_data[0:int(MAX_GET_SIZE/2)]]
    aspect_list = [(aspect['float_value'])
                  for aspect in aspect_data[0:int(MAX_GET_SIZE/2)]]            
    items = get_batch_data(trade_table, trade_list, entity_table, entity_list, aspect_table, aspect_list)
    print(f"Got {len(items[trade_table.name])} trades from {trade_table.name}\n"
          f"and {len(items[entity_table.name])} actors from {entity_table.name}.")
    print("The first 2 trades returned are: ")
    #pprint.pprint(items[trade_table.name][:2])
    print(f"The first 2 actors returned are: ")
    #pprint.pprint(items[entity_table.name][:2])

    print(
        "Archiving the first 10 trades by creating a table to store archived "
        "trades and deleting them from the main trade table.")
    # Duplicate the trades in the list to demonstrate how the batch writer can be
    # configured to remove duplicate requests from the batch.
    trade_list = trade_data[0:10] + trade_data[0:10]

    archive_table = archive_trades(trade_table, trade_list, trade_list)
    print(f"trades successfully archived to {archive_table.name}.")

    #archive_table.delete()
    #trade_table.delete()
    #actor_table.delete()
    print(f"Deleted {trade_table.name}, {archive_table.name}, and {entity_table.name}.")
    print("Thanks for watching!")


if __name__ == '__main__':
    usage_demo()

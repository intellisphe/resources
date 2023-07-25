import os
import time
from datetime import datetime, timedelta
import csv
import pandas as pd

from sphereapi import SphereApi
from s3export import S3Export
from influxexport import InfluxExport

def create_dataframe(data, column_names):
    df = pd.DataFrame(data['data'], columns=column_names)
    return df

def write_to_file(query_execution_id, data, column_names):
    column_names2d = [column_names] # convert to 2d
    data = column_names2d + data

    with open(f"{query_execution_id}.csv","w+") as csv_file:
        csvWriter = csv.writer(csv_file, delimiter=',')
        csvWriter.writerows(data)
    
    print(f'File name {query_execution_id}.csv')

def getData(api, timestamp_start, timestamp_end):
    query_result = api.query_start(timestamp_start, timestamp_end)

    query_execution_id = query_result['query_execution_id']
    print(f"Query execution id: {query_execution_id}")
    
    next_token = query_result['next_token'] if 'next_token' in query_result else None

    data = query_result['data']

    count = len(data)
    print(count)
    all_data = data
    while next_token:
        result = api.query_data(
            query_execution_id=query_execution_id,
            next_token=next_token)
        
        data = result['data']
        count = count + len(data)
        print(count)
        all_data += data
        next_token = result['next_token'] if 'next_token' in result else None
    
    return (all_data, query_execution_id, count)

def getDataAsync(api, timestamp_start, timestamp_end):

    # start the query
    query_result = api.query_start_async(timestamp_start, timestamp_end)
    query_execution_id = query_result['query_execution_id']

    print(f"Query execution id: {query_execution_id}")
    
    # Wait for 5 seconds for the query to complete
    time.sleep(5)

    # Get execution status
    for i in range(1, 11):
        # get query execution
        query_status = api.query_status(query_execution_id=query_execution_id)
        query_execution_status = query_status['status']
        
        print(f"STATUS: {query_execution_status}")

        if query_execution_status == 'SUCCEEDED':    
            break
        elif query_execution_status == 'FAILED':
            raise Exception(f"STATUS: {query_execution_status}")
        else:
            time.sleep(i)
    else:
        api.query_stop(query_execution_id=query_execution_id)
        raise Exception('TIME OVER')

    count = 0
    all_data = []
    next_token = None
    # Get the data
    while True:
        result = api.query_data(
            query_execution_id=query_execution_id,
            next_token=next_token)
        
        data = result['data']
        count = count + len(data)
        print(count)
        all_data += data
        next_token = result['next_token'] if 'next_token' in result else None        
        
        # If no more data exit the loop
        if not next_token:
            break
    
    return (all_data, query_execution_id, count)

def main():

    # Get current datetime
    now = datetime.now()
    # Get start of the current hour
    timestamp_end = now.replace(minute=0, second=0, microsecond=0)
    
    # subtract X and Y hours respectively
    # we want to query hours X to Y from current time
    timestamp_start = timestamp_end + timedelta(hours=-56)
    timestamp_middle = timestamp_end + timedelta(hours=-51)
    timestamp_end = timestamp_end + timedelta(hours=-48)
    
    print(f'Start time {timestamp_start}')
    print(f'Middle time {timestamp_middle}')
    print(f'End time {timestamp_end}')
    
    # Convert to timestamp
    timestamp_start = time.mktime(timestamp_start.timetuple())
    timestamp_middle = time.mktime(timestamp_middle.timetuple())
    timestamp_end = time.mktime(timestamp_end.timetuple())
    
    timestamp_msec_start = int(timestamp_start * 1e3)
    timestamp_msec_middle = int(timestamp_middle * 1e3)
    timestamp_msec_end = int(timestamp_end * 1e3)

    api = SphereApi()

    # Get the schema
    schema = api.schema()
    column_names = [col['name'] for col in schema['table']['columns']]

    # Get the ad sizes for video
    adsize = api.adsize('video')
    print(adsize)
    
    # Test 1: Data returned by ASYNC and SYNCHRONOUS apis must be same

    (dataAsync, query_execution_id_async, countAsync) = getDataAsync(
        api=api,
        timestamp_start=timestamp_msec_start,
        timestamp_end=timestamp_msec_end)
    
    # Sort by timestamp
    dataAsync = sorted(dataAsync, key=lambda x: x[1], reverse=False)

    write_to_file(
        query_execution_id=query_execution_id_async,
        data=dataAsync,
        column_names=column_names)

    (data, query_execution_id, count) = getData(
        api=api,
        timestamp_start=timestamp_msec_start,
        timestamp_end=timestamp_msec_end)

    # Sort by timestamp
    data = sorted(data, key=lambda x: x[1], reverse=False)

    write_to_file(
        query_execution_id=query_execution_id,
        data=data,
        column_names=column_names)

    if countAsync != count:
        print(f'Data count from the async and synchronous apis are not same. Sync {count} and asyc {countAsync}')
    else:
        print(f'Data count from the async and synchronous apis are same {countAsync}.')

    if dataAsync != data:
        print(f'Sync and async data contents are not same.')
    else:
        print('Data content from the async and synchronous apis are same.')
        
    # Test 2: Data returned for timestamp between A and C, must be equal to the total of A and B + B and C
    (dataAsyncAB, query_execution_idAB, countAsyncAB) = getDataAsync(
        api=api,
        timestamp_start=timestamp_msec_start,
        timestamp_end=timestamp_msec_middle)

    write_to_file(
        query_execution_id=query_execution_idAB,
        data=dataAsyncAB,
        column_names=column_names)

    (dataAsyncBC, query_execution_idBC, countAsyncBC) = getDataAsync(
        api=api,
        timestamp_start=timestamp_msec_middle,
        timestamp_end=timestamp_msec_end)

    write_to_file(
        query_execution_id=query_execution_idBC,
        data=dataAsyncBC,
        column_names=column_names)
        
    if countAsyncAB+countAsyncBC != countAsync:
        print(f'Data count from the intermediate timestamp are not same. AB {countAsyncAB} + BC {countAsyncBC} != AC {countAsync}')
    else:
        print(f'Data count from the intermediate timestamp are same {countAsync}.')

    # Combine
    dataAsyncAC = dataAsyncAB + dataAsyncBC
    # Sort by timestamp
    dataAsyncAC = sorted(dataAsyncAC, key=lambda x: x[1], reverse=False)

    write_to_file(
        query_execution_id=f'{query_execution_idAB}-{query_execution_idBC}',
        data=dataAsyncAC,
        column_names=column_names)

    if dataAsync != dataAsyncAC:
        print(f'Sum of data contents from intermediate timestamps and contents from the entire time are not same.')
    else:
        print('Data content from the intermediate timestamps are same.')

    file_path = os.path.join(os.getcwd(), f'{query_execution_id_async}.csv')
    s3 = S3Export()
    s3.export(file_path)

    df = create_dataframe(data=dataAsync, column_names=column_names)
    influx = InfluxExport()
    influx.export(data=df, schema=schema)

if __name__ == '__main__':
    main()
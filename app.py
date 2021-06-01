import pandas as pd
import time
from random import randrange

# Define Data Frames
dataset = pd.DataFrame(columns=['user_id', 'timestamp', 'heart_rate', 'respiration_rate', 'activity'])
minuteframe = pd.DataFrame(columns=['user_id', 'seg_start', 'seg_end', 'avg_hr', 'min_hr', 'max_hr', 'avg_rr'])
hourframe = pd.DataFrame(columns=['user_id', 'seg_start', 'seg_end', 'avg_hr', 'min_hr', 'max_hr', 'avg_rr'])


def set_data(user_id, timestamp, data_values=None):
    """
    Set Data in JSON format
    :param user_id: User Id of the user
    :param timestamp: Timestamp at which teh machine sends the data
    :param data_values: Data values of various health parameters
    :return:
    """
    data = {
        'user_id': user_id,
        'timestamp': timestamp,
        'heart_rate': data_values['heart_rate'],
        'respiration_rate': data_values['respiration_rate'],
        'activity': data_values['activity']
    }
    return data


def simulate(user_id):
    """
    Simulate the data for 7200 seconds
    :param user_id: User Id of the user
    """
    print('Simulation started')
    timestamp = int(time.time())
    # Iterate and generate random data values for 7200 seconds
    for i in range(0, 2 * 60 * 60):
        data_values = {
            'heart_rate': randrange(40, 120),
            'respiration_rate': randrange(12, 30),
            'activity': randrange(0, 10)
        }
        print('At second ' + str(i))
        data = set_data(user_id, timestamp + i, data_values)
        processor(data)
        # time.sleep(1)
    print('Simulation completed')
    dataset.to_json('dataset.json', orient='records')
    minuteframe.to_csv('minuteframe.csv')
    print('Data Files saved')
    print('Hourly process started')
    process_hourly()
    print('Hourly process completed')
    minuteframe.to_csv('hourframe.csv')


def processor(data):
    """
    Process Data values and update Minute frame data in real-time
    :param data: Data values provided by user
    """
    global dataset, minuteframe
    dataset = dataset.append(data, ignore_index=True)
    seg_start_timestamp = data['timestamp'] - (data['timestamp'] % (15 * 60))
    seg_end_timestamp = data['timestamp'] + (15 * 60) - 1

    # Get data from Data set by filtering using 15-min timestamps
    dataset_filter = ((dataset['timestamp'] >= seg_start_timestamp) & (dataset['timestamp'] < seg_end_timestamp)
                      & (dataset['user_id'] == data['user_id']))
    datapoint = dataset.loc[dataset_filter]

    # Append if not exists, create otherwise
    minuteframe_filter = (minuteframe['seg_start'] == seg_start_timestamp)
    if minuteframe[minuteframe_filter].empty:
        minuteframe = minuteframe.append({
            'user_id': data['user_id'],
            'seg_start': seg_start_timestamp,
            'seg_end': seg_end_timestamp,
            'avg_hr': round(datapoint['heart_rate'].mean()),
            'min_hr': datapoint['heart_rate'].min(),
            'max_hr': datapoint['heart_rate'].max(),
            'avg_rr': round(datapoint['respiration_rate'].mean())
        }, ignore_index=True)
    else:
        minuteframe.loc[minuteframe_filter, ['avg_hr', 'min_hr', 'max_hr', 'avg_rr']] = [
            round(datapoint['heart_rate'].mean()),
            datapoint['heart_rate'].min(),
            datapoint['heart_rate'].max(),
            round(datapoint['respiration_rate'].mean())
        ]


def process_hourly():
    """
    Process Minute frame data and create Hour frame data
    """
    global hourframe
    for index, data in minuteframe.iterrows():
        seg_start_timestamp = data['seg_start'] - (data['seg_start'] % (60 * 60))
        seg_end_timestamp = data['seg_start'] + (60 * 60) - 1

        # Create Hour Frame if it does not exists
        hourframe_filter = (hourframe['seg_start'] == seg_start_timestamp)
        if hourframe[hourframe_filter].empty:
            # Get data from Minute Frame by filtering using hourly timestamps
            minuteset_filter = ((minuteframe['seg_start'] >= seg_start_timestamp)
                                & (minuteframe['seg_start'] < seg_end_timestamp)
                                & (minuteframe['user_id'] == data['user_id']))
            minuteset = minuteframe.loc[minuteset_filter]

            hourframe = hourframe.append({
                'user_id': data['user_id'],
                'seg_start': seg_start_timestamp,
                'seg_end': seg_end_timestamp,
                'avg_hr': round(minuteset['avg_hr'].mean()),
                'min_hr': minuteset['min_hr'].min(),
                'max_hr': minuteset['max_hr'].max(),
                'avg_rr': round(minuteset['avg_rr'].mean())
            }, ignore_index=True)


simulate('akansh')
print('Glimpses of Data set----------------------')
print(dataset.head())
print('Glimpses of Minute set----------------------')
print(minuteframe.head())
print('Glimpses of Hour set----------------------')
print(hourframe.head())

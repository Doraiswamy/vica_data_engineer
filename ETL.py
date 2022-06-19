import os
import json
import pandas as pd
from datetime import datetime

def extract(file_to_process):
    with open(file_to_process) as json_file:
        data = json.load(json_file)
        df = pd.DataFrame(data)
    return df


def transform(data, file):
    head, tail = os.path.split(file)
    if tail == 'apps.json':
        for index, row in data.iterrows():
            row['_id'] = row['_id']['$oid']
    elif tail == 'sessions.json':
        for index, row in data.iterrows():
            row['_id'] = row['_id']['$oid']
            row['app'] = row['app']['$oid']
            row['createdAt'] = row['createdAt']['$date']
            last_three_digits = int(str(row['createdAt'])[-3:])
            row['createdAt'] = int(str(row['createdAt'])[:-3])
            row['createdAt'] = datetime.utcfromtimestamp(row['createdAt']).strftime(
                '%Y-%m-%d %H:%M:%S.'+str(last_three_digits))
            row['createdAt'] = datetime.strptime(row['createdAt'], '%Y-%m-%d %H:%M:%S.%f')
    else:
        for index, row in data.iterrows():
            row['_id'] = row['_id']['$oid']
            row['session'] = row['session']['$oid']
            row['createdAt'] = row['createdAt']['$date']
            last_three_digits = int(str(row['createdAt'])[-3:])
            row['createdAt'] = int(str(row['createdAt'])[:-3])
            row['createdAt'] = datetime.utcfromtimestamp(row['createdAt']).strftime(
                '%Y-%m-%d %H:%M:%S.' + str(last_three_digits))
            row['createdAt'] = datetime.strptime(row['createdAt'], '%Y-%m-%d %H:%M:%S.%f')
    return data


def load(targetfile, df_cd_final):
    df_cd_final.to_csv(targetfile)




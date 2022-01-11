# -*- utf-8 -*-
import re
import pandas as pd
from copy import deepcopy
import json
import os


def flatten_json(y):
    out = [y]

    def flatten(x, result, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], result, name + a + '_')
        elif type(x) is list:
            result[str(name[:-1])] = x
        else:
            result[str(name[:-1])] = str(x)

    def find_list_key(x):
        for k in x:
            if type(x[k]) is list:
                return k

    done = False
    while not done:
        done = True
        for i in range(len(out)):
            row = out[i]
            result = {}
            flatten(row, result)
            out[i] = result
        for i in reversed(range(len(out))):
            row = out[i]
            list_key = find_list_key(row)
            if list_key is not None:
                done = False
                list_value = row.pop(list_key)
                for item in list_value:
                    tmp = deepcopy(row)
                    tmp[list_key] = item
                    out.append(tmp)
                del out[i]

    return out


df = 0


def logRegex(rule, lin):
    '''
    Process each line by regular expression, get the timestamp and the result after json tiling
    '''
    global df
    groups = re.search(rule, lin)
    if groups:
        time = groups.group(1)
        textBody = groups.group(2)
        msg = json.loads(groups.group(3))
        flat = flatten_json(msg)
        df = pd.json_normalize(flat)
        df['time'] = time
    return df


def run(file_dir):
    '''
    Traversing and cleaning log files
    '''
    files = os.listdir(file_dir)
    new_df = []
    rule = r"(\d{4,4}-\d{2,2}-\d{2,2} \d{2,2}:\d{2,2}:\d{2,2}\.\d{6,6})(.*create_mission)(.*},\"msgType\":888.*)"
    strings = ('"msgType":888', '"msgType":999')
    for file_name in files:
        with open(file_dir + '/' + file_name, encoding='utf-8') as data:
            for line in data:
                if any(s in line for s in strings):
                    if 'create_mission' in line:
                        df1 = logRegex(rule, line)
                        new_df.append(df1)
                    else:
                        pass

                else:
                    pass
    df_all = pd.concat(new_df)


if __name__ == '__main__':
    logDir = './data'
    run(logDir)

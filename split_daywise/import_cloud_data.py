from typing import List
from datetime import datetime, timedelta
import pytz
import os
import numpy as np
from domain.datapoint import DataPoint
import bz2
import datetime
import uuid
from typing import List
from uuid import UUID

from datetime import datetime
from typing import Any
import sys
import json
import codecs
import gzip

tz = pytz.timezone('US/Central')
print(tz)

def get_fileName(cur_dir, file_sufix):
    filenames = [name for name in os.listdir(cur_dir) if
                 name.endswith(file_sufix)]
    return filenames

def convert_sample(sample):
    return list([float(x.strip()) for x in sample.split(',')])

def line_parser(input):
    ts, offset, sample = input.split(',', 2)
    start_time = int(ts) / 1000.0
    offset = int(offset)
    return DataPoint(start_time=datetime.fromtimestamp(start_time, tz), offset=offset, sample=convert_sample(sample))

def load_datapointarray(cur_dir, file_sufix, file_type = '.csv.bz2'):
    filenames = get_fileName(cur_dir, file_sufix + file_type)
    data = []
    for filename in filenames:
        if file_type.endswith('bz2'):
            fp = bz2.BZ2File(cur_dir + filename)
            file_content = fp.read()
            fp.close()
            file_content = file_content.decode('utf-8')
        elif file_type.endswith('csv'):
            fp = open(cur_dir + filename)
            file_content = fp.read()
            fp.close()


        lines = file_content.splitlines()
        dt = list(map(line_parser, lines))
        data.extend(dt)

    return data


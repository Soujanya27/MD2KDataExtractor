import gzip
import json
from typing import Any
import os

from from_phone_data_to_cloudcsv.datasource_to_streamname_mapping import datasourceID_to_name


class DataPoint:
    def __init__(self,
                 start_time: int = None,
                 sample: Any = None):
        """
        DataPoint is the lowest data representations entity in CerebralCortex.
        :param start_time:
        :param end_time:
        :param offset: in milliseconds
        :param sample:
        """
        self._start_time = start_time
        self._sample = sample

    @property
    def sample(self):
        return self._sample

    @sample.setter
    def sample(self, val):
        self._sample = val

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    def getKey(self):
        return self._start_time

    def __eq__(self, dp):
        return self._start_time == dp.start_time

    def __hash__(self):
        return hash(('start_time', self.start_time))


def row_to_datapoint(row: str) -> DataPoint:
    """
    Format data based on mCerebrum's current GZ-CSV format into what Cerebral
    Cortex expects
    :param row:
    :return: single DataPoint
    :rtype: DataPoint
    """
    ts, offset, values = row.split(',', 2)
    ts = int(ts)
    return DataPoint(start_time=ts, sample=row)


def get_gzip_file_contents(filepath: str) -> str:
    """
    Read and return gzip compressed file contents
    :param filepath:
    :return: gzip_file_content
    :rtype: str
    """
    try:
        print(filepath)
        fp = gzip.open(filepath)
        gzip_file_content = fp.read()
        fp.close()
        gzip_file_content = gzip_file_content.decode('utf-8')
        return gzip_file_content
    except:
        return None

def zipfile_to_datapoint(zip_filepath, filename):
    gzip_file_content = get_gzip_file_contents(zip_filepath + filename)
    datapoints = []
    if gzip_file_content is not None:
        datapoints = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
    return datapoints

def get_input_datapoints_and_save_as_csv(filepath, output_filename):

    print('-------------', output_filename)

    zipfiles = [d for d in os.listdir(filepath) if d.endswith('csv.gz')]
    datapoints = []
    for zf in zipfiles:
        if 'corrupt' in zf:
            continue
        # if '2018062713_19_archive' in filepath:
        #     continue
        datapoints.extend(zipfile_to_datapoint(filepath, zf))
    print(len(datapoints))


    datapoints.sort(key=lambda x: x.start_time)

    data = [v.sample for v in datapoints]

    with open(output_filename,'w') as file:
        for line in data:
            file.write(line)
            file.write('\n')

    return datapoints

if __name__ == '__main__':

    basedir = '/home/nsleheen/Data/RicePhoneBackUp/'

    is_for_all_participants = True

    if is_for_all_participants == True:
        pids = [d for d in os.listdir(basedir) if len(d) == 4]
        pids.sort()
    else:
        # run only for these participants
        pids = [ '3060', '3062', '3063', '3064', '3098']
    print(pids)

    for pid in pids:

        output_base_dir = basedir +  'daywise_data/'
        output_dir = output_base_dir + pid

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        # data_dir = basedir + pid + '/org.md2k.datakit/files/raw/'
        data_dir = basedir + pid + '/files/raw/'


        M = datasourceID_to_name(pid, basedir)

        raw_files = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

        for raw_file in raw_files:
            print(raw_file)
            ds_id = raw_file[3:]
            filename = M[ds_id]
            print(ds_id, filename)
            get_input_datapoints_and_save_as_csv(data_dir + raw_file + '/', output_dir + '/' + pid + '+' + filename + '.csv')







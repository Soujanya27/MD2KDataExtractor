import gzip
import json
from typing import Any
import os


def get_datastream_name_from_json(file_content):
    dt = json.loads(file_content)

    if 'dataSource' in  dt:
        platform = 'None'
        ds_dt = dt['dataSource']
        app_id = ds_dt['application']['id']
        ds_id = dt['ds_id']
        if 'platform' in ds_dt:
            if 'type' in ds_dt['platform']:
                platform = ds_dt['platform']['type']
            if 'id' in ds_dt['platform']:
                platform = platform + '+' + ds_dt['platform']['id']
        type = ds_dt['type']
        filename = str(ds_id) + '+'+app_id+ '+'+type + '+' + platform
        print(app_id, ds_id, platform, type)
        print(filename)

        return filename
    return 'data'

def datasourceID_to_name(pid, basedir):
    # basedir = '/run/user/1008/gvfs/smb-share:server=md2k_lab.local,share=md2k_lab_share/Data/ROBAS/'

    # data_dir = basedir + pid + '/org.md2k.datakit/files/cerebralcortex/'
    data_dir = basedir + pid + '/files/cerebralcortex/'

    ds_files = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

    M = dict()
    for dsf in ds_files:

        jsonfile = [d for d in os.listdir(data_dir + dsf) if d.endswith('json.gz')]
        fn = data_dir + dsf + '/' + str(jsonfile[0])
        # print(dsf, jsonfile[0])

        fp = gzip.open(data_dir + dsf + '/' + str(jsonfile[0]))

        gzip_file_content = fp.read()
        fp.close()
        gzip_file_content = gzip_file_content.decode('utf-8')

        fname = get_datastream_name_from_json(gzip_file_content)
        if fname != 'data':
            M[dsf[2:]] = fname

        # print(dsf, dsf[2:])

    print(M)
    return M





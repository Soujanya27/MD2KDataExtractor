import os
from split_daywise.import_cloud_data import *
from split_daywise.export_streamprocessor import *
import bz2
import time

data_dir = '/run/user/1008/gvfs/smb-share:server=md2k_lab.local,share=md2k_lab_share/Data/Rice/'
output_data_dir = '/run/user/1008/gvfs/smb-share:server=md2k_lab.local,share=md2k_lab_share/Data/Rice/EMA_DQ_PuffMarker/'
output_raw_data_dir = '/run/user/1008/gvfs/smb-share:server=md2k_lab.local,share=md2k_lab_share/Data/Rice/daywise_raw_new_fast/'

smoking_self_report_file = 'SMOKING+SELF_REPORT+PHONE'
activity_type_file = 'ACTIVITY_TYPE+PHONE'
puffmarker_smoking_epi_cloud_file = 'PUFFMARKER_SMOKING_EPISODE+PHONE'
puffmarker_pufflabel_cloud_file = 'org.md2k.streamprocessor+PUFF_LABEL+PHONE'
puffmarker_puffprobability_cloud_file = 'org.md2k.streamprocessor+PUFF_PROBABILITY+PHONE'
puffmarker_puff_features_cloud_file = 'org.md2k.streamprocessor+PUFFMARKER_FEATURE_VECTOR+PHONE'

ema_random_file = 'EMA+RANDOM_EMA+PHONE'
ema_smoking_file = 'EMA+SMOKING_EMA+PHONE'
ema_end_of_day_file = 'EMA+END_OF_DAY_EMA+PHONE'
ema_stressed_file = 'EMA+STRESS_EMA+PHONE'
# ema_random_file = 'EMA+RANDOM_EMA+PHONE.csv.bz2'
# ema_smoking_file = 'EMA+SMOKING_EMA+PHONE.csv.bz2'
# ema_end_of_day_file = 'EMA+END_OF_DAY_EMA+PHONE.csv.bz2'
# ema_stressed_file = 'EMA+STRESS_EMA+PHONE.csv.bz2'

dataquality_MS_left_file = 'org.md2k.motionsense+DATA_QUALITY+ACCELEROMETER+MOTION_SENSE+LEFT_WRIST'
dataquality_MS_right_file = 'org.md2k.motionsense+DATA_QUALITY+ACCELEROMETER+MOTION_SENSE+RIGHT_WRIST'
dataquality_RIP_file = 'org.md2k.autosense+DATA_QUALITY+RESPIRATION+AUTOSENSE_CHEST+CHEST'


rip_filename = 'autosense+RESPIRATION+AUTOSENSE_CHEST+CHEST'
accel_left_filename = 'motionsense+ACCELEROMETER+MOTION_SENSE+LEFT_WRIST'
accel_right_filename = 'motionsense+ACCELEROMETER+MOTION_SENSE+RIGHT_WRIST'

gyro_left_filename = 'motionsense+GYROSCOPE+MOTION_SENSE+LEFT_WRIST'
gyro_right_filename = 'motionsense+GYROSCOPE+MOTION_SENSE+RIGHT_WRIST'


def get_fileName(cur_dir, file_sufix):
    filenames = [name for name in os.listdir(cur_dir) if
                 name.endswith(file_sufix)]
    return filenames

def convert_sample(sample):
    return list([float(x.strip()) for x in sample.split(',')])

def write_matrix(data, filename):
    # data = np.array(data)
    np.savetxt(filename, data, fmt='%s')

def load_datapointarray_and_export(cur_dir, file_sufix, output_dir):

    filenames = get_fileName(cur_dir, file_sufix + '.csv.bz2')

    for filename in filenames:

        fp = bz2.BZ2File(cur_dir + filename)
        gzip_file_content = fp.read()
        fp.close()
        gzip_file_content = gzip_file_content.decode('utf-8')

        lines = gzip_file_content.splitlines()
        write_matrix(lines, output_dir+filename[:-4] )

def copy_withoutrawfiles(uid, pid):
    out_dir = output_data_dir + pid + '/'
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    load_datapointarray_and_export(data_dir + uid + '/', puffmarker_pufflabel_cloud_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', puffmarker_puffprobability_cloud_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', puffmarker_puff_features_cloud_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', puffmarker_smoking_epi_cloud_file, out_dir)

    load_datapointarray_and_export(data_dir + uid + '/', smoking_self_report_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', ema_end_of_day_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', ema_random_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', ema_smoking_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', ema_stressed_file, out_dir)

    load_datapointarray_and_export(data_dir + uid + '/', dataquality_MS_left_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', dataquality_MS_right_file, out_dir)
    load_datapointarray_and_export(data_dir + uid + '/', dataquality_RIP_file, out_dir)


def do_daywise_split(data, data_type, cur_pid_output_dir):
    D = {}
    for v in data:
        sid = v.start_time.strftime('s%Y-%m-%d')
        if sid not in D.keys():
            D[sid] = []
        D[sid].append(v)

    for day in D.keys():
        cur_output_dir = cur_pid_output_dir + day + '/'
        if not os.path.exists(cur_output_dir):
            os.mkdir(cur_output_dir)

        data_tmp = D[day]
        data_tmp.sort(key=lambda x: x.start_time)
        export_as_streamprocessor(cur_output_dir, data_tmp, data_type)


def copy_rawfiles(uid, pid):
    cur_data_dir = data_dir + uid + '/'
    cur_pid_output_dir = output_raw_data_dir + pid + '/'
    if not os.path.exists(cur_pid_output_dir):
        os.mkdir(cur_pid_output_dir)

    rip = load_datapointarray(cur_data_dir, rip_filename)
    accel_l = load_datapointarray(cur_data_dir, accel_left_filename)
    accel_r = load_datapointarray(cur_data_dir, accel_right_filename)
    gyro_l = load_datapointarray(cur_data_dir, gyro_left_filename)
    gyro_r = load_datapointarray(cur_data_dir, gyro_right_filename)

    print('data import complete', len(rip), len(accel_l), len(accel_r))

    days = list(set(list([v.start_time.strftime('s%Y-%m-%d') for v in rip])))
    days.extend(list(set(list([v.start_time.strftime('s%Y-%m-%d') for v in accel_l]))))
    days.extend(list(set(list([v.start_time.strftime('s%Y-%m-%d') for v in accel_r]))))

    print('For ', pid, 'days: ', days)

    for day in days:
        cur_output_dir = cur_pid_output_dir + day + '/'
        if not os.path.exists(cur_output_dir):
            os.mkdir(cur_output_dir)

        rip_tmp = [v for v in rip if v.start_time.strftime('s%Y-%m-%d') == day]
        accel_l_tmp = [v for v in accel_l if v.start_time.strftime('s%Y-%m-%d') == day]
        accel_r_tmp = [v for v in accel_r if v.start_time.strftime('s%Y-%m-%d') == day]
        gyro_l_tmp = [v for v in gyro_l if v.start_time.strftime('s%Y-%m-%d') == day]
        gyro_r_tmp = [v for v in gyro_r if v.start_time.strftime('s%Y-%m-%d') == day]

        # export_datastream_ascloud(cur_output_dir + rip_filename+'.csv', rip_tmp)
        # export_datastream_ascloud(cur_output_dir + accel_left_filename+'.csv', accel_l_tmp)
        # export_datastream_ascloud(cur_output_dir + accel_right_filename+'.csv', accel_r_tmp)
        # export_datastream_ascloud(cur_output_dir + gyro_left_filename+'.csv', gyro_l_tmp)
        # export_datastream_ascloud(cur_output_dir + gyro_right_filename+'.csv', gyro_r_tmp)

        export_as_streamprocessor(cur_output_dir, rip_tmp, RIP)
        export_as_streamprocessor(cur_output_dir, accel_l_tmp, ACCEL_LEFT)
        export_as_streamprocessor(cur_output_dir, accel_r_tmp, ACCEL_RIGHT)
        export_as_streamprocessor(cur_output_dir, gyro_l_tmp, GYRO_LEFT)
        export_as_streamprocessor(cur_output_dir, gyro_r_tmp, GYRO_RIGHT)

def copy_rawfiles_fast(uid, pid):
    cur_data_dir = data_dir + uid + '/'
    cur_pid_output_dir = output_raw_data_dir + pid + '/'
    if not os.path.exists(cur_pid_output_dir):
        os.mkdir(cur_pid_output_dir)

    rip = load_datapointarray(cur_data_dir, rip_filename)
    accel_l = load_datapointarray(cur_data_dir, accel_left_filename)
    accel_r = load_datapointarray(cur_data_dir, accel_right_filename)
    gyro_l = load_datapointarray(cur_data_dir, gyro_left_filename)
    gyro_r = load_datapointarray(cur_data_dir, gyro_right_filename)
    print('data import complete', len(rip), len(accel_l), len(accel_r))

    do_daywise_split(rip, RIP, cur_pid_output_dir)
    do_daywise_split(accel_l, ACCEL_LEFT, cur_pid_output_dir)
    do_daywise_split(accel_r, ACCEL_RIGHT, cur_pid_output_dir)
    do_daywise_split(gyro_l, GYRO_LEFT, cur_pid_output_dir)
    do_daywise_split(gyro_r, GYRO_RIGHT, cur_pid_output_dir)



def main_copy_nonraw_file(pids, uids):

    for i, uid in enumerate(uids):
        pid = pids[i]

        print(pid, uid)
        start = time.time()
        copy_withoutrawfiles(uid, pid)

        print('data extraction complete in ', (time.time()-start)) #10285

def main_copy_raw_file(pids, uids):

    completed_pids = [d for d in os.listdir(output_raw_data_dir)]# os.path.isdir(os.path.join(data_dir, d))]
    completed_pids.sort()
    print(completed_pids)

    for i, uid in enumerate(uids):
        pid = pids[i]

        if pid in completed_pids:
            print(pid, 'YES')
            continue
        print(pid, uid)
        # copy_withoutrawfiles(uid, pid)
        start = time.time()

        # copy_rawfiles(uid, pid)
        copy_rawfiles_fast(uid, pid)
        print('data extraction complete in ', (time.time()-start)) #10285


pids = ['3001']
uids = ['3001']

# main_copy_raw_file(pids, uids)
main_copy_nonraw_file(pids, uids)

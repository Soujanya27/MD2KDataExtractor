from typing import List
import numpy as np


def write_matrix(data, filename):
    data = np.array(data)
    np.savetxt(filename, data, fmt='%s', delimiter=",")

def append_to_file(filename, txt):
    fh = open(filename, 'a')
    fh.write(txt + '\n')
    fh.close()

import pytz
tz = pytz.timezone('US/Central')

LEFT_WRIST = 'leftwrist'
RIGHT_WRIST = 'rightwrist'

ax_left_filename = 'left-wrist-accelx.csv'
ay_left_filename = 'left-wrist-accely.csv'
az_left_filename = 'left-wrist-accelz.csv'
gx_left_filename = 'left-wrist-gyrox.csv'
gy_left_filename = 'left-wrist-gyroy.csv'
gz_left_filename = 'left-wrist-gyroz.csv'
ax_right_filename = 'right-wrist-accelx.csv'
ay_right_filename = 'right-wrist-accely.csv'
az_right_filename = 'right-wrist-accelz.csv'
gx_right_filename = 'right-wrist-gyrox.csv'
gy_right_filename = 'right-wrist-gyroy.csv'
gz_right_filename = 'right-wrist-gyroz.csv'



RIP = 'RIP'
ACCEL_LEFT = 'ACCEL_LEFT'
ACCEL_RIGHT = 'ACCEL_RIGHT'
GYRO_LEFT = 'GYRO_LEFT'
GYRO_RIGHT = 'GYRO_RIGHT'

def export_data(filename: str, data):
    data = [[v[0], v[1]] for v in data]
    write_matrix(data, filename)

    # for dp in data:
    #     txt = str(dp[0]) + ',' + str(dp[1])
    #     append_to_file(filename, txt)

# data is a list of dp=(t, x, y, z)
def export_as_streamprocessor(data_dir : str, data: list, data_type):

    if data_type == RIP:
        rip_data = [[v.start_time.timestamp()*1000, v.sample[0]] for v in data]
        export_data(data_dir + 'rip.csv', rip_data)
        return

    fact = 1
    if data_type in [ACCEL_RIGHT, GYRO_RIGHT]:
        fact = -1
    x = [[v.start_time.timestamp()*1000, v.sample[0]] for v in data]
    y = [[v.start_time.timestamp()*1000, fact*v.sample[1]] for v in data]
    z = [[v.start_time.timestamp()*1000, v.sample[2]] for v in data]

    if data_type == ACCEL_LEFT:
        export_data(data_dir + ax_left_filename, x)
        export_data(data_dir + ay_left_filename, y)
        export_data(data_dir + az_left_filename, z)
    if data_type == ACCEL_RIGHT:
        export_data(data_dir + ax_right_filename, x)
        export_data(data_dir + ay_right_filename, y)
        export_data(data_dir + az_right_filename, z)

    if data_type == GYRO_LEFT:
        export_data(data_dir + gx_left_filename, x)
        export_data(data_dir + gy_left_filename, y)
        export_data(data_dir + gz_left_filename, z)
    if data_type == GYRO_RIGHT:
        export_data(data_dir + gx_right_filename, x)
        export_data(data_dir + gy_right_filename, y)
        export_data(data_dir + gz_right_filename, z)



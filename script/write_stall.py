import matplotlib.pyplot as plt
import sys
import os

ops_file_path = '../build/record_speed_result.txt'


cn_duration_types = [
    "FlushDuration",
    "L0StopDuration",
    "L0SlowDuration",
]

mn_duration_types = [
    "MNCompactionDuration",
]

base_dir = "../build/"

cn_duration_file_paths = [base_dir + x + ".txt" for x in cn_duration_types]
mn_duration_file_paths = [base_dir + x + ".txt" for x in mn_duration_types]

# 读取文件中的数据
with open(ops_file_path, 'r') as file:
    lines = file.readlines()

# 提取每一行的数字
ops_file_data = [int(line.strip()) for line in lines]
ops_data = ops_file_data[1:]
ops_start_time = ops_file_data[0]

# 计算每秒的速度变化
speed = [ops_data[i] - ops_data[i-1] for i in range(1, len(ops_data))]
max_speed = max(speed)

# 构建时间轴
time = list(range(1, len(speed) + 1))

# 画图
plt.plot(time, speed, marker='o', markersize='1', linestyle='-')


CnDurationColors = [
    'red',
    'green',
    'yellow',
]

MnDurationColors = [
    'blue',
]

for i, file_path in enumerate(cn_duration_file_paths):
    if os.path.exists(file_path):
        with open(file_path) as f:
            lines = f.readlines()
        for l in lines:
            start_point, end_point = [int(x) for x in l.strip().split(',')]
            plt.hlines(y=max_speed + 500, xmin=2 * (start_point - ops_start_time) / 1e6, xmax=2 * (end_point - ops_start_time) / 1e6, color=CnDurationColors[i])
        max_speed += 10000



for file_path in sorted(os.listdir(base_dir)):
    if (file_path.startswith(mn_duration_types[0])):
        with open(os.path.join(base_dir, file_path)) as f:
            lines = f.readlines()
        print(file_path)
        for l in lines:
            start_point, end_point = [int(x) for x in l.strip().split(',')]
            plt.hlines(y=max_speed + 10000, xmin=2 * (start_point - ops_start_time) / 1e6, xmax=2 * (end_point - ops_start_time) / 1e6, color='blue')
        max_speed += 10000


plt.xlabel('Time (seconds)')
plt.ylabel('Ops/')
plt.title('Ops Over Time')
plt.grid(True)


plt.savefig(f'{ops_file_path}.png')

import matplotlib.pyplot as plt

# 读取文件中的数据
with open('build/record_speed_result.txt', 'r') as file:
    lines = file.readlines()

# 提取每一行的数字
data = [int(line.strip()) for line in lines]

# 计算每秒的速度变化
speed = [data[i] - data[i-1] for i in range(1, len(data))]

# 构建时间轴
time = list(range(1, len(speed) + 1))

# 画图
plt.plot(time, speed, marker='o', markersize='1', linestyle='-')
plt.xlabel('Time (seconds)')
plt.ylabel('Speed')
plt.title('Speed Over Time')
plt.grid(True)


plt.savefig('speed_over_time.png')

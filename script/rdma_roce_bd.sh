#!/usr/bin/bash

# 读取RoCE模式的RDMA设备的counter数据，计算并输出实时的使用带宽

counter_path="/sys/class/infiniband/mlx5_0/ports/1/counters/port_rcvt_data"

start=0
lasting_time=0

## 如果等待4秒没有变化则认为已经结束
max_pending_time=4
pending_time=0

get_counter()
{
    echo `cat $counter_path`
}

start_counter=$(get_counter $counter_path)
curr_counter=$(get_counter $counter_path)
prev_counter=$(get_counter $counter_path)

#没用的小玩意，试试看
print_dot_c=0

while true
do
    if [ $start -eq 0 ] && [ $prev_counter -ne $curr_counter ];
    then
        printf "\r\e[K\n"
        start=1
        lasting_time=1
        start_counter=$prev_counter
        echo "bw test start, counter path: $counter_path, initial counter $start_counter"
    fi

    sleep 1
    lasting_time=$((lasting_time + 1))
    prev_counter=$curr_counter
    curr_counter=$(get_counter $counter_path)

    if [ $start -ne 0 ];
    then
        if [ $prev_counter -ne $curr_counter ];
        then
            pending_time=0
        elif [ $pending_time -eq 0 ];
        then
            pending_time=1
        else
            pending_time=$((pending_time+1))
        fi
        if [ $pending_time -ne 0 ];
        then
            if [ $pending_time -eq $max_pending_time ];
            then
                echo "pending $max_pending_time, assuming test finish, lasting `echo $(($lasting_time - $max_pending_time + 1))`s, overall bandwidth: `echo "scale=2; ($curr_counter - $start_counter) * 4 / 1024 / 1024 / ($lasting_time - $max_pending_time + 1)" | bc` MB/s"
                exit 0
            fi
        fi
        echo "current bandwidth `echo "scale=2; ($curr_counter - $prev_counter) * 4 / 1024 / 1024" | bc ` MB/s"
    else
        if [ $print_dot_c -eq 0 ];
        then
            printf "\rwaiting for data"
        elif [ $print_dot_c -eq 4 ];
        then
            printf "\rwaiting for data\e[K"
            print_dot_c=0
        else
            echo -n "."
        fi
        print_dot_c=$((print_dot_c+1))
    fi
done

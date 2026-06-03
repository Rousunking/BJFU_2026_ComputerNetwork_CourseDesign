import sys
import os
import socket
import time
import struct
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common.protocol import (
    UDP_TYPE_CONN_REQ, UDP_TYPE_CONN_ACK, UDP_TYPE_DATA, UDP_TYPE_ACK,
    STUDENT_ID,
    udp_pack_conn_request, udp_pack_data,
    udp_unpack,
    write_log, log_timestamp,
)

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log_client.txt')

TIMEOUT_MS = 300
WINDOW_SIZE = 5
PAYLOAD_SIZE = 80
TARGET_PACKETS = 30

def log(msg: str):
    write_log(LOG_FILE, msg)

def main():
    if len(sys.argv) != 4:
        print("用法: python udpclient.py <serverIP> <serverPort> <filepath>")
        print("示例: python udpclient.py 172.20.0.3 9999 test_ascii_file.txt")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    filepath = sys.argv[3]

    if not os.path.exists(filepath):
        print(f"错误: 文件不存在: {filepath}")
        sys.exit(1)

    with open(filepath, 'rb') as f:
        file_data = f.read()

    file_size = len(file_data)
    packets = []
    for i in range(0, file_size, PAYLOAD_SIZE):
        chunk = file_data[i:i + PAYLOAD_SIZE]
        packets.append({
            'seq': len(packets),
            'data': chunk,
            'start': i,
            'end': min(i + PAYLOAD_SIZE, file_size) - 1,
            'length': len(chunk),
        })

    total_packets = len(packets)
    if total_packets < TARGET_PACKETS:
        print(f"注意: 文件仅能生成 {total_packets} 个报文 (目标 {TARGET_PACKETS}), "
              f"将发送所有可用报文")

    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"===== UDP Client run_log.txt =====\n")
        f.write(f"启动时间: {log_timestamp()}\n")
        f.write(f"服务器: {server_ip}:{server_port}\n")
        f.write(f"文件: {filepath} ({file_size} bytes)\n")
        f.write(f"总报文数: {total_packets}, 每报文 {PAYLOAD_SIZE} bytes\n")
        f.write(f"窗口大小: {WINDOW_SIZE}, 超时: {TIMEOUT_MS}ms\n\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_addr = (server_ip, server_port)

    print("===== 阶段 1: 连接建立 =====")
    print(f"向 {server_ip}:{server_port} 发送连接请求...")

    conn_req = udp_pack_conn_request(total_packets)
    sock.sendto(conn_req, server_addr)
    log(f"SEND 连接请求: StudentID=0x{STUDENT_ID:04X}, TotalBlocks={total_packets}")

    sock.settimeout(TIMEOUT_MS / 1000.0)
    connected = False
    for retry in range(5):
        try:
            data, addr = sock.recvfrom(4096)
            pkt = udp_unpack(data)
            if pkt and pkt['type'] == UDP_TYPE_CONN_ACK:
                log(f"RECV 连接确认 来自 {addr}")
                connected = True
                print("连接建立成功!\n")
                break
        except socket.timeout:
            print(f"  连接请求超时, 重试 {retry + 1}/5...")
            sock.sendto(conn_req, server_addr)
            log(f"RETRANSMIT 连接请求 (重试 {retry + 1})")

    if not connected:
        print("错误: 无法建立连接")
        sock.close()
        sys.exit(1)

    print("===== 阶段 2: 数据传输 (SR: Selective Repeat) =====\n")

    acked = [False] * total_packets
    send_time = {}
    first_send_time = {}
    rtt_values = []
    total_sent = 0

    send_base = 0

    try:
        while send_base < total_packets:
            window_end = min(send_base + WINDOW_SIZE, total_packets)
            for i in range(send_base, window_end):
                if i not in send_time:
                    pkt = udp_pack_data(packets[i]['seq'], packets[i]['data'])
                    sock.sendto(pkt, server_addr)
                    now = time.time()
                    send_time[i] = now
                    if i not in first_send_time:
                        first_send_time[i] = now
                    total_sent += 1

                    data_text = packets[i]['data'].decode('ascii', errors='replace')
                    log(f"SEND Data Seq={i} [{packets[i]['start']}-{packets[i]['end']}] "
                        f"Len={packets[i]['length']} "
                        f"Data={repr(data_text[:30])}{'...' if len(data_text) > 30 else ''}")

            try:
                sock.settimeout(TIMEOUT_MS / 1000.0)
                data, addr = sock.recvfrom(4096)
                pkt = udp_unpack(data)

                if pkt is None or pkt['type'] != UDP_TYPE_ACK:
                    continue

                ack_num = pkt['ack_num']
                server_time = pkt.get('server_time', '??:??:??')
                now = time.time()

                log(f"RECV ACK={ack_num} ServerTime={server_time}")

                if send_base <= ack_num < send_base + WINDOW_SIZE:
                    if not acked[ack_num] and ack_num in send_time:
                        rtt = (now - send_time[ack_num]) * 1000
                        rtt_values.append(rtt)
                        acked[ack_num] = True
                        print(f"第 {ack_num + 1} 个（第 {packets[ack_num]['start']}~{packets[ack_num]['end']} 字节）"
                              f"client 端已发送")
                        print(f"  → RTT={rtt:.1f}ms, 服务器时间={server_time}")

                    while send_base < total_packets and acked[send_base]:
                        send_base += 1

                if send_base < total_packets and not acked[send_base]:
                    gap = sum(1 for j in range(send_base + 1, min(send_base + WINDOW_SIZE, total_packets)) if acked[j])
                    if gap >= 3:
                        log(f"FAST-RETRANSMIT Seq={send_base} (窗口内 {gap} 个后续包已确认, 但 Seq={send_base} 未确认)")
                        print(f"快重传: 第 {send_base + 1} 个数据包 (后续 {gap} 包已确认)")
                        rpkt = udp_pack_data(packets[send_base]['seq'], packets[send_base]['data'])
                        sock.sendto(rpkt, server_addr)
                        send_time[send_base] = time.time()
                        total_sent += 1

            except socket.timeout:
                now = time.time()
                for i in range(send_base, min(send_base + WINDOW_SIZE, total_packets)):
                    if not acked[i] and i in send_time:
                        if now - send_time[i] > TIMEOUT_MS / 1000.0:
                            pkt = udp_pack_data(packets[i]['seq'], packets[i]['data'])
                            sock.sendto(pkt, server_addr)
                            send_time[i] = now
                            total_sent += 1
                            print(f"重传第 {i + 1} 个（第 {packets[i]['start']}~{packets[i]['end']} 字节）"
                                  f"数据包")
                            log(f"RETRANSMIT Data Seq={i} [{packets[i]['start']}-{packets[i]['end']}]")

    except KeyboardInterrupt:
        print("\n用户中断")

    print(f"\n===== 阶段 3: 统计信息 =====")
    actual_packets = total_packets
    print(f"目标报文数: {actual_packets}")
    print(f"实际发送次数 (含重传): {total_sent}")
    print(f"成功确认数: {sum(acked)}")

    if total_sent > 0:
        delivery_ratio = actual_packets / total_sent * 100
        loss_rate = 100 - delivery_ratio
        print(f"丢包率: {loss_rate:.1f}% (交付率: {delivery_ratio:.1f}%)")
    else:
        print("丢包率: N/A (无发送记录)")

    if rtt_values:
        import pandas as pd
        rtt_series = pd.Series(rtt_values)
        rtt_max = rtt_series.max()
        rtt_min = rtt_series.min()
        rtt_avg = rtt_series.mean()
        rtt_std = rtt_series.std()

        print(f"\nRTT 统计 (ms):")
        print(f"  最大 RTT: {rtt_max:.2f} ms")
        print(f"  最小 RTT: {rtt_min:.2f} ms")
        print(f"  平均 RTT: {rtt_avg:.2f} ms")
        print(f"  标准差:   {rtt_std:.2f} ms")
        print(f"  样本数:   {len(rtt_values)}")
    else:
        print("RTT: 无样本数据")

    log(f"\n===== 统计信息 =====")
    log(f"目标报文数: {actual_packets}")
    log(f"实际发送次数: {total_sent}")
    if total_sent > 0:
        log(f"丢包率: {loss_rate:.1f}%")
    if rtt_values:
        log(f"RTT max={rtt_max:.2f}ms min={rtt_min:.2f}ms avg={rtt_avg:.2f}ms std={rtt_std:.2f}ms")

    sock.close()
    print(f"\n日志已保存至: {LOG_FILE}")

if __name__ == '__main__':
    main()

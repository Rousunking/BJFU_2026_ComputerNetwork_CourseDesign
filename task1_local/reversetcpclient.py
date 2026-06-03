# task1/reversetcpclient.py
# TCP 字符串反转服务——客户端
# 运行环境: Windows Host OS
# 用法: python reversetcpclient.py <serverIP> <serverPort> <Lmin> <Lmax> <filepath>
#
# 协议:
#   Client → Server:  Initialization (Type=1, N=总块数)
#   Server → Client:  Agreement     (Type=2)
#   Client → Server:  reverseRequest(Type=3, Length, Data)  …循环 N 次
#   Server → Client:  reverseAnswer (Type=4, Length, reverseData) …循环 N 次
#
# 功能:
#   - 读取纯 ASCII 文本文件，按随机长度分块 (seed=42, Lmin~Lmax)
#   - 向服务器请求逐块反转
#   - 输出每块反转结果
#   - 组装完整反转文件并写入输出文件
#   - 自动生成 run_log.txt

import sys
import os
import socket
import random
import struct

from protocol import (
    TCP_TYPE_INIT, TCP_TYPE_AGREE, TCP_TYPE_REQUEST, TCP_TYPE_ANSWER,
    tcp_pack_init, tcp_pack_request,
    tcp_unpack_header,
    recv_exact,
    write_log, log_timestamp,
)

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log_client.txt')


def log(msg: str):
    write_log(LOG_FILE, msg)


def split_into_chunks(data: bytes, lmin: int, lmax: int, seed: int = 42) -> list:
    """
    将文件数据按随机长度切分为若干块。
    - 使用固定 seed 保证可复现
    - 每块长度 ∈ [Lmin, Lmax]
    - 最后一块可能不足 Lmin
    - 返回: [(chunk_data, start_offset, length), ...]
    """
    random.seed(seed)
    chunks = []
    pos = 0
    total = len(data)

    while pos < total:
        chunk_len = random.randint(lmin, lmax)
        if pos + chunk_len > total:
            chunk_len = total - pos
        chunks.append({
            'index': len(chunks) + 1,       # 1-based
            'data': data[pos:pos + chunk_len],
            'offset': pos,                   # 起始字节偏移
            'length': chunk_len,
        })
        pos += chunk_len

    return chunks


def get_chunk_offset(chunk_index: int, chunks: list) -> int:
    """
    根据块序号 (1-based) 返回该块在原始文件中的起始字节偏移量。
    用于验收时说明分块逻辑。
    """
    for c in chunks:
        if c['index'] == chunk_index:
            return c['offset']
    return -1


def main():
    if len(sys.argv) != 6:
        print("用法: python reversetcpclient.py <serverIP> <serverPort> <Lmin> <Lmax> <filepath>")
        print("示例: python reversetcpclient.py 172.20.0.3 8888 50 100 test_ascii_file.txt")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    lmin = int(sys.argv[3])
    lmax = int(sys.argv[4])
    filepath = sys.argv[5]

    # ---- 1. 读取 ASCII 文件 ----
    if not os.path.exists(filepath):
        print(f"错误: 文件不存在: {filepath}")
        sys.exit(1)

    with open(filepath, 'rb') as f:
        file_data = f.read()

    # 验证是否为 ASCII 可打印字符
    try:
        file_data.decode('ascii')
    except UnicodeDecodeError as e:
        print(f"警告: 文件包含非 ASCII 字符 (位置 {e.start}), 将用 '?' 替换")
        # 仍然继续，服务端反转时用 errors='replace'

    file_text = file_data.decode('ascii', errors='replace')

    # ---- 2. 随机分块 (seed=42 固定复现) ----
    chunks = split_into_chunks(file_data, lmin, lmax, seed=42)
    N = len(chunks)

    # ---- 3. 初始化日志 ----
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"===== TCP Client run_log.txt =====\n")
        f.write(f"启动时间: {log_timestamp()}\n")
        f.write(f"服务器: {server_ip}:{server_port}\n")
        f.write(f"文件: {filepath} ({len(file_data)} bytes)\n")
        f.write(f"分块参数: Lmin={lmin}, Lmax={lmax}, seed=42, N={N}\n\n")

    # 打印分块信息
    print(f"文件大小: {len(file_data)} bytes, 共分为 {N} 块")
    for c in chunks:
        print(f"  第 {c['index']} 块: 起始偏移={c['offset']}, 长度={c['length']}")
    print()

    # ---- 4. 连接服务器 ----
    print(f"正在连接 {server_ip}:{server_port} ...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)  # 连接超时 30s

    try:
        sock.connect((server_ip, server_port))
        print("连接成功!\n")
        log(f"TCP 连接建立: {server_ip}:{server_port}")
    # AF_INET = 使用IPv4地址
    # SOCK_STREAM = 使用TCP协议（可靠传输）
        # ---- 5. 发送 Initialization ----
        init_pkt = tcp_pack_init(N)
        sock.sendall(init_pkt)
        log(f"SEND Initialization: Type=1, N={N}")

        # ---- 6. 接收 Agreement ----
        header = recv_exact(sock, 2)  # Agreement 只有 2 字节 Type
        msg_type, _ = tcp_unpack_header(header + b'\x00\x00\x00\x00')  # 补零以便解包
        if msg_type != TCP_TYPE_AGREE:
            print(f"协议错误: 期望 Agreement (Type=2), 收到 Type={msg_type}")
            sock.close()
            sys.exit(1)

        log(f"RECV Agreement: Type=2")
        print("收到服务器 Agreement，开始传输数据...\n")

        # ---- 7. 循环: 发送 reverseRequest → 接收 reverseAnswer ----
        reversed_chunks = []
        for c in chunks:
            idx = c['index']
            data = c['data']
            data_text = data.decode('ascii', errors='replace')

            # 发送 reverseRequest
            req_pkt = tcp_pack_request(data)
            sock.sendall(req_pkt)
            log(f"SEND reverseRequest #{idx}: Len={len(data)}, "
                f"Data={repr(data_text[:50])}{'...' if len(data_text) > 50 else ''}")

            # 接收 reverseAnswer 头
            header = recv_exact(sock, 6)
            msg_type, ans_len = tcp_unpack_header(header)
            if msg_type != TCP_TYPE_ANSWER:
                print(f"协议错误: 期望 reverseAnswer (Type=4), 收到 Type={msg_type}")
                break

            # 接收 reverseAnswer 数据
            rev_data = recv_exact(sock, ans_len)
            rev_text = rev_data.decode('ascii', errors='replace')

            log(f"RECV reverseAnswer #{idx}: Len={ans_len}, "
                f"Data={repr(rev_text[:50])}{'...' if len(rev_text) > 50 else ''}")

            # 输出反转结果
            print(f"第 {idx} 块：{rev_text}")
            reversed_chunks.append(rev_data)

        # ---- 8. 组装完整反转文件并输出 ----
        full_reversed = b''.join(reversed_chunks)
        output_path = filepath.rsplit('.', 1)[0] + '_reversed.txt'
        with open(output_path, 'wb') as f:
            f.write(full_reversed)

        print(f"\n完整反转文本已写入: {output_path}")
        log(f"完整反转文件已写入: {output_path} ({len(full_reversed)} bytes)")

    except ConnectionError as e:
        print(f"连接错误: {e}")
        log(f"ERROR: 连接错误: {e}")
    except socket.timeout:
        print("操作超时")
        log("ERROR: 操作超时")
    except Exception as e:
        print(f"错误: {e}")
        log(f"ERROR: {e}")
    finally:
        sock.close()
        log("连接关闭")
        print(f"\n日志已保存至: {LOG_FILE}")


if __name__ == '__main__':
    main()

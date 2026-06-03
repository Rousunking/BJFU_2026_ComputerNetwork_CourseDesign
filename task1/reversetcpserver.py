# task1/reversetcpserver.py
# TCP 字符串反转服务——服务端
# 运行环境: WSL / Linux Guest OS
# 用法: python reversetcpserver.py <port>
#
# 协议:
#   Client → Server:  Initialization (Type=1, N=总块数)
#   Server → Client:  Agreement     (Type=2)
#   Client → Server:  reverseRequest(Type=3, Length, Data)  …循环 N 次
#   Server → Client:  reverseAnswer (Type=4, Length, reverseData) …循环 N 次
#
# 功能:
#   - 多线程并发处理 ≥2 个客户端
#   - 对每个块的内容逐字节反转
#   - 自动生成 run_log.txt 记录所有收发事件

import sys
import os
import socket
import threading
import struct

from protocol import (
    TCP_TYPE_INIT, TCP_TYPE_AGREE, TCP_TYPE_REQUEST, TCP_TYPE_ANSWER,
    tcp_pack_agree, tcp_pack_answer,
    tcp_unpack_header,
    recv_exact,
    write_log, log_timestamp,
)

# 日志文件路径 (在 task1 目录下生成)
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log_server.txt')
LOG_LOCK = threading.Lock()  # 保护多线程并发写日志


def log(msg: str):
    """线程安全地写入日志"""
    with LOG_LOCK:
        write_log(LOG_FILE, msg)


def handle_client(conn: socket.socket, addr: tuple, client_id: int):
    """
    处理单个客户端连接的核心逻辑
    """
    client_ip, client_port = addr
    log(f"[Client-{client_id} {client_ip}:{client_port}] 新连接建立")

    try:
        # ---- 阶段 1: 接收 Initialization ----
        header = recv_exact(conn, 6)  # Type(2) + N(4) = 6 Bytes
        msg_type, n_blocks = tcp_unpack_header(header)

        if msg_type != TCP_TYPE_INIT:
            log(f"[Client-{client_id}] 错误: 期望 Type=1(Init), 收到 Type={msg_type}")
            conn.close()
            return

        log(f"[Client-{client_id}] RECV Initialization: N={n_blocks}")

        # ---- 阶段 2: 发送 Agreement ----
        conn.sendall(tcp_pack_agree())
        log(f"[Client-{client_id}] SEND Agreement")

        # ---- 阶段 3: 循环处理 reverseRequest / reverseAnswer ----
        for i in range(n_blocks):
            # 3a: 接收请求头 Type(2) + Length(4)
            header = recv_exact(conn, 6)
            msg_type, data_len = tcp_unpack_header(header)

            if msg_type != TCP_TYPE_REQUEST:
                log(f"[Client-{client_id}] 错误: 期望 Type=3(Request), 收到 Type={msg_type}")
                break

            # 3b: 接收数据体
            data = recv_exact(conn, data_len)
            data_text = data.decode('ascii', errors='replace')
            log(f"[Client-{client_id}] RECV reverseRequest #{i + 1}: Len={data_len}, "
                f"Data={repr(data_text[:50])}{'...' if len(data_text) > 50 else ''}")

            # 3c: 反转字符串 (逐字节反转)
            reversed_data = data[::-1]
            rev_text = reversed_data.decode('ascii', errors='replace')

            # 3d: 发送 reverseAnswer
            answer_pkt = tcp_pack_answer(reversed_data)
            conn.sendall(answer_pkt)
            log(f"[Client-{client_id}] SEND reverseAnswer #{i + 1}: Len={len(reversed_data)}, "
                f"Data={repr(rev_text[:50])}{'...' if len(rev_text) > 50 else ''}")

        log(f"[Client-{client_id}] 处理完成, 共 {n_blocks} 个块")

    except ConnectionError:
        log(f"[Client-{client_id}] 连接异常断开")
    except Exception as e:
        log(f"[Client-{client_id}] 异常: {e}")
    finally:
        conn.close()
        log(f"[Client-{client_id}] 连接关闭")


def main():
    if len(sys.argv) != 2:
        print("用法: python reversetcpserver.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])

    # 清空旧日志
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"===== TCP Server run_log.txt =====\n")
        f.write(f"启动时间: {log_timestamp()}\n")
        f.write(f"监听端口: {port}\n\n")

    # 创建 TCP socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('0.0.0.0', port))
    server_sock.listen(10)  # 允许最多 10 个排队连接

    print(f"[TCP Server] 正在监听 0.0.0.0:{port} ...")
    print(f"[TCP Server] 日志文件: {LOG_FILE}")
    log(f"服务器启动, 监听 0.0.0.0:{port}")

    client_counter = 0

    try:
        while True:
            conn, addr = server_sock.accept()
            client_counter += 1
            print(f"[TCP Server] 客户端 #{client_counter} 来自 {addr[0]}:{addr[1]}")

            # 为每个客户端创建线程处理
            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, client_counter),
                daemon=True,
            )
            t.start()

    except KeyboardInterrupt:
        print("\n[TCP Server] 收到中断信号, 正在关闭...")
    finally:
        server_sock.close()
        log("服务器关闭")


if __name__ == '__main__':
    main()

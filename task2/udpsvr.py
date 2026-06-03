# task2/udpsvr.py
# UDP 可靠传输模拟——服务端 (SR 接收方, 多线程)
# 运行环境: WSL / Linux Guest OS
# 用法: python udpsvr.py <port> [loss_rate] [corrupt_rate]
#
# 功能:
#   - 模拟 TCP 连接建立 (通过自定义应用层首部)
#   - 随机丢弃数据包，模拟不可靠传输
#   - 随机翻转比特，模拟数据损坏
#   - SR 接收方: 接受窗口内所有包，逐包独立 ACK
#   - 多线程: 主线程 recvfrom 分发，每客户端独立工作线程，最多同时 10 个

import sys
import os
import socket
import random
import threading
import queue
from datetime import datetime

from protocol import (
    UDP_TYPE_CONN_REQ, UDP_TYPE_CONN_ACK, UDP_TYPE_DATA, UDP_TYPE_ACK,
    STUDENT_ID, calc_checksum,
    udp_pack_conn_ack, udp_pack_ack,
    udp_unpack,
    write_log, log_timestamp,
)

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log_server.txt')
MAX_CLIENTS = 10
WINDOW_SIZE = 5  # SR 接收窗口大小

# 全局锁
log_lock = threading.Lock()
send_lock = threading.Lock()
# 客户端会话注册表: addr -> Session
clients_lock = threading.Lock()
sessions = {}


def log(msg: str):
    """线程安全日志"""
    with log_lock:
        write_log(LOG_FILE, msg)


def safe_send(sock, pkt, addr):
    """线程安全发送 UDP 报文"""
    with send_lock:
        sock.sendto(pkt, addr)


class Session:
    """单个客户端的会话状态与工作线程"""

    def __init__(self, addr, total_blocks, loss_rate, corrupt_rate):
        self.addr = addr
        self.total_blocks = total_blocks
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        # 本客户端的报文队列 (主线程分发到这里)
        self.pkt_queue = queue.Queue()
        # SR 接收方状态
        self.rcv_base = 0
        self.rcv_buffer = {}
        self.received_count = 0
        self.done = threading.Event()  # 传输完成信号

    def tag(self) -> str:
        return f"[{self.addr[0]}:{self.addr[1]}]"

    def run(self, sock, client_sem: threading.Semaphore):
        """工作线程主循环: 从 pkt_queue 取包处理"""
        log(f"{self.tag()} 线程启动")

        try:
            while self.received_count < self.total_blocks:
                try:
                    data, addr = self.pkt_queue.get(timeout=2.0)
                except queue.Empty:
                    continue

                pkt = udp_unpack(data)
                if pkt is None:
                    continue

                if pkt['type'] == UDP_TYPE_DATA:
                    self._handle_data(pkt, sock)
                elif pkt['type'] == UDP_TYPE_CONN_REQ:
                    # 重复连接请求, 重发 ACK
                    log(f"{self.tag()} 重复连接请求, 重发 ACK")
                    safe_send(sock, udp_pack_conn_ack(), self.addr)

        except Exception as e:
            log(f"{self.tag()} 异常: {e}")
        finally:
            log(f"{self.tag()} 线程结束 (rcv_base={self.rcv_base}, "
                f"received={self.received_count}/{self.total_blocks})")
            with clients_lock:
                sessions.pop(self.addr, None)
            client_sem.release()
            self.done.set()

    def _handle_data(self, pkt, sock):
        """处理单个 DATA 报文 (SR 接收方逻辑)"""
        seq_num = pkt.get('seq_num', -1)
        data_len = pkt.get('data_len', 0)
        data_payload = pkt.get('data', b'')
        data_text = data_payload.decode('ascii', errors='replace')

        log(f"{self.tag()} RECV Data Seq={seq_num} Len={data_len} "
            f"Data={repr(data_text[:40])}{'...' if len(data_text) > 40 else ''}")

        # ---- 随机丢包模拟 ----
        if random.random() < self.loss_rate:
            log(f"{self.tag()} DISCARD Seq={seq_num} (模拟丢包)")
            return

        # ---- 随机损坏模拟 ----
        if self.corrupt_rate > 0 and random.random() < self.corrupt_rate:
            corrupted = bytearray(data_payload)
            byte_idx = random.randint(0, len(corrupted) - 1)
            bit_idx = random.randint(0, 7)
            corrupted[byte_idx] ^= (1 << bit_idx)
            data_payload = bytes(corrupted)
            if calc_checksum(data_payload) != pkt.get('checksum'):
                log(f"{self.tag()} CORRUPT Seq={seq_num} byte[{byte_idx}].bit{bit_idx}, 校验失败 → 丢弃")
                return

        # ---- SR 接收方逻辑 ----
        if seq_num >= self.rcv_base and seq_num < self.rcv_base + WINDOW_SIZE:
            if seq_num not in self.rcv_buffer:
                self.rcv_buffer[seq_num] = data_payload
                self.received_count += 1
                log(f"{self.tag()} ACCEPT (SR) Seq={seq_num}")

            server_time = datetime.now().strftime('%H:%M:%S')
            safe_send(sock, udp_pack_ack(seq_num, server_time), self.addr)
            log(f"{self.tag()} SEND ACK={seq_num}")

            # 交付连续的已缓存包
            while self.rcv_base in self.rcv_buffer:
                del self.rcv_buffer[self.rcv_base]
                log(f"{self.tag()} DELIVER Seq={self.rcv_base}, 新 rcv_base={self.rcv_base + 1}")
                self.rcv_base += 1

        elif seq_num >= self.rcv_base - WINDOW_SIZE and seq_num < self.rcv_base:
            log(f"{self.tag()} DUPLICATE (SR) Seq={seq_num}, 重发 ACK")
            server_time = datetime.now().strftime('%H:%M:%S')
            safe_send(sock, udp_pack_ack(seq_num, server_time), self.addr)

        else:
            log(f"{self.tag()} IGNORE (SR) Seq={seq_num} "
                f"窗口=[{self.rcv_base}, {self.rcv_base + WINDOW_SIZE - 1}]")

        if self.received_count >= self.total_blocks:
            log(f"{self.tag()} 全部 {self.total_blocks} 个分组已接收!")
            print(f"[{self.tag()}] 传输完成! ({self.total_blocks} 包)")


def main():
    if len(sys.argv) < 2:
        print("用法: python udpsvr.py <port> [loss_rate] [corrupt_rate]")
        print("示例: python udpsvr.py 9999 0.2 0.1    (丢包率 20%, 损坏率 10%)")
        sys.exit(1)

    port = int(sys.argv[1])
    loss_rate = float(sys.argv[2]) if len(sys.argv) >= 3 else 0.2
    corrupt_rate = float(sys.argv[3]) if len(sys.argv) >= 4 else 0.0

    # 初始化日志
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write("===== UDP Server run_log.txt =====\n")
        f.write(f"启动时间: {log_timestamp()}\n")
        f.write(f"监听端口: {port}\n")
        f.write(f"丢包率: {loss_rate * 100:.0f}%\n")
        f.write(f"损坏率: {corrupt_rate * 100:.0f}%\n")
        f.write(f"最大并发客户端: {MAX_CLIENTS}\n\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', port))

    # 并发控制信号量: 最多 MAX_CLIENTS 个客户端同时连接
    client_sem = threading.Semaphore(MAX_CLIENTS)

    print(f"[UDP Server] 正在监听 0.0.0.0:{port} "
          f"(丢包率={loss_rate * 100:.0f}%, 损坏率={corrupt_rate * 100:.0f}%, "
          f"最大客户端={MAX_CLIENTS})")
    print(f"[UDP Server] 日志文件: {LOG_FILE}")
    log(f"服务器启动, 监听 0.0.0.0:{port}, 最大并发={MAX_CLIENTS}")

    try:
        while True:
            data, addr = sock.recvfrom(4096)
            pkt = udp_unpack(data)

            if pkt is None:
                continue

            msg_type = pkt['type']

            # ---- 主线程分发: 根据地址找到对应客户端线程 ----
            with clients_lock:
                session = sessions.get(addr)

            if session is not None:
                # 已知客户端, 将报文放入其队列
                session.pkt_queue.put((data, addr))

            elif msg_type == UDP_TYPE_CONN_REQ:
                # 新客户端连接请求
                if client_sem.acquire(blocking=False):
                    total_blocks = pkt.get('total_blocks', 0)
                    session = Session(addr, total_blocks, loss_rate, corrupt_rate)
                    with clients_lock:
                        sessions[addr] = session
                    log(f"{addr} 连接请求: TotalBlocks={total_blocks}")
                    print(f"[UDP Server] 新客户端 {addr[0]}:{addr[1]} "
                          f"({len(sessions)}/{MAX_CLIENTS})")

                    # 发送连接确认
                    safe_send(sock, udp_pack_conn_ack(), addr)
                    log(f"{addr} SEND 连接确认")

                    # 启动工作线程
                    t = threading.Thread(target=session.run,
                                         args=(sock, client_sem),
                                         daemon=True)
                    t.start()
                else:
                    log(f"{addr} 拒绝连接: 已达最大并发 {MAX_CLIENTS}")
                    print(f"[UDP Server] 拒绝 {addr[0]}:{addr[1]}: "
                          f"已达最大并发 ({MAX_CLIENTS})")

            else:
                # 未知客户端且不是连接请求, 忽略
                log(f"RECV 未知报文 Type={msg_type} 来自 {addr}, 忽略")

    except KeyboardInterrupt:
        print("\n[UDP Server] 收到中断信号, 正在关闭...")
    finally:
        sock.close()
        log("服务器关闭")


if __name__ == '__main__':
    main()

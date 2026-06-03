# common/protocol.py
# 共享协议定义：TCP 和 UDP 任务的报文类型常量、编解码工具函数
# 用于 Task1 (TCP) 和 Task2 (UDP)

import struct
from datetime import datetime

# ============================================================
#  Task1: TCP 报文类型常量
# ============================================================
TCP_TYPE_INIT = 1       # Initialization:  客户端→服务器，告知块数 N
TCP_TYPE_AGREE = 2      # Agreement:       服务器→客户端，确认就绪
TCP_TYPE_REQUEST = 3    # reverseRequest:   客户端→服务器，请求反转
TCP_TYPE_ANSWER = 4     # reverseAnswer:    服务器→客户端，返回反转结果

# ============================================================
#  Task1: TCP 报文编解码
# ============================================================
def tcp_pack_init(n: int) -> bytes:
    """打包 Initialization 报文: [2B Type=1][4B N] → 6 Bytes"""
    return struct.pack('>HI', TCP_TYPE_INIT, n)

def tcp_pack_agree() -> bytes:
    """打包 Agreement 报文: [2B Type=2] → 2 Bytes"""
    return struct.pack('>H', TCP_TYPE_AGREE)

def tcp_pack_request(data: bytes) -> bytes:
    """打包 reverseRequest 报文: [2B Type=3][4B Length][Data] → 6+len(Data) Bytes"""
    return struct.pack('>HI', TCP_TYPE_REQUEST, len(data)) + data

def tcp_pack_answer(data: bytes) -> bytes:
    """打包 reverseAnswer 报文: [2B Type=4][4B Length][reverseData] → 6+len(Data) Bytes"""
    return struct.pack('>HI', TCP_TYPE_ANSWER, len(data)) + data

def tcp_unpack_header(header: bytes) -> tuple:
    """解包 TCP 报文头 (至少 2 字节), 返回 (type, length_or_n)"""
    msg_type = struct.unpack('>H', header[:2])[0]
    if msg_type in (TCP_TYPE_AGREE,):
        return msg_type, 0
    val = struct.unpack('>I', header[2:6])[0]
    return msg_type, val

# ============================================================
#  Task2: UDP 报文类型常量
# ============================================================
UDP_TYPE_CONN_REQ = 1   # 连接建立请求
UDP_TYPE_CONN_ACK = 2   # 连接建立确认
UDP_TYPE_DATA = 3       # 数据报文
UDP_TYPE_ACK = 4        # 确认应答

# StudentID 常量: 学号后4位 2104 XOR 0x5A3C = 0x5204
STUDENT_ID = 0x5204

# ============================================================
#  Task2: UDP 报文编解码  (自定义应用层协议首部)
# ============================================================
def udp_pack_conn_request(total_blocks: int) -> bytes:
    """
    打包连接建立请求报文:
    [2B StudentID=0x5204][2B Type=1][4B TotalBlocks] → 8 Bytes
    """
    return struct.pack('>HHI', STUDENT_ID, UDP_TYPE_CONN_REQ, total_blocks)

def udp_pack_conn_ack() -> bytes:
    """
    打包连接建立确认报文:
    [2B StudentID][2B Type=2] → 4 Bytes
    """
    return struct.pack('>HH', STUDENT_ID, UDP_TYPE_CONN_ACK)

def udp_pack_data(seq_num: int, data: bytes) -> bytes:
    """
    打包数据报文:
    [2B StudentID][2B Type=3][4B SeqNum][4B DataLen][2B Checksum][Data] → 14+len(Data) Bytes
    Checksum = 16-bit XOR of all data bytes
    """
    checksum = calc_checksum(data)
    return struct.pack('>HHIIH', STUDENT_ID, UDP_TYPE_DATA, seq_num, len(data), checksum) + data

def calc_checksum(data: bytes) -> int:
    """计算 16-bit XOR 校验和: 每 2 字节为一组异或"""
    result = 0
    for i in range(0, len(data), 2):
        if i + 1 < len(data):
            word = (data[i] << 8) | data[i + 1]
        else:
            word = data[i] << 8
        result ^= word
    return result & 0xFFFF

def udp_pack_ack(ack_num: int, server_time_str: str = "") -> bytes:
    """
    打包 ACK 报文:
    [2B StudentID][2B Type=4][4B AckNum][8B ServerTime] → 16 Bytes
    ServerTime 格式: 'HH:MM:SS' 补空格到 8 字节
    """
    time_bytes = server_time_str.ljust(8)[:8].encode('ascii')
    return struct.pack('>HHI', STUDENT_ID, UDP_TYPE_ACK, ack_num) + time_bytes

def udp_unpack(data: bytes) -> dict:
    """
    解包 UDP 报文，返回字典:
    { 'student_id', 'type', 'seq_num'/'ack_num'/'total_blocks', 'data_len', 'data', 'server_time' }
    学号不匹配时返回 None
    """
    if len(data) < 4:
        return None
    student_id, msg_type = struct.unpack('>HH', data[:4])

    # 学号校验: 不匹配直接丢弃
    if student_id != STUDENT_ID:
        return None

    result = {'student_id': student_id, 'type': msg_type}

    if msg_type == UDP_TYPE_CONN_REQ and len(data) >= 8:
        result['total_blocks'] = struct.unpack('>I', data[4:8])[0]
    elif msg_type == UDP_TYPE_CONN_ACK:
        pass  # 无额外字段
    elif msg_type == UDP_TYPE_DATA and len(data) >= 14:
        seq_num, data_len, checksum = struct.unpack('>IIH', data[4:14])
        result['seq_num'] = seq_num
        result['data_len'] = data_len
        result['checksum'] = checksum
        payload = data[14:14 + data_len] if len(data) >= 14 + data_len else data[14:]
        result['data'] = payload
        # 校验和验证
        expected = calc_checksum(payload)
        result['checksum_valid'] = (checksum == expected)
    elif msg_type == UDP_TYPE_ACK and len(data) >= 16:
        result['ack_num'] = struct.unpack('>I', data[4:8])[0]
        result['server_time'] = data[8:16].decode('ascii').strip()

    return result

# ============================================================
#  日志工具函数
# ============================================================
def log_timestamp() -> str:
    """返回格式化的时间戳字符串 HH:MM:SS.fff"""
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

def write_log(log_file: str, msg: str):
    """追加一行日志到文件"""
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{log_timestamp()}] {msg}\n")

# ============================================================
#  精确读取指定字节数 (用于 TCP 流式读取)
# ============================================================
def recv_exact(sock, n: int) -> bytes:
    """从 TCP socket 精确读取 n 字节"""
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("连接已关闭")
        data += chunk
    return data

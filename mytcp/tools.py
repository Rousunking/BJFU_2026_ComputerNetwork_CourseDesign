import struct
TCP_TYPE_INIT = 1       # Initialization:  客户端→服务器，告知块数 N
TCP_TYPE_AGREE = 2      # Agreement:       服务器→客户端，确认就绪
TCP_TYPE_REQUEST = 3    # reverseRequest:   客户端→服务器，请求反转
TCP_TYPE_ANSWER = 4     # reverseAnswer:    服务器→客户端，返回反转结果
from datetime import datetime
def log_timestamp():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]
def write_log(log_file: str, msg: str):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f'[{log_timestamp()}] {msg}\n')
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
def recv_exact(sock, n: int) -> bytes:
    data=b''
    while len(data)<n:
        chunk=sock.recv(n-len(data))
        if not chunk:
            raise ConnectionError("连接已关闭")
        data+=chunk
    return data
    
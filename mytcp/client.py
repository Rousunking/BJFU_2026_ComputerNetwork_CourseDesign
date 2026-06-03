import sys
import os
import socket
from tools import (recv_exact, write_log, log_timestamp,
                    tcp_pack_init, tcp_pack_request, tcp_unpack_header,
                    TCP_TYPE_AGREE, TCP_TYPE_ANSWER)
import random
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log_client.txt')
def log(msg: str):
    write_log(LOG_FILE, msg)
def split_into_chunks(data: bytes, lmin: int, lmax: int, seed: int = 42) -> list:
    random.seed(seed)
    chunks=[]
    pos=0
    total=len(data)
    while pos<total:
        chunk_length=random.randint(lmin,lmax)
        if pos+chunk_length>total:
            chunk_length=total-pos
        chunks.append({
            'index':len(chunks)+1,
            'data':data[pos:pos+chunk_length],
            'offset':pos,
            'length':chunk_length,
        })
        pos+=chunk_length
    return chunks

def main():
    if (len(sys.argv)!=6):
        print('输入参数有误')
        sys.exit(1)
    server_ip=sys.argv[1]
    server_port=int(sys.argv[2])
    lmin=int(sys.argv[3])
    lmax=int(sys.argv[4])
    filepath=sys.argv[5]

    if not os.path.exists(filepath):
        print(f"错误，文件不存在{filepath}")
        sys.exit(1)
    with open(filepath,"rb") as f:
        file_data=f.read()

    try:
        file_data.decode('ascii')
    except UnicodeDecodeError as e:
        print(f"警告,文件中存在非acsii字符,在位置({e.start})，将用符号?替代")
    file_text =file_data.decode('ascii',errors='replace')


    chunks=split_into_chunks(file_data,lmin,lmax,42)
    N=len(chunks)

    with open(LOG_FILE,'w',encoding='utf-8') as f:
        f.write("tcp_run_log\n")
        f.write(f'启动时间{log_timestamp()}\n')
        f.write(f"服务器: {server_ip}:{server_port}\n")
        f.write(f"文件: {filepath} ({len(file_data)} bytes)\n")
        f.write(f"分块参数: Lmin={lmin}, Lmax={lmax}, seed=42, N={N}\n\n")
    print(f"文件大小: {len(file_data)} bytes, 共分为 {N} 块")
    for c in chunks:
        print(f"  第 {c['index']} 块: 起始偏移={c['offset']}, 长度={c['length']}")
    print()

    print(f"正在连接服务器{server_ip}:{server_port}")
    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.settimeout(30)

    try:
        sock.connect((server_ip,server_port))
        print(f'连接建立成功{server_ip}:{server_port}')
        log(f'连接建立成功{server_ip}:{server_port}')

        init_pkt=tcp_pack_init(N)
        sock.sendall(init_pkt)
        log(f"SEND Initialization: Type=1, N={N}")

        header=tcp_unpack_header(recv_exact(sock,2))[0]
        if header!=TCP_TYPE_AGREE:
            print(f"协议错误: 期望 Agreement (Type=2), 收到 Type={header}")
            sys.exit(1)
        log(f"RECV Agreement: Type=2")
        print("收到服务器 Agreement，开始传输数据...\n")

        reversed_chunks=[]
        for c in chunks:
            idx=c['index']
            data=c['data']
            data_text=data.decode('ascii',errors='replace')
        
            req_pkt=tcp_pack_request(data)
            sock.sendall(req_pkt)
            log(f"SEND reverseRequest #{idx}: Len={len(data)}, "
                f"Data={repr(data_text[:50])}{'...' if len(data_text) > 50 else ''}")

            header=recv_exact(sock,6)
            msg_type , ans_len=tcp_unpack_header(header)
            if msg_type != TCP_TYPE_ANSWER:
                print(f"协议错误: 期望 reverseAnswer (Type=4), 收到 Type={msg_type}")
                break
            
            reversed_data=recv_exact(sock,ans_len)
            reversed_text=reversed_data.decode('ascii',errors='replace')
            log(f"RECV reverseAnswer #{idx}: Len={ans_len}, "
                f"Data={repr(reversed_text[:50])}{'...' if len(reversed_text) > 50 else ''}")

            print(f"第 {idx} 块：{reversed_text}")
            reversed_chunks.append(reversed_data)

        full_reversed=b''.join(reversed_chunks)
        output_path=filepath.rsplit('.',1)[0]+'_reversed.txt'
        with open(output_path,'wb') as f:
            f.write(full_reversed)
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
        log('连接关闭')
        print(f'日志已保存至{LOG_FILE}')
if __name__=='__main__':
    main()
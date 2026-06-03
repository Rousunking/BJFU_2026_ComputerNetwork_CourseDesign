================================================================================
                     Task1: TCP Socket 编程 —— 说明文档
================================================================================

【运行环境】
  - 客户端: Windows 10/11 (Host OS), Python 3.8+
  - 服务端: Ubuntu/WSL (Guest OS), Python 3.8+
  - 依赖: 无第三方库 (仅标准库 socket, struct, threading, random)

【文件清单】
  task1/
    protocol.py         - 协议定义 & 报文编解码 & 日志工具
    reversetcpclient.py - TCP 客户端
    reversetcpserver.py - TCP 服务端
    test_ascii_file.txt - 测试用 ASCII 文件
    run_log.txt         - 运行时自动生成

【运行方式】

  1. 先在 WSL/Linux 上启动服务端:
     cd /mnt/c/PyCharm/TCP
     python task1/reversetcpserver.py <port>
     示例: python task1/reversetcpserver.py 8888

  2. 在 Windows 上运行客户端:
     cd c:\PyCharm\TCP
     python task1\reversetcpclient.py <serverIP> <serverPort> <Lmin> <Lmax> <filepath>
     示例: python task1\reversetcpclient.py 172.20.0.3 8888 50 100 task1\test_ascii_file.txt

     (serverIP 是 WSL 的 IP 地址, 可用 wsl hostname -I 或 ip addr show eth0 查看)

【协议说明】

  应用层自定义二进制协议 (大端序/网络字节序):

  ┌──────────────────────────────────────────────────────┐
  │ Type=1  Initialization:  [2B Type][4B N]             │  6 Bytes
  │ Type=2  Agreement:       [2B Type]                   │  2 Bytes
  │ Type=3  reverseRequest:  [2B Type][4B Len][Data]     │  6+Len Bytes  
  │ Type=4  reverseAnswer:   [2B Type][4B Len][RevData]  │  6+Len Bytes
  └──────────────────────────────────────────────────────┘

  交互流程:
    Client                                  Server
      │── Initialization (Type=1, N) ──────→│
      │←─ Agreement (Type=2) ───────────────│
      │── reverseRequest (Type=3, Data1) ──→│
      │←─ reverseAnswer (Type=4, RevData1) ─│
      │── reverseRequest (Type=3, Data2) ──→│
      │←─ reverseAnswer (Type=4, RevData2) ─│
      │          ... (重复 N 次)              │

【分块算法】
  - seed=42 固定随机种子, 保证结果可复现
  - 每块长度在 [Lmin, Lmax] 范围内随机
  - 最后一块可能不足 Lmin
  - get_chunk_offset(chunk_index, chunks) 可查询任意块的起始字节偏移

【技术要点】
  1. TCP 流式数据用 recv_exact() 精确读取, 处理 TCP 分包/粘包
  2. struct.pack('>HI', ...) 大端序编码保证跨平台兼容
  3. 服务端 threading 实现并发处理 ≥2 客户端
  4. 线程锁保护日志文件并发写入
  5. 反转操作: data[::-1] 逐字节反转 ASCII 字符串


【Wireshark 抓包筛选表达式】
  假设服务端 IP 为 172.20.0.3:

  ip.addr == 172.20.0.3    筛选与服务端的所有通信 (含 TCP 三次握手、数据、四次挥手)

【配置选项】
  服务端参数:  python reversetcpserver.py <port>
    port        监听端口, 如 8888

  客户端参数:  python reversetcpclient.py <serverIP> <serverPort> <Lmin> <Lmax> <filepath>
    serverIP    WSL 服务端 IP 地址
    serverPort  服务端监听端口
    Lmin        分块最小字节数
    Lmax        分块最大字节数
    filepath    待反转的 ASCII 文件路径

  固定参数:
    seed = 42   随机分块种子, 保证每次运行分块方式一致

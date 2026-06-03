================================================================================
                     Task2: UDP Socket 编程 —— 说明文档
================================================================================

【运行环境】
  - 客户端: Windows 10/11 (Host OS), Python 3.8+ (需安装 pandas)
  - 服务端: Ubuntu/WSL (Guest OS), Python 3.8+
  - 依赖: pandas (可选, 用于 RTT 标准差计算; 若未安装则回退到纯 Python 统计)

【文件清单】
  task2/
    protocol.py       - 协议定义 & 报文编解码 & 日志工具
    udpclient.py      - UDP 客户端 (SR 发送方)
    udpsvr.py         - UDP 服务端 (SR 接收方, 多线程)
    test_udp_file.txt - 测试用 ASCII 文件 (≥2400 bytes, 可生成 ≥30 个报文)
    run_log.txt       - 运行时自动生成

【运行方式】

  1. 先在 WSL/Linux 上启动服务端:
     cd /mnt/c/PyCharm/TCP
     python task2/udpsvr.py <port> [loss_rate]
     示例: python task2/udpsvr.py 9999 0.2    (丢包率 20%)

  2. 在 Windows 上运行客户端:
     cd c:\PyCharm\TCP
     python task2\udpclient.py <serverIP> <serverPort> <filepath>
     示例: python task2\udpclient.py 172.20.0.3 9999 task2\test_udp_file.txt

     (serverIP 是 WSL 的 IP 地址)

【自定义协议首部设计】

  每 UDP 报文均携带如下应用层首部:

  ┌─────────────┬─────────┬─────────────────────────────────┐
  │ StudentID   │ Type    │ Type-Specific Payload           │
  │ 2 Bytes     │ 2 Bytes │                                 │
  └─────────────┴─────────┴─────────────────────────────────┘

  StudentID = 学号后4位 XOR 0x5A3C
  示例: 2104 (0x0838) XOR 0x5A3C = 0x5204

  四种报文类型:

  Type=1 (连接请求):  [2B SID][2B 1][4B TotalBlocks]           → 8 Bytes
  Type=2 (连接确认):  [2B SID][2B 2]                            → 4 Bytes
  Type=3 (数据报文):  [2B SID][2B 3][4B SeqNum][4B Len][Data]  → 12+Len Bytes
  Type=4 (ACK应答):   [2B SID][2B 4][4B AckNum][8B SvrTime]    → 16 Bytes

  此首部设计从命名到字段大小到排列顺序均为独立设计，低碰撞概率。

【协议机制】

  ┌─────────────────────────────────────────────────────────┐
  │ 阶段 1: 连接建立 (模拟 TCP 三次握手)                      │
  │   Client → Server: 连接请求 (Type=1)                     │
  │   Server → Client: 连接确认 (Type=2)                     │
  │                                                         │
  │ 阶段 2: 数据传输 (SR 滑动窗口)                            │
  │   - 窗口大小: 5                                          │
  │   - 每报文载荷: 80 bytes                                 │
  │   - 超时: 300ms (每包独立定时器)                          │
  │   - 服务端随机丢包 (loss_rate 可配置, 默认 0.0)            │
  │   - 服务端随机损坏比特 (corrupt_rate 可配置, 默认 0.0)     │
  │   - 接收方缓存乱序包, 逐包独立 ACK (SR 接收方)             │
  │   - 发送方只重传超时/快重传触发的单个包 (SR 发送方)         │
  └─────────────────────────────────────────────────────────┘

【输出示例】

  每个报文确认时输出:
    第 1 个（第 0~79 字节）client 端已发送
      → RTT=12.5ms, 服务器时间=20:35:42

  超时重传时输出:
    重传第 3 个（第 160~239 字节）数据包

  全部完成后输出统计:
    丢包率: 25.0% (交付率: 75.0%)
    RTT 统计 (ms):
      最大 RTT: 45.23 ms
      最小 RTT: 8.12 ms
      平均 RTT: 15.67 ms
      标准差:   9.34 ms

【可调参数 (在 udpclient.py 文件顶部)】
  TIMEOUT_MS = 300       - 超时时间 (毫秒)
  WINDOW_SIZE = 5        - 发送窗口大小
  PAYLOAD_SIZE = 80      - 每 UDP 载荷字节数
  TARGET_PACKETS = 30    - 目标报文数

【技术要点】
  1. SR 滑动窗口协议: 发送方按窗口滑动发送, 每包独立定时器
  2. 逐包独立 ACK: 服务端收到每个包立即回复 ACK=seq
  3. SR 接收方缓存: rcv_buffer 缓存乱序到达的包, 按序交付
  4. 超时重传: 只对超时的单个包重传, 不重传整个窗口
  5. 快重传: 窗口内后续包已被确认数 ≥3 时, 立即重传 send_base
  6. 校验和: 16-bit XOR 校验和检测数据损坏, 损坏则丢弃
  7. 多线程服务端: 主线程 recvfrom 分发, 每客户端独立工作线程, Semaphore(10) 控制并发
  8. 学号校验: udp_unpack 内置 StudentID 验证, 不匹配直接丢弃


【Wireshark 抓包筛选表达式】
  假设服务端 IP 为 172.20.0.3:

  ip.addr == 172.20.0.3    筛选与服务端的所有通信 (含连接握手、数据包、ACK)

【配置选项】
  服务端参数:  python udpsvr.py <port> [loss_rate] [corrupt_rate]
    port         监听端口, 如 9999
    loss_rate    丢包率 (0.0~1.0), 默认 0.0
    corrupt_rate 损坏率 (0.0~1.0), 默认 0.0

  客户端参数:  python udpclient.py <serverIP> <serverPort> <filepath>
    serverIP     WSL 服务端 IP 地址
    serverPort   服务端监听端口
    filepath     待传输的 ASCII 文件路径

  客户端固定参数 (在 udpclient.py 文件顶部修改):
    TIMEOUT_MS = 300       超时时间 (毫秒)
    WINDOW_SIZE = 5        发送窗口大小
    PAYLOAD_SIZE = 80      每个 UDP 载荷字节数
    TARGET_PACKETS = 30    目标发送报文数

  服务端固定参数 (在 udpsvr.py 文件顶部修改):
    MAX_CLIENTS = 10       最大同时连接客户端数

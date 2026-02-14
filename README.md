#  RocksDB LighTune

**RocksDB LighTune** 是一个基于大语言模型（LLM）的 RocksDB 动态性能调优工具。

本项目包含一个修改版的 RocksDB 基准测试工具源码和一个 Python 自动化控制脚本。通过修改 `db_bench` 使其支持运行时动态加载参数，并结合 Python 脚本利用盘古大模型（或其他 LLM）API，通过发送实时监控吞吐量、CPU 和内存指标，从而实现全自动化的数据库参数调优。

##  项目结构

| 文件名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `db_bench_tool.cc` | C++ Source | 修改后的 RocksDB 工具源码，增加了动态监听参数文件的逻辑。 |
| `dynamic_tune.py` | Python Script | 核心控制器。负责启动测试、监控指标、调用 LLM 并下发调优指令。 |

---

##  安装与编译 (Prerequisites)

在使用本工具前，需要拥有 RocksDB 的源码环境，并替换文件重新编译。
```bash
mkdir LighTune
cd LightTune
git clone https://github.com/MemStor/LighTune.git
wget https://github.com/facebook/rocksdb/archive/refs/tags/v8.8.1.tar.gz
tar -xzf v8.8.1.tar.gz
cd rocksdb-8.8.1
cp /LighTune/db_bench_tool.cc /tools/db_bench_tool.cc
make -j static_lib db_bench
```

##  环境配置
### 1.配置大模型api
方案A，使用云端大模型
打开dynamic_tune.py，找到 call_llm_api 函数部分。根据需要修改 base_url、api_key 和 model 名称。
```
client = OpenAI(
    api_key=os.environ.get('DEEPSEEK_API_KEY'),  # 从环境变量获取 Key
    base_url="https://api.deepseek.com"          # DeepSeek 官方接口
)
# ...
response = client.chat.completions.create(
    model="deepseek-chat",                       # 模型名称
    # ...
)
```
方案B, 使用本地部署大模型(比如pangu7b模型)
```
client = OpenAI(
    api_key="empty",  # 通常不需要api_key
    base_url="http://localhost:1040/v1"          # 1040为模型部署的端口号
)
# ...
response = client.chat.completions.create(
    model="pangu_embedded_7b",                   # 模型名称
    # ...
)
```
### 2. 修改路径配置
在dynamic_tune.py 顶部，修改以下配置以匹配你的系统：
```

DB_BENCH_PATH = "/home/user/rocksdb/db_bench"  #编译好的 db_bench 绝对路径
DB_DIR = "/tmp/rocksdb_test_db"  #运行负载写入数据库位置
OPTIONS_FILE_PATH = "./options_file.ini"  #配置文件位置
TUNE_INTERVAL = 100      # 调优间隔
MONITOR_DURATION = 5     # 每次决策前，参考过去 5 秒的数据
TOTAL_DURATION = 1000    # 负载总运行时间
```
## 运行脚本
配置api_key
```bash
export DEEPSEEK_API_KEY="sky--xxxxxxxx"
python3 dynamic_tune.py
```
## 输出
#### throughput_chart.png: 全程吞吐量折线图，直观展示 AI 调优后的性能变化。
#### tuning_history.log: 详细记录了每一次 AI 的决策过程、Prompt、回复内容及参数变更对比。
#### throughput_per_second.log: 原始性能数据，包含每秒的时间戳和吞吐量。

import os
import time
import subprocess
import threading
import json
import re
from datetime import datetime
from collections import deque, defaultdict
from openai import OpenAI
import matplotlib.pyplot as plt
import shutil
import shlex
# ================= 配置区域 =================
DB_BENCH_PATH = "/workspace/test/rocksdb-8.8.1/db_bench"
DB_DIR = "/tmp/rocksdbtest-0/dbbenc"
OPTIONS_FILE_PATH = "/workspace/elmo/options_file.ini"
DYN_PARAM_FILE = "/tmp/rocksdb_dyn_params"

# 日志文件路径
TUNING_LOG_FILE = "tuning_history.log"        # 记录 LLM 调优决策 (每100s一条)
PER_SECOND_LOG_FILE = "throughput_per_second.log"  # 记录每秒吞吐 (每1s一条)
PLOT_IMAGE_FILE = "throughput_chart.png"      # 最终生成的图表

# 初始参数
current_params = {
    "max_background_jobs": "2",
    "max_background_compactions": "-1",
    "level0_file_num_compaction_trigger": "4",
    "level0_slowdown_writes_trigger": "20",
    "level0_stop_writes_trigger": "36",
    "write_buffer_size": "67108864",
    "target_file_size_base": "67108864"
}

# 调优配置
TUNE_INTERVAL = 100      # 每隔 100 秒调优一次
MONITOR_DURATION = 5     # 每次决策前，参考过去 5 秒的数据
TOTAL_DURATION = 1000    # 总运行时间

# ===========================================

# 全局数据存储
# key: int(秒), value: float(这一秒所有线程吞吐之和)
throughput_data = defaultdict(float)
throughput_lock = threading.Lock()


class ResourceMonitor(threading.Thread):
    def __init__(self, pid, interval=1.0):
        super().__init__()
        self.pid = pid
        self.interval = interval
        self.samples = deque(maxlen=3600)
        self.stop_event = threading.Event()

    def run(self):
        CLK_TCK = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        prev_ticks = None
        prev_time = time.time()

        while not self.stop_event.is_set():
            try:
                with open(f"/proc/{self.pid}/stat", "r") as f:
                    fields = f.read().split()
                    cur_utime = int(fields[13])
                    cur_stime = int(fields[14])
                    cur_ticks = cur_utime + cur_stime

                now = time.time()
                if prev_ticks is not None:
                    delta_ticks = cur_ticks - prev_ticks
                    delta_sec = now - prev_time
                    cpu_cores = (delta_ticks / CLK_TCK) / \
                        delta_sec if delta_sec > 0 else 0
                else:
                    cpu_cores = 0

                rss_mb = 0
                with open(f"/proc/{self.pid}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS"):
                            rss_kb = int(line.split()[1])
                            rss_mb = rss_kb / 1024.0
                            break

                self.samples.append({
                    "timestamp": now,
                    "cpu_cores": cpu_cores,
                    "rss_mb": rss_mb
                })
                prev_ticks = cur_ticks
                prev_time = now
            except:
                break
            time.sleep(self.interval)

    def get_recent_metrics(self, duration_seconds):
        now = time.time()
        recent = [s for s in self.samples if now -
                  s["timestamp"] <= duration_seconds]
        if not recent:
            return 0.0, 0.0
        avg_cpu = sum(s["cpu_cores"] for s in recent) / len(recent)
        max_rss = max(s["rss_mb"] for s in recent)
        return avg_cpu, max_rss


class OutputReader(threading.Thread):
    def __init__(self, process):
        super().__init__()
        self.process = process
        # 正则: 匹配 db_bench 的瞬时吞吐量
        self.pattern = re.compile(
            r'and \(\s*([0-9.]+)\s*,.*?\) ops/second in \(.*?,([0-9.]+)\) seconds')
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set() and self.process.poll() is None:
            line = self.process.stdout.readline()
            if not line:
                break

            try:
                decoded_line = line.decode('utf-8', errors='ignore').strip()
            except:
                continue

            if "ops/second" in decoded_line:
                # print(f"[DB_BENCH] {decoded_line}") # 可选：打印原始日志
                match = self.pattern.search(decoded_line)
                if match:
                    try:
                        instant_tp = float(match.group(1))
                        total_elapsed = float(match.group(2))
                        second_key = int(total_elapsed)

                        # 存入全局字典，自动聚合所有线程的数据
                        with throughput_lock:
                            throughput_data[second_key] += instant_tp
                    except:
                        pass

    def get_avg_throughput_last_n_seconds(self, duration_seconds=5):
        if not throughput_data:
            return 0.0
        with throughput_lock:
            all_seconds = sorted(throughput_data.keys())
            if not all_seconds:
                return 0.0
            last_second = all_seconds[-1]
            start_second = max(0, last_second - duration_seconds)
            total_tp = 0.0
            count = 0
            for sec in range(int(start_second), int(last_second) + 1):
                if sec in throughput_data:
                    total_tp += throughput_data[sec]
                    count += 1
            return total_tp / count if count > 0 else 0.0

# ==============================================================================
# 每秒日志记录器
# ==============================================================================


class ThroughputLogger(threading.Thread):
    """
    专门负责将 throughput_data 中的数据，按秒顺序写入 CSV 日志文件。
    """

    def __init__(self):
        super().__init__()
        self.stop_event = threading.Event()

        # 初始化日志文件头
        with open(PER_SECOND_LOG_FILE, "w") as f:
            f.write("elapsed_seconds,total_ops_per_sec,timestamp\n")

    def run(self):
        next_sec_to_log = 1  # 从第 1 秒开始记录

        while not self.stop_event.is_set():
            time.sleep(1.0)  # 每秒检查一次

            with throughput_lock:
                if not throughput_data:
                    continue
                # 获取当前最大的秒数（代表最新进度）
                max_sec_in_dict = max(throughput_data.keys())

            # 只要 throughput_data 里有比 next_sec_to_log 更大的秒数，
            # 说明 next_sec_to_log 这一秒的所有线程数据都已经到齐了，可以写入文件
            while next_sec_to_log < max_sec_in_dict:
                # 获取该秒吞吐，如果没有数据（比如卡顿了）则记为 0
                val = throughput_data.get(next_sec_to_log, 0.0)
                current_ts = datetime.now().isoformat()

                with open(PER_SECOND_LOG_FILE, "a") as f:
                    f.write(f"{next_sec_to_log},{val:.2f},{current_ts}\n")

                # 指针后移
                next_sec_to_log += 1

# ==============================================================================


def call_llm_api(context_data):
    api_key = os.environ.get('DEEPSEEK_API_KEY')
    if not api_key:
        print("[AI-Tuner] Error: DEEPSEEK_API_KEY not set.")
        return "{}"

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

    prompt = f"""
    You are a RocksDB tuning expert.
    
    [Metrics (Last {MONITOR_DURATION}s avg)]
    - Throughput: {context_data['avg_throughput']:.2f} ops/sec
    - CPU Cores: {context_data['avg_cpu_cores']:.2f}
    - Memory RSS: {context_data['peak_memory_mb']:.2f} MB
    
    [Current Parameters]
    {json.dumps(context_data['current_params'], indent=2)}
    
    [Goal]
    Maximize stable throughput. 
    
    [Output]
    JSON only. Example: {{"level0_slowdown_writes_trigger": "30"}}
    """

    print("\n[AI-Tuner] Sending request to DeepSeek...")
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You output valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.7
        )
        content = response.choices[0].message.content
        print(f"[AI-Tuner] DeepSeek Response: {content}")
        return content
    except Exception as e:
        print(f"[AI-Tuner] Error: {e}")
        return "{}"


def apply_parameters(new_params):
    """
    使用写临时文件 + 原子重命名的方式，彻底杜绝 C++ 读到空文件或半截文件的问题。
    """
    if not new_params:
        return

    print(
        f"[AI-Tuner] Starting to apply {len(new_params)} parameters sequentially...")

    for key, value in new_params.items():
        try:
            # 定义临时文件名
            tmp_file = DYN_PARAM_FILE + ".tmp"

            # 1. 写入临时文件 (C++ 看不到这个文件)
            with open(tmp_file, "w") as f:
                f.write(f"{key}={value}\n")
                f.flush()
                os.fsync(f.fileno())  # 确保数据真正落盘

            # 2. 原子替换 (Atomic Replace)
            # 这相当于 Linux 的 'mv tmp_file real_file'
            # 这一步是瞬间完成的，C++ 要么读到旧的，要么读到新的，绝不会读到空的。
            os.replace(tmp_file, DYN_PARAM_FILE)

            # 不需要 os.utime，因为 replace 操作会自然更新目录项和文件的元数据

            print(f"   -> Applied: {key}={value}")

            # 3. 更新本地状态
            if key in current_params:
                current_params[key] = str(value)

            # 4. 等待 C++ 轮询
            time.sleep(2.5)

        except Exception as e:
            print(f"[AI-Tuner] Error applying {key}: {e}")
            # 如果出错，尝试清理临时文件
            if os.path.exists(DYN_PARAM_FILE + ".tmp"):
                try:
                    os.remove(DYN_PARAM_FILE + ".tmp")
                except:
                    pass

    print("[AI-Tuner] All parameters applied.")


def log_tuning_action(timestamp, duration, metrics, old_p, new_p):
    log_entry = {
        "timestamp": timestamp,
        "llm_latency": duration,
        "metrics_before_tuning": metrics,
        "params_before": old_p,
        "changes": new_p
    }
    with open(TUNING_LOG_FILE, "a") as f:
        f.write(json.dumps(log_entry) + "\n")


def generate_plot():
    print("\n[Plot] Generating throughput chart...")
    if not throughput_data:
        print("[Plot] No data collected.")
        return

    with throughput_lock:
        sorted_seconds = sorted(throughput_data.keys())
        time_points = sorted_seconds
        tp_points = [throughput_data[s] for s in sorted_seconds]

    plt.figure(figsize=(14, 7))
    plt.plot(time_points, tp_points, color='#1f77b4',
             linewidth=1.0, label='Total Throughput')
    plt.title(
        f'RocksDB Throughput (Total Duration: {len(time_points)}s)', fontsize=14)
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Throughput (ops/sec)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_IMAGE_FILE, dpi=150)
    print(f"[Plot] Chart saved to {PLOT_IMAGE_FILE}")


def main():
    # 1. 环境准备
    if os.path.exists(DYN_PARAM_FILE):
        os.remove(DYN_PARAM_FILE)
    open(DYN_PARAM_FILE, 'a').close()
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)

    # 2. 启动 db_bench
    cmd = [
        DB_BENCH_PATH,
        f"--db={DB_DIR}",
        f"--options_file={OPTIONS_FILE_PATH}",
        "--benchmarks=fillrandom",
        "--use_existing_db=0",
        "--num=50000000",
        f"--duration={TOTAL_DURATION}",
        "--stats_interval_seconds=1",
        "--threads=1"
    ]
    print(f"[Main] Starting: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)

    # 3. 启动所有辅助线程
    reader = OutputReader(proc)
    reader.start()

    resource_monitor = ResourceMonitor(proc.pid)
    resource_monitor.start()

    # 启动每秒吞吐量记录器
    tp_logger = ThroughputLogger()
    tp_logger.start()

    # 4. 主循环控制调优
    bench_start_time = time.time()
    next_tune_time = bench_start_time + TUNE_INTERVAL

    try:
        while proc.poll() is None:
            now = time.time()

            # 调优周期
            if now >= next_tune_time:
                print(
                    f"\n[Cycle] Tuning Triggered at {datetime.now().strftime('%H:%M:%S')}")

                avg_cpu, peak_mem = resource_monitor.get_recent_metrics(
                    MONITOR_DURATION)
                avg_tp = reader.get_avg_throughput_last_n_seconds(
                    MONITOR_DURATION)

                metrics = {
                    "avg_cpu_cores": avg_cpu,
                    "peak_memory_mb": peak_mem,
                    "avg_throughput": avg_tp
                }

                context = metrics.copy()
                context["current_params"] = current_params.copy()

                llm_start = time.time()
                resp = call_llm_api(context)
                try:
                    changes = json.loads(resp.replace(
                        "```json", "").replace("```", "").strip())
                except:
                    changes = {}
                llm_duration = time.time() - llm_start

                if changes:
                    apply_parameters(changes)
                else:
                    print("[AI-Tuner] No changes suggested.")

                log_tuning_action(
                    datetime.now().isoformat(),
                    llm_duration,
                    metrics,
                    context["current_params"],
                    changes
                )

                next_tune_time += TUNE_INTERVAL
                print(
                    f"[Cycle] Next tuning at {datetime.fromtimestamp(next_tune_time).strftime('%H:%M:%S')}")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[Main] Interrupted.")
    finally:
        # --- 新增诊断代码 ---
        exit_code = proc.poll()

        # 如果 exit_code 不是 None，说明进程自己挂了（不是我们手动 terminate 的）
        if exit_code is not None:
            print(
                f"\n[CRITICAL] db_bench process ended unexpectedly with EXIT CODE: {exit_code}")

            if exit_code == -11:
                print("❌ 原因: Segmentation Fault (段错误)。")
            elif exit_code == -6:
                print("❌ 原因: Aborted (中止)。")
                print("   -> 说明 C++ 抛出了未捕获的异常 (比如 SetOptions 失败抛错)。")
                print("   -> 请检查 C++ 代码是否包裹了 try { ... } catch (...)。")
            else:
                print(f"❌ 原因: 未知错误 (Code {exit_code})。")
        else:
            print("\n[Main] Stopping db_bench manually...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except:
                proc.kill()

        # --- 原有清理逻辑 ---
        reader.stop_event.set()
        resource_monitor.stop_event.set()
        tp_logger.stop_event.set()

        reader.join()
        resource_monitor.join()
        tp_logger.join()

        generate_plot()
        print(f"[Main] Detailed logs saved to: {PER_SECOND_LOG_FILE}")
        print("[Main] Done.")


if __name__ == "__main__":
    main()

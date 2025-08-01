import gc
import qlib
import pandas as pd
from qlib.contrib.evaluate import backtest_daily
from qlib.contrib.strategy import TopkDropoutStrategy
import logging
import time
import psutil
import os
from datetime import datetime
import sys

# 新增: 详细debug日志文件handler
class DebugFileFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.DEBUG

def setup_logging():
    """设置日志配置，输出到文件和控制台，并增加详细debug日志文件"""
    log_filename = f"test_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    debug_log_filename = f"test_debug_detail_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # 主日志handler
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    stream_handler = logging.StreamHandler()
    # 详细debug日志handler
    debug_file_handler = logging.FileHandler(debug_log_filename, encoding='utf-8')
    debug_file_handler.setLevel(logging.DEBUG)
    debug_file_handler.addFilter(DebugFileFilter())

    # 配置日志格式
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[file_handler, stream_handler, debug_file_handler]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"日志文件已创建: {log_filename}")
    logger.info(f"详细debug日志文件: {debug_log_filename}")
    return logger, log_filename, debug_log_filename

def log_system_info(logger, step_name):
    """记录系统信息"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent()

        logger.info(f"[{step_name}] 系统状态:")
        logger.info(f"  - 内存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        logger.info(f"  - CPU使用率: {cpu_percent:.2f}%")
        logger.info(f"  - 系统总内存: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.2f} GB")
        logger.info(f"  - 系统可用内存: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.2f} GB")
    except Exception as e:
        logger.error(f"获取系统信息失败: {e}")

if __name__ == '__main__':
    # 设置日志
    logger, log_file, debug_log_file = setup_logging()

    logger.info("=== 开始内存优化测试 ===")
    log_system_info(logger, "程序启动")

    try:
        # 初始化 QLib 时使用更保守的设置
        logger.info("步骤1: 开始初始化 QLib...")
        logger.debug("[DEBUG] 即将调用log_system_info(Qlib初始化前)")
        log_system_info(logger, "QLib初始化前")

        start_time = time.time()
        logger.debug("[DEBUG] 调用qlib.init前")
        qlib.init(
            provider_uri="C:/Users/ASUS/Downloads/qlib_data",
            region="us",
            # 添加内存优化配置
            mem_cache_size_limit=1024,  # 限制内存缓存为1GB
            mem_cache_expire=3600       # 缓存过期时间1小时
        )
        logger.debug("[DEBUG] 调用qlib.init后")
        init_time = time.time() - start_time
        logger.info(f"   QLib 初始化完成，耗时: {init_time:.2f}秒")
        log_system_info(logger, "QLib初始化后")

        # 读取数据并严格限制数据量
        logger.info("步骤2: 开始读取预测数据...")
        logger.debug("[DEBUG] 数据读取前log_system_info")
        log_system_info(logger, "数据读取前")

        start_time = time.time()
        logger.debug("[DEBUG] pd.read_csv前")
        pred_score = pd.read_csv("C:/Users/ASUS/Downloads/预测数据_考虑相关系数.csv")
        logger.debug("[DEBUG] pd.read_csv后")
        read_time = time.time() - start_time
        logger.info(f"   数据读取完成，原始数据: {len(pred_score)} 行，耗时: {read_time:.2f}秒")
        logger.info(f"   数据列: {list(pred_score.columns)}")
        log_system_info(logger, "数据读取后")

        # 只取前20行数据，并且只选择必要的列
        logger.info("步骤3: 开始筛选数据...")
        logger.debug(f"[DEBUG] 筛选前数据列: {list(pred_score.columns)}")
        # 只保留必要的列
        required_cols = ['datetime', 'instrument', 'mu']  # 只保留最基本的列
        if 'mu' not in pred_score.columns:
            # 如果没有mu列，使用第一个数值列
            numeric_cols = pred_score.select_dtypes(include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                required_cols = ['datetime', 'instrument', numeric_cols[0]]
                logger.info(f"   使用列: {required_cols}")

        logger.debug(f"[DEBUG] required_cols: {required_cols}")
        pred_score = pred_score[required_cols].head(20)  # 只取20行
        logger.debug(f"[DEBUG] 筛选后数据: {pred_score.shape}")
        logger.info(f"   筛选后数据: {len(pred_score)} 行")
        log_system_info(logger, "数据筛选后")

        # 重命名信号列为score
        logger.info("步骤4: 开始数据预处理...")
        signal_col = required_cols[2]
        logger.debug(f"[DEBUG] 信号列: {signal_col}")
        pred_score = pred_score.rename(columns={signal_col: 'score'})
        logger.info(f"   信号列 '{signal_col}' 已重命名为 'score'")

        # 数据预处理
        start_time = time.time()
        logger.info("   转换datetime列...")
        logger.debug("[DEBUG] pd.to_datetime前")
        pred_score["datetime"] = pd.to_datetime(pred_score["datetime"])
        logger.debug("[DEBUG] pd.to_datetime后")

        logger.info("   设置多级索引...")
        logger.debug("[DEBUG] set_index前")
        pred_score = pred_score.set_index(["instrument", "datetime"]).sort_index()
        logger.debug("[DEBUG] set_index后")
        process_time = time.time() - start_time
        logger.info(f"   数据预处理完成，耗时: {process_time:.2f}秒")

        # 打印pred_score
        print(pred_score)
        logger.info(f"pred_score预览:\n{pred_score}")

        # 强制垃圾回收
        logger.info("   执行垃圾回收...")
        logger.debug("[DEBUG] gc.collect前")
        gc.collect()
        logger.debug("[DEBUG] gc.collect后")
        log_system_info(logger, "数据预处理后")

        # 显示数据信息
        date_range = pred_score.index.get_level_values('datetime')
        stock_count = pred_score.index.get_level_values('instrument').nunique()
        logger.info(f"   时间范围: {date_range.min()} 到 {date_range.max()}")
        logger.info(f"   股票数量: {stock_count}")
        logger.info(f"   数据形状: {pred_score.shape}")

        # 使用最小配置
        logger.info("步骤5: 准备策略配置...")
        STRATEGY_CONFIG = {
            "topk": 50,      # 选50只股票
            "n_drop": 5,     # 丢弃5只
            "signal": pred_score,
        }
        logger.debug(f"[DEBUG] STRATEGY_CONFIG: {STRATEGY_CONFIG}")
        logger.info(f"   策略配置: {STRATEGY_CONFIG}")
        log_system_info(logger, "策略配置前")

        logger.info("   创建策略对象...")
        start_time = time.time()
        logger.debug("[DEBUG] TopkDropoutStrategy前")
        strategy_obj = TopkDropoutStrategy(**STRATEGY_CONFIG)
        logger.debug("[DEBUG] TopkDropoutStrategy后")
        strategy_time = time.time() - start_time
        logger.info(f"   策略创建完成，耗时: {strategy_time:.2f}秒")
        log_system_info(logger, "策略创建后")

        # 新的回测区间
        test_start = "2023-01-01"
        test_end = "2025-05-07"
        logger.info(f"步骤6: 开始回测 ({test_start} 到 {test_end})...")
        logger.warning("注意: 即将开始回测，这是最可能导致程序无响应的步骤")
        log_system_info(logger, "回测开始前")

        start_time = time.time()
        logger.info("   调用 backtest_daily 函数...")
        logger.debug("[DEBUG] backtest_daily前")
        report_normal, positions_normal = backtest_daily(
            start_time=test_start,
            end_time=test_end,
            strategy=strategy_obj,
            benchmark='NASDAQ'
        )
        logger.debug("[DEBUG] backtest_daily后")
        backtest_time = time.time() - start_time
        logger.info(f"   回测完成，耗时: {backtest_time:.2f}秒")
        log_system_info(logger, "回测完成后")

        logger.info("步骤7: 处理回测结果...")
        logger.debug(f"[DEBUG] report_normal.shape: {getattr(report_normal, 'shape', None)}")
        logger.debug(f"[DEBUG] positions_normal.shape: {getattr(positions_normal, 'shape', None)}")
        logger.info(f"   报告形状: {report_normal.shape}")

        # 显示简单结果
        logger.info("=== 回测结果摘要 ===")
        logger.info("报告数据:")
        logger.info(f"{report_normal}")

        logger.info("=== 测试成功完成 ===")
        log_system_info(logger, "程序结束")
        logger.info(f"日志已保存到文件: {log_file}")
        logger.info(f"详细debug日志已保存到文件: {debug_log_file}")

    except (KeyboardInterrupt, SystemExit) as e:
        logger.error(f"程序被中断: {e}")
        log_system_info(logger, "程序被中断")
        raise
    except Exception as e:
        logger.error(f"程序执行过程中出错: {e}")
        logger.error("详细错误信息:")
        import traceback
        logger.error(traceback.format_exc())

        log_system_info(logger, "程序异常结束")

        logger.error("\n建议:")
        logger.error("1. 检查系统内存是否充足")
        logger.error("2. 关闭其他占用内存的程序")
        logger.error("3. 考虑使用更小的数据集")
        logger.error("4. 检查数据文件是否存在和格式是否正确")
        logger.error(f"5. 查看详细日志文件: {log_file}")
        logger.error(f"6. 查看详细debug日志文件: {debug_log_file}")
    finally:
        # 确保日志被写入文件
        logging.shutdown()
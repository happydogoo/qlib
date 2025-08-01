#!/usr/bin/env python3
"""
系统监控脚本
在运行 test.py 时，可以同时运行此脚本来监控系统状态
"""

import psutil
import time
import logging
from datetime import datetime
import os

def setup_monitor_logging():
    """设置监控日志"""
    log_filename = f"system_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - MONITOR - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"系统监控日志文件: {log_filename}")
    return logger, log_filename

def find_python_processes():
    """查找所有Python进程"""
    python_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent']):
        try:
            if 'python' in proc.info['name'].lower():
                python_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return python_processes

def monitor_system(logger, duration_minutes=10, interval_seconds=5):
    """监控系统状态"""
    logger.info(f"开始监控系统，持续时间: {duration_minutes}分钟，间隔: {interval_seconds}秒")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    while time.time() < end_time:
        try:
            # 系统整体状态
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            logger.info("=== 系统状态 ===")
            logger.info(f"CPU使用率: {cpu_percent:.1f}%")
            logger.info(f"内存使用: {memory.percent:.1f}% ({memory.used/1024/1024/1024:.2f}GB / {memory.total/1024/1024/1024:.2f}GB)")
            logger.info(f"可用内存: {memory.available/1024/1024/1024:.2f}GB")
            
            # 查找Python进程
            python_processes = find_python_processes()
            if python_processes:
                logger.info("=== Python进程 ===")
                for proc in python_processes:
                    try:
                        memory_mb = proc.info['memory_info'].rss / 1024 / 1024
                        cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else 'N/A'
                        logger.info(f"PID {proc.info['pid']}: {memory_mb:.1f}MB - {cmdline[:100]}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            
            # 检查是否有进程无响应
            if cpu_percent > 90:
                logger.warning("⚠️  CPU使用率过高!")
            
            if memory.percent > 90:
                logger.warning("⚠️  内存使用率过高!")
            
            # 检查是否有僵尸进程
            zombie_count = 0
            for proc in psutil.process_iter():
                try:
                    if proc.status() == psutil.STATUS_ZOMBIE:
                        zombie_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            if zombie_count > 0:
                logger.warning(f"⚠️  发现 {zombie_count} 个僵尸进程")
            
            logger.info("-" * 50)
            
        except Exception as e:
            logger.error(f"监控过程中出错: {e}")
        
        time.sleep(interval_seconds)
    
    logger.info("监控结束")

if __name__ == '__main__':
    logger, log_file = setup_monitor_logging()
    
    print("系统监控脚本")
    print("=" * 50)
    print("此脚本将监控系统状态，特别是Python进程的资源使用情况")
    print("建议在运行 test.py 之前启动此脚本")
    print(f"监控日志将保存到: {log_file}")
    print("=" * 50)
    
    try:
        # 默认监控10分钟，每5秒记录一次
        monitor_system(logger, duration_minutes=10, interval_seconds=5)
    except KeyboardInterrupt:
        logger.info("用户中断监控")
    except Exception as e:
        logger.error(f"监控脚本出错: {e}")
        import traceback
        logger.error(traceback.format_exc())

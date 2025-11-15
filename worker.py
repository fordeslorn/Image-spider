import sys
import os
import json
import logging
import signal
from pathlib import Path
from contextlib import contextmanager

# 确保能找到 src 模块
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from src.spiders.pixiv import PixivSpider

@contextmanager
def set_custom_pipelines(settings, mode: str):
    """动态设置 Scrapy 管道"""
    original_pipelines = settings.getdict('ITEM_PIPELINES')
    if mode == 'image':
        settings.set('ITEM_PIPELINES', {
            'src.pipelines.CustomImagesPipeline': 1,
            'src.pipelines.ApiDataCollectorPipeline': 300,
        })
        settings.set('IMAGES_STORE', '.download_imgs')
    else:
        settings.set('ITEM_PIPELINES', {'src.pipelines.ApiDataCollectorPipeline': 300})
    try:
        yield
    finally:
        settings.set('ITEM_PIPELINES', original_pipelines)


# ========== 新增：自定义日志处理器（用于收集日志） ==========
class ListHandler(logging.Handler):
    """将日志收集到列表中的处理器"""
    def __init__(self):
        super().__init__()
        self.log_lines = []  # 存储日志的列表
    
    def emit(self, record):
        """每条日志都会调用这个方法"""
        try:
            # 格式化日志并添加到列表
            msg = self.format(record)
            self.log_lines.append(msg)
        except Exception:
            self.handleError(record)
    
    def get_logs(self):
        """获取收集到的所有日志"""
        return self.log_lines

def setup_logging(log_file: str):
    """配置日志"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # ========== 第 1 步：完全禁用 Scrapy 的日志系统 ==========
    from scrapy.utils.log import configure_logging
    configure_logging(install_root_handler=False)  # 必须在最开始调用
    
    # ========== 第 2 步：清除所有现有的处理器 ==========
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.setLevel(logging.INFO)  # 改为 INFO，减少噪音
    
    # ========== 第 3 步：创建处理器 ==========
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    list_handler = ListHandler()
    list_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    list_handler.setFormatter(formatter)
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(list_handler)
    
    # ========== 第 4 步：彻底禁用 Scrapy 各模块的日志传播 ==========
    for logger_name in ['scrapy', 'pixiv', 'src', 'py.warnings', 'filelock']:
        module_logger = logging.getLogger(logger_name)
        module_logger.propagate = False  # 不向上传播
        module_logger.setLevel(logging.WARNING)  # 只记录警告及以上级别
    
    return list_handler

def read_jsonl_file(file_path: str) -> list:
    """
    读取 JSONL 格式的文件
    
    参数:
        file_path: 文件路径
    
    返回:
        数据列表
    """
    data = []
    if not os.path.exists(file_path):
        logging.getLogger(__name__).warning(f"\033[33mFile not found: {file_path}\033[0m")
        return data
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            # 如果文件为空，返回空列表
            if not content:
                return data
            
            # 尝试按行解析 JSONL
            for line_num, line in enumerate(content.splitlines(), 1):
                line = line.strip()
                if not line:  # 跳过空行
                    continue
                
                try:
                    # 解析每一行的 JSON
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logging.getLogger(__name__).error(
                        f"\033[31mError parsing line {line_num} in {file_path}: {e}\033[0m"
                    )
                    logging.getLogger(__name__).debug(f"Problematic line: {line[:100]}...")
                    continue
    except Exception as e:
        logging.getLogger(__name__).error(f"\033[31mError reading {file_path}: {e}\033[0m")
    
    return data


# ========== 新增：超时处理器（仅限 Unix 系统） ==========
class TimeoutError(Exception):
    """超时异常"""
    pass

def timeout_handler(signum, frame):
    """超时信号处理函数"""
    raise TimeoutError("Spider execution timeout (1 hour)")


def run_spider(task_id: str, user_id: str, cookie: str, mode: str, result_file: str):
    """主要入口点 - 使用 CrawlerProcess 运行爬虫"""
    # 新增：确认 worker.py 被调用
    print(f"========== WORKER.PY STARTED ==========")
    print(f"task_id: {task_id}")
    print(f"user_id: {user_id}")
    print(f"mode: {mode}")
    print(f"result_file: {result_file}")
    print(f"cookie length: {len(cookie)}")
    print("=" * 50)

    log_dir = os.path.join(os.path.dirname(__file__), '.task_logs')
    log_file = os.path.join(log_dir, f'{task_id}.log')
    
    # ========== 修改：setup_logging 现在返回 list_handler ==========
    list_handler = setup_logging(log_file)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting spider for user {user_id} with mode {mode}")

    # ========== 新增：设置超时（仅限 Unix/Linux/Mac） ==========
    timeout_set = False
    if hasattr(signal, 'SIGALRM'):  # Windows 不支持 SIGALRM
        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(3600)  # 1 小时超时
            timeout_set = True
            logger.info(f"\033[34m[{task_id}] Timeout set to 1 hour\033[0m")
        except Exception as e:
            logger.warning(f"\033[33m[{task_id}] Failed to set timeout: {e}\033[0m")

    try:
        settings = get_project_settings()
        
        # 关键设置
        settings.set('HTTPCACHE_ENABLED', False)
        settings.set('LOG_ENABLED', True)
        settings.set('LOG_LEVEL', 'INFO')
        
        logger.info(f"\033[34m[{task_id}] Creating CrawlerProcess...\033[0m")
        
        # 使用 CrawlerProcess 而不是 CrawlerRunner + asyncio
        # CrawlerProcess 会创建自己的事件循环和反应器，完全独立
        process = CrawlerProcess(settings)
        
        with set_custom_pipelines(settings, mode):
            logger.info(f"\033[34m[{task_id}] Starting crawl...\033[0m")
            
            # crawl() 方法会将爬虫添加到队列中
            process.crawl(
                PixivSpider,
                task_id=task_id,
                user_id=user_id,
                cookie=cookie
            )
            
            # start() 是同步阻塞的，会运行所有加入的爬虫，直到全部完成
            logger.info(f"\033[34m[{task_id}] Calling process.start() - this will block until spider finishes...\033[0m")

            # 新增：确认是否真的调用
            print(f"[DEBUG] About to call process.start()")
            process.start()
            print(f"[DEBUG] process.start() returned!")

            logger.info(f"\033[34m[{task_id}] process.start() returned - crawler finished!\033[0m")
        
        logger.info(f"\033[34m[{task_id}] Crawling completed successfully!\033[0m")

        # 读取数据文件
        data_file = str(Path(__file__).parent / '.task_data' / f'{task_id}.jsonl')
        images_file = str(Path(__file__).parent / '.task_data' / f'{task_id}_images.jsonl')
        
        results = read_jsonl_file(data_file)
        images = read_jsonl_file(images_file)
        
        logger.info(f"\033[34m[{task_id}] Loaded {len(results)} data items and {len(images)} images\033[0m")

        # ========== 修改：从 list_handler 获取日志 ==========
        result = {
            "status": "completed",
            "mode": mode,
            "logs": list_handler.get_logs(),  # 包含所有收集到的日志
            "results": results,
            "images": images
        }
    
    # ========== 新增：细分异常处理 ==========
    except KeyboardInterrupt:
        logger.warning(f"\033[33m[{task_id}] Task cancelled by user\033[0m")
        result = {
            "status": "cancelled",
            "mode": mode,
            "logs": list_handler.get_logs(),
            "results": [],
            "images": [],
            "error": "Task was cancelled by user"
        }
    except TimeoutError as e:
        logger.error(f"\033[31m[{task_id}] Task timeout: {e}\033[0m")
        result = {
            "status": "timeout",
            "mode": mode,
            "logs": list_handler.get_logs(),
            "results": [],
            "images": [],
            "error": str(e)
        }
    except Exception as e:
        logger.exception(f"\033[31m[{task_id}] Error during crawling: {e}\033[0m")
        result = {
            "status": "failed",
            "mode": mode,
            "logs": list_handler.get_logs(),
            "results": [],
            "images": [],
            "error": str(e),
            "error_type": type(e).__name__
        }
    finally:
        # ========== 新增：取消超时 ==========
        if timeout_set:
            signal.alarm(0)
            logger.info(f"\033[34m[{task_id}] Timeout cancelled\033[0m")

    # 写入最终结果文件
    try:
        # 处理结果文件路径
        result_path = Path(result_file)
        
        # 如果路径没有父目录（如 "test_result.json"），使用当前目录
        if result_path.parent == Path('.'):
            result_path = Path(__file__).parent / result_file
        
        # 创建父目录
        result_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入文件
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\033[34m[{task_id}] Result saved to {result_path}\033[0m")
    except Exception as e:
        logger.error(f"\033[31m[{task_id}] Failed to save result: {e}\033[0m")
    



if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Usage: python worker.py <task_id> <user_id> <cookie> <mode> <result_file>")
        print("Example: python worker.py abc-123 66330905 'PHPSESSID=...' image results.json")
        sys.exit(1)
    
    task_id = sys.argv[1]
    user_id = sys.argv[2]
    cookie = sys.argv[3]
    mode = sys.argv[4]
    result_file = sys.argv[5]
    run_spider(task_id, user_id, cookie, mode, result_file)
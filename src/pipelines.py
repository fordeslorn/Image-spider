import os
import json
import logging
from pathlib import Path
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline

logger = logging.getLogger(__name__)

class ApiDataCollectorPipeline:
    """
    收集爬取到的数据并写入文件。
    支持开发环境和打包后的环境。
    """
    
    def open_spider(self, spider):
        """爬虫启动时调用"""
        self.task_id = getattr(spider, 'task_id', None)
        
        if not self.task_id:
            logger.warning("\033[33mtask_id not found in spider\033[0m")
            self.data_file = None
            return
        
        # 确定数据文件的存储路径
        # 优先使用相对路径（兼容打包后的应用）
        self.data_file = self._get_data_file_path()
        
        # 创建目录
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        
        # 初始化文件：写入空数组
        try:
            # 如果文件已存在，删除旧文件
            if os.path.exists(self.data_file):
                os.remove(self.data_file)
                logger.info(f"\033[33m[{self.task_id}] Removed old data file: {self.data_file}\033[0m")
            
            # 创建空文件（mode='w' 覆盖模式）
            with open(self.data_file, 'w', encoding='utf-8') as f:
                pass  # 创建空文件
            
            logger.info(f"\033[34m[{self.task_id}] Data file created: {self.data_file}\033[0m")
        except Exception as e:
            logger.error(f"\033[31m[{self.task_id}] Failed to create data file: {e}\033[0m")

        self.buffer = []
        self.buffer_size = 30
    
    def _get_data_file_path(self):
        """
        获取数据文件路径。
        兼容打包后的应用：优先使用相对路径
        """
        task_id = self.task_id
        
        # 方案 1：使用相对路径（推荐，兼容打包）
        # 将文件存储在项目根目录的 .task_data 文件夹中
        data_dir = Path(__file__).parent.parent / '.task_data'
        
        # 方案 2：如果上面的路径找不到，使用临时目录
        if not data_dir.exists():
            try:
                data_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                # 最后的备选：使用系统临时目录
                import tempfile
                data_dir = Path(tempfile.gettempdir()) / 'pixiv-spider' / '.task_data'
                data_dir.mkdir(parents=True, exist_ok=True)
        
        return str(data_dir / f'{task_id}.jsonl')
    
    def process_item(self, item, spider):
        """处理每一个数据项"""
        if not self.data_file:
            return item
        
        try:
            # 将 Item 转换为字典
            item_dict = ItemAdapter(item).asdict()
            
            # ========== 只添加到缓冲区 ==========
            self.buffer.append(item_dict)
            
            # 当缓冲区满了才写入（提高性能）
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()
                
        except Exception as e:
            logger.error(f"\033[31m[{self.task_id}] Error processing item: {e}\033[0m")
        
        return item
    
    def _flush_buffer(self):
        """将缓冲区数据写入文件（追加模式）"""
        if not self.buffer or not self.data_file:
            return
        
        try:
            # ========== 这里应该是 'a'（追加模式）==========
            with open(self.data_file, 'a', encoding='utf-8') as f:  # ← 确认是 'a'
                for item in self.buffer:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
            logger.info(f"\033[34m[{self.task_id}] Flushed {len(self.buffer)} items to {self.data_file}\033[0m")
            self.buffer = []  # 清空缓冲区
        except Exception as e:
            logger.error(f"\033[31m[{self.task_id}] Failed to flush buffer: {e}\033[0m")

    def close_spider(self, spider):
        """爬虫关闭时调用"""
        self._flush_buffer()
        
        if self.data_file and os.path.exists(self.data_file):
            logger.info(f"\033[34m[{self.task_id}] Data collection completed: {self.data_file}\033[0m")


class CustomImagesPipeline(ImagesPipeline):
    """
    处理图片下载并记录下载结果。
    支持开发环境和打包后的环境。
    """
    
    def open_spider(self, spider):
        """爬虫启动时调用"""
        super().open_spider(spider)
        
        self.task_id = getattr(spider, 'task_id', None)
        self.images_file = self._get_images_file_path() if self.task_id else None
        
        if self.images_file:
            os.makedirs(os.path.dirname(self.images_file), exist_ok=True)
            try:
                if os.path.exists(self.images_file):
                    os.remove(self.images_file)
                    logger.info(f"\033[33m[{self.task_id}] Removed old images file: {self.images_file}\033[0m")
                
                # 创建空文件
                with open(self.images_file, 'w', encoding='utf-8') as f:
                    pass
                logger.info(f"\033[34m[{self.task_id}] Images file created: {self.images_file}\033[0m")
            except Exception as e:
                logger.error(f"\033[31m[{self.task_id}] Failed to create images file: {e}\033[0m")
                
    def _get_images_file_path(self):
        """获取图片列表文件路径"""
        task_id = self.task_id
        
        # 同数据文件的逻辑
        images_dir = Path(__file__).parent.parent / '.task_data'
        
        if not images_dir.exists():
            try:
                images_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                import tempfile
                images_dir = Path(tempfile.gettempdir()) / 'pixiv-spider' / '.task_data'
                images_dir.mkdir(parents=True, exist_ok=True)
        
        return str(images_dir / f'{task_id}_images.jsonl')
    
    def item_completed(self, results, item, info):
        """图片下载完成时调用"""
        if not self.images_file:
            return item
        
        try:
            successful = []
            failed = []
            
            for ok, x in results:
                if ok:
                    successful.append({
                        'url': x.get('url', ''),
                        'path': x.get('path', ''),
                        'checksum': x.get('checksum', ''),
                        'status': 'success'  # ← 新增
                    })
                else:
                    # ========== 新增：记录失败的图片 ==========
                    failed_info = {
                        'url': x.value.get('url', '') if hasattr(x, 'value') else 'unknown',
                        'status': 'failed',
                        'error': str(x.getErrorMessage()) if hasattr(x, 'getErrorMessage') else str(x)
                    }
                    failed.append(failed_info)
            
            # ========== 保存所有结果（包括失败的）==========
            with open(self.images_file, 'a', encoding='utf-8') as f:
                for img in successful:
                    f.write(json.dumps(img, ensure_ascii=False) + '\n')
                    filename = img['path'].split('/')[-1]
                    logger.info(f"\033[32m[{self.task_id}] ✅ Downloaded: {filename}\033[0m")
                
                for img in failed:
                    f.write(json.dumps(img, ensure_ascii=False) + '\n')  # ← 记录失败
                    logger.warning(
                        f"\033[33m[{self.task_id}] ⚠ Download failed: {img['url']}\033[0m"
                    )
                    logger.warning(
                        f"\033[33m[{self.task_id}]   Error: {img['error'][:100]}\033[0m"
                    )
            
            logger.info(
                f"\033[34m[{self.task_id}] Download summary: "
                f"{len(successful)} succeeded, {len(failed)} failed\033[0m"
            )
                
        except Exception as e:
            logger.error(f"\033[31m[{self.task_id}] Error saving image info: {e}\033[0m")
        
        return item
    
    def close_spider(self, spider):
        """爬虫关闭时调用"""
        if self.images_file and os.path.exists(self.images_file):
            logger.info(f"\033[34m[{self.task_id}] Image collection completed: {self.images_file}\033[0m")
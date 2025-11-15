import sys
import os

# 添加 src 目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from src.spiders.pixiv import PixivSpider

def test_spider():
    """直接测试爬虫"""
    
    # 你的 Pixiv Cookie（从浏览器复制）
    cookie = "first_visit_datetime_pc=2025-05-20%2023%3A38%3A01; p_ab_id=8; p_ab_id_2=3; p_ab_d_id=74108474; yuid_b=EAgDlxU; privacy_policy_agreement=7; privacy_policy_notification=0; a_type=0; c_type=25; b_type=0; login_ever=yes; PHPSESSID=120298966_5ByK1EFATjPaApVObYppBLs42UXQ3DXo; device_token=7202f744b4ab9a791067c54582576bf7; _cfuvid=.HSyW6Asq0yaNsSg1zfqbgdcBlpgMnxmoe6ZLQ1N5fw-1763177728706-0.0.1.1-604800000; cf_clearance=OWllTT4xhVzNNmAE6kSknjKKDF_nT4XCo6G_5XEvezU-1763177729-1.2.1.1-pW2yBi77zGMv9AlZaZudsiX60f7I1v8QU1AKFVZguvA_brVl9WuDDJPlCpXt9UyanAJt0SQCnorCDWv376xEgrr220lkEqfwIFgswGKa4AfgTEiWAjM3m31qxs.0_0DucKUpvTnQQ6GSxEi5pLwCGlqaKQCUFGUvVHzau7g8gsc8r9YUoQrjqmKp63vIkMeuN7bz4LmoIbXbMH52GZNChsQdyynQ0ZhYuYcKyscD.Z8; __cf_bm=dXHhLGQsRK.MHkL4yEbBCrlZhR.UwXXbZ6BVoG_fBPY-1763179599-1.0.1.1-Hav_97wuSi.p7UI728PD_bqZ2d1DMoyU_TzoArtCZd_90WlHSVWcJI9hKxQXVXtkO8km0HI.vBkHPvB8kqeSef2MfbrLeV0VhRj3zSlAF.2Gofq7raiGoI1PYbSPj_BJ"  # 替换为真实 cookie
    
    # 目标用户 ID
    user_id = "66330905"
    
    # 获取项目设置
    settings = get_project_settings()
    
    # 临时修改设置（方便调试）
    settings.set('LOG_LEVEL', 'DEBUG')  # 显示详细日志
    settings.set('IMAGES_STORE', 'test_images')  # 图片保存到 test_images 目录
    settings.set('ITEM_PIPELINES', {
        'src.pipelines.CustomImagesPipeline': 1,
        'src.pipelines.ApiDataCollectorPipeline': 300,
    })
    
    # 创建爬虫进程
    process = CrawlerProcess(settings)
    
    # 启动爬虫
    process.crawl(
        PixivSpider,
        task_id='test-001',
        user_id=user_id,
        cookie=cookie
    )
    
    print(f"开始爬取用户 {user_id} 的作品...")
    process.start()  # 阻塞直到完成

if __name__ == '__main__':
    test_spider()
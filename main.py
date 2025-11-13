import os
from dotenv import load_dotenv

print("loading .env file...")
load_dotenv()
print("loading .env file completed.")

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from src.spiders.pixiv import PixivSpider 

def crawl_pixiv_data():
    if not os.environ.get("PIXIV_COOKIES"):
        print("\n\033[31merror: cannot find PIXIV_COOKIES in environment variables!\033[0m")
        print("\033[33mPlease ensure that the .env file is in the project root directory and the variable name is correct.\n\033[0m")
        return

    print("\033[32malready starting crawler...\033[0m")
    
    # 获取项目设置 
    settings = get_project_settings()
    settings.set('FEEDS', {
        'pixiv.json': {
            'format': 'json',
            'encoding': 'utf8',
            'store_empty': False,
            'fields': None,
            'indent': 4,
        }
    })

    process = CrawlerProcess(settings)
    process.crawl(PixivSpider) # 告诉 process 要运行哪个爬虫
    process.start() # 脚本会在这里阻塞，直到爬虫运行结束

def crawl_pixiv_image():
    # 需要开启 pixiv.py 中的 "scrapy.pipelines.images.ImagesPipeline"管道

    if not os.environ.get("PIXIV_COOKIES"):
        print("\n\033[31merror: cannot find PIXIV_COOKIES in environment variables!\033[0m")
        print("\033[33mPlease ensure that the .env file is in the project root directory and the variable name is correct.\n\033[0m")
        return

    print("\033[32malready starting crawler...\033[0m")
     
    process = CrawlerProcess(get_project_settings())
    process.crawl(PixivSpider)  
    process.start()  

if __name__ == "__main__":
    # crawl_pixiv_data()
    crawl_pixiv_image()

import scrapy
import json
import os
import logging
from ..items import PixivItem

class PixivSpider(scrapy.Spider):
    name = "pixiv"
    allowed_domains = ["www.pixiv.net", "pximg.net"]
    # start_urls = ["https://www.pixiv.net"]
    user_id = os.environ.get("userId", "").strip()
    
    custom_settings = {  # Scrapy 内置的 Spider 类属性（一个 dict），其值会在该爬虫运行时覆盖项目级 settings
        "IMAGES_STORE": "download_img/pixiv_images",
        "ITEM_PIPELINES": {
            "scrapy.pipelines.images.ImagesPipeline": 1
        },
    }

    async def start(self):
        my_cookies = os.environ.get("PIXIV_COOKIES")

        if not my_cookies:
            self.log("\033[31merror: PIXIV_COOKIES not found!\033[0m", level=scrapy.log.ERROR)
            return

        cookies_dict = {i.split('=')[0].strip():'='.join(i.split('=')[1:]).strip() for i in my_cookies.split(';')}

        api_url = f'https://www.pixiv.net/ajax/user/{self.user_id}/profile/all'

        yield scrapy.Request(
            url=api_url,
            cookies=cookies_dict,
            callback=self.parse_api
        )

    def parse_api(self, response):
        self.log(f"\033[32msuccessfully fetched API response for user {self.user_id}\033[0m")
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.log(f"\033[31mAPI response is not valid JSON: {response.text}\033[0m", level=scrapy.log.ERROR)
            return

        if data.get('error'):
            self.log(f"\033[31mAPI returned an error: {data.get('message')}\033[0m", level=scrapy.log.ERROR)
            return

        # 根据API结构来遍历所有作品的ID
        illusts = data.get('body', {}).get('illusts', {})
        if not illusts:
            self.log("\033[31merror: no illustration information found in API response.\033[0m", level=scrapy.log.WARNING)
            return

        self.log(f"\033[32mfound {len(illusts)} illustrations, preparing to fetch details one by one...\033[0m")
        for illust_id in illusts.keys():
            # 为每个作品构造其详情页的 API URL
            illust_detail_api_url = f"https://www.pixiv.net/ajax/illust/{illust_id}"

            # 发出新的请求去获取单个作品的详情，并交给下一个解析方法处理
            yield response.follow(illust_detail_api_url, callback=self.parse_illust_detail)

    def parse_illust_detail(self, response):
        data = json.loads(response.text)
        if data.get('error'):
            self.log(f"\033[31merror: failed to fetch illustration details: {data.get('message')}\033[0m", level=scrapy.log.ERROR)
            return

        body = data.get('body', {})
        original_url = body.get('urls', {}).get('original')
        user_id = body.get('userId')
        user_name = body.get('userName')
        
        if original_url:
            self.log(f"\033[32msuccessfully extracted image URL: {original_url}\033[0m")
            item = PixivItem()

            item['user_id'] = user_id
            item['user_name'] = user_name
            item['image_urls'] = [original_url]

            yield item
        else:
            self.log(f"\033[33m[Warning]\033[0m 在作品详情中信息不完整: {response.url}", level=logging.WARNING)



import scrapy
import json
from ..items import PixivItem

class PixivSpider(scrapy.Spider):
    name = "pixiv"
    allowed_domains = ["www.pixiv.net", "pximg.net"]

    def __init__(self, user_id=None, cookie=None, task_id=None, *args, **kwargs):
        super(PixivSpider, self).__init__(*args, **kwargs)
        if not all([user_id, cookie, task_id]):
            raise ValueError("爬虫必须提供 user_id, cookie, 和 task_id")
        
        self.user_id = user_id
        self.cookie_str = cookie
        self.task_id = task_id
        
        self.logger.info(f"\033[32m[{self.task_id}] PixivSpider initialized with user_id={self.user_id}\033[0m")

    def start_requests(self):
        self.logger.info(f"\033[34m[{self.task_id}] ========== START_REQUESTS CALLED ==========\033[0m")
        
        try:
            # 解析 cookie
            self.logger.info(f"\033[34m[{self.task_id}] Raw cookie string length: {len(self.cookie_str)}\033[0m")
            
            cookies_dict = {}
            for item in self.cookie_str.split(';'):
                item = item.strip()
                if '=' in item:
                    key, value = item.split('=', 1)
                    cookies_dict[key.strip()] = value.strip()
            
            self.logger.info(f"\033[34m[{self.task_id}] Parsed {len(cookies_dict)} cookies\033[0m")
            self.logger.info(f"\033[34m[{self.task_id}] Cookie keys: {list(cookies_dict.keys())}\033[0m")
            
            if not cookies_dict:
                self.logger.error(f"\033[31m[{self.task_id}] ❌ Cookie parsing failed! No cookies found\033[0m")
                return
            
            api_url = f'https://www.pixiv.net/ajax/user/{self.user_id}/profile/all'
            self.logger.info(f"\033[34m[{self.task_id}] ✓ Requesting API: {api_url}\033[0m")
            
            request = scrapy.Request(
                url=api_url,
                cookies=cookies_dict,
                callback=self.parse_api,
                errback=self.errback_parse_api,
                dont_filter=True # 加这个，避免 robots.txt 阻止
            )
            
            self.logger.info(f"\033[34m[{self.task_id}] ✓ Yielding request to {api_url}\033[0m")
            yield request
            self.logger.info(f"\033[34m[{self.task_id}] ✓ Request yielded successfully\033[0m")
            
        except Exception as e:
            self.logger.exception(f"\033[31m[{self.task_id}] ❌ Error in start_requests: {e}\033[0m")
            raise

    def errback_parse_api(self, failure):
        """处理请求失败"""
        self.logger.error(f"\033[31m[{self.task_id}] ❌ API request failed!\033[0m")
        self.logger.error(f"\033[31m[{self.task_id}] Error type: {failure.type.__name__}\033[0m")
        self.logger.error(f"\033[31m[{self.task_id}] Error value: {failure.value}\033[0m")

    def parse_api(self, response):
        self.logger.info(f"\033[34m[{self.task_id}] ========== PARSE_API CALLED ==========\033[0m")
        self.logger.info(f"\033[34m[{self.task_id}] Response status: {response.status}\033[0m")
        
        # ========== 添加：打印响应内容（前 500 字符）==========
        self.logger.debug(f"\033[36m[{self.task_id}] Response preview: {response.text[:500]}...\033[0m")
        
        try:
            data = json.loads(response.text)
            
            if data.get('error'):
                self.logger.error(
                    f"\033[31m[{self.task_id}] ❌ API error: {data.get('message')}\033[0m"
                )
                return
            
            illusts: dict = data.get('body', {}).get('illusts', {})
            self.logger.info(f"\033[34m[{self.task_id}] ✓ Found {len(illusts)} illustrations\033[0m")
            
            if not illusts:
                self.logger.warning(
                    f"\033[33m[{self.task_id}] ⚠ No illustrations found. "
                    f"This might indicate an invalid cookie or private profile.\033[0m"
                )
                return

            count = 0
            for illust_id in illusts.keys():
                illust_detail_url = f"https://www.pixiv.net/ajax/illust/{illust_id}"
                count += 1
                
                # ========== 只打印前 10 个，避免日志过长 ==========
                if count <= 10:
                    self.logger.info(
                        f"\033[34m[{self.task_id}] Queuing detail request {count}: {illust_id}\033[0m"
                    )
                elif count == 11:
                    self.logger.info(
                        f"\033[34m[{self.task_id}] ... and {len(illusts) - 10} more\033[0m"
                    )
                
                yield response.follow(
                    illust_detail_url,
                    callback=self.parse_illust_detail,
                    dont_filter=True
                )
                    
        except Exception as e:
            self.logger.exception(f"\033[31m[{self.task_id}] ❌ Error in parse_api: {e}\033[0m")

    def parse_illust_detail(self, response):
        """解析作品详情"""
        try:
            data = json.loads(response.text)
            illust_id = response.url.split('/')[-1]
            
            if data.get('error'):
                error_msg = data.get('message', 'Unknown error')
                self.logger.error(
                    f"\033[31m[{self.task_id}] ❌ Illust {illust_id} API error: {error_msg}\033[0m"
                )
                return
            
            body = data.get('body', {})
            
            # ========== 处理多图作品 ==========
            page_count = body.get('pageCount', 1)
            illust_type = body.get('illustType', 0)
            
            image_urls = []
            
            if page_count > 1:
                # 多图作品：需要获取所有分页的 URL
                self.logger.info(
                    f"\033[33m[{self.task_id}] Illust {illust_id} is multi-page ({page_count} pages)\033[0m"
                )
                
                # 请求多图详情 API
                pages_url = f"https://www.pixiv.net/ajax/illust/{illust_id}/pages"
                yield response.follow(
                    pages_url,
                    callback=self.parse_pages,
                    cb_kwargs={'illust_id': illust_id, 'body': body},
                    dont_filter=True
                )
                return  # 不继续执行，等待 parse_pages 处理
            
            # ========== 单图作品 ==========
            urls = body.get('urls', {})
            
            # ========== 关键修复：处理 None 值 ==========
            original_url = urls.get('original') or ''  # ← 改这里！
            original_url = original_url.strip() if original_url else ''
            
            # 或者更简洁的写法：
            # original_url = (urls.get('original') or '').strip()
            
            if not original_url:
                # 尝试备用 URL
                original_url = (
                    (urls.get('regular') or '').strip() or 
                    (urls.get('small') or '').strip()
                )
                
                if original_url:
                    self.logger.info(
                        f"\033[33m[{self.task_id}] Using fallback URL for {illust_id}\033[0m"
                    )
                else:
                    self.logger.error(
                        f"\033[31m[{self.task_id}] ❌ Illust {illust_id} has no usable URL\033[0m"
                    )
                    return
            
            image_urls = [original_url]
            
            # ========== 生成 Item ==========
            user_name = body.get('userName', 'Unknown')
            title = body.get('title', 'Untitled')
            
            self.logger.info(
                f"\033[32m[{self.task_id}] ✅ Illust {illust_id} | "
                f"Title: {title} | {len(image_urls)} image(s)\033[0m"
            )
            
            item = PixivItem()
            item['user_id'] = self.user_id
            item['user_name'] = user_name
            item['image_urls'] = image_urls
            yield item
            
        except Exception as e:
            self.logger.exception(
                f"\033[31m[{self.task_id}] ❌ Error in parse_illust_detail: {e}\033[0m"
            )

    def parse_pages(self, response, illust_id, body):
        """解析多图作品的所有页面"""
        try:
            data = json.loads(response.text)
            
            if data.get('error'):
                self.logger.error(
                    f"\033[31m[{self.task_id}] ❌ Pages API error for {illust_id}\033[0m"
                )
                return
            
            pages = data.get('body', [])
            image_urls = []
            
            for page in pages:
                urls = page.get('urls', {})
                
                # ========== 同样修复：处理 None 值 ==========
                original = (urls.get('original') or '').strip()
                
                if original:
                    image_urls.append(original)
                else:
                    # 备用 URL
                    regular = (urls.get('regular') or '').strip()
                    if regular:
                        image_urls.append(regular)
            
            if image_urls:
                user_name = body.get('userName', 'Unknown')
                title = body.get('title', 'Untitled')
                
                self.logger.info(
                    f"\033[32m[{self.task_id}] ✅ Multi-page illust {illust_id} | "
                    f"Title: {title} | {len(image_urls)} images\033[0m"
                )
                
                item = PixivItem()
                item['user_id'] = self.user_id
                item['user_name'] = user_name
                item['image_urls'] = image_urls
                yield item
            else:
                self.logger.error(
                    f"\033[31m[{self.task_id}] ❌ No images found in multi-page illust {illust_id}\033[0m"
                )
        
        except Exception as e:
            self.logger.exception(
                f"\033[31m[{self.task_id}] ❌ Error parsing pages for {illust_id}: {e}\033[0m"
            )
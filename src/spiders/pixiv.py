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
            
            # 检查 API 错误
            if data.get('error'):
                error_msg = data.get('message', 'Unknown error')
                self.logger.error(
                    f"\033[31m[{self.task_id}] ❌ Illust {illust_id} API error: {error_msg}\033[0m"
                )
                return
            
            body = data.get('body', {})
            urls = body.get('urls', {})
            
            # ========== 关键修复：详细检查每个 URL ==========
            original_url = urls.get('original', '').strip()  # 去除空白字符
            
            # 打印原始值（调试用）
            self.logger.debug(
                f"\033[36m[{self.task_id}] Illust {illust_id} original_url raw value: "
                f"'{original_url}' (type: {type(original_url).__name__}, length: {len(original_url)})\033[0m"
            )
            
            # ========== 如果 original 是空字符串或 None，尝试备用 URL ==========
            if not original_url:  # 空字符串或 None 都会进入这里
                available_urls = list(urls.keys()) if urls else []
                self.logger.warning(
                    f"\033[33m[{self.task_id}] ⚠ Illust {illust_id} 'original' URL is empty/None. "
                    f"Available keys: {available_urls}\033[0m"
                )
                
                # ========== 打印所有 URL 的值（调试用）==========
                for key in available_urls:
                    value = urls.get(key, '')
                    self.logger.debug(
                        f"\033[36m[{self.task_id}]   {key}: '{value}' "
                        f"(length: {len(str(value))})\033[0m"
                    )
                
                # 尝试备用 URL（按优先级）
                original_url = (
                    urls.get('regular', '').strip() or 
                    urls.get('small', '').strip() or 
                    urls.get('thumb', '').strip()
                )
                
                if original_url:
                    self.logger.info(
                        f"\033[33m[{self.task_id}] ℹ Using fallback URL for {illust_id}: "
                        f"{original_url[:60]}...\033[0m"
                    )
                else:
                    # ========== 所有 URL 都是空的 ==========
                    self.logger.error(
                        f"\033[31m[{self.task_id}] ❌ Illust {illust_id} ALL URLs are empty! "
                        f"Dumping full response:\033[0m"
                    )
                    # 打印完整的响应 JSON（方便分析）
                    self.logger.debug(f"\033[36m{json.dumps(body, indent=2, ensure_ascii=False)[:1000]}...\033[0m")
                    return
            
            # ========== 成功提取 URL ==========
            user_name = body.get('userName', 'Unknown')
            title = body.get('title', 'Untitled')
            
            self.logger.info(
                f"\033[32m[{self.task_id}] ✅ Illust {illust_id} | "
                f"Title: {title} | Artist: {user_name}\033[0m"
            )
            self.logger.info(
                f"\033[32m[{self.task_id}]    URL: {original_url}\033[0m"
            )
            
            item = PixivItem()
            item['user_id'] = self.user_id
            item['user_name'] = user_name
            item['image_urls'] = [original_url]
            yield item
            
        except json.JSONDecodeError as e:
            self.logger.error(
                f"\033[31m[{self.task_id}] ❌ JSON decode error: {e}\033[0m"
            )
        except Exception as e:
            self.logger.exception(
                f"\033[31m[{self.task_id}] ❌ Unexpected error in parse_illust_detail: {e}\033[0m"
            )
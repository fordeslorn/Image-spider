# Image-spider
Used for crawling some image site

## Usage
Create a .env file in the project root directory with environment variable below:
```markdown 
PIXIV_COOKIES="xxxxxx"   # Your pixiv account cookies
userId=xxxxxx            # The author ID which you want to crawl
```

The project uses UV to manage the venv.
```shell
uv run python main.py
```

If your connection to pixiv is poor, you need to use a proxy. <br>
**The proxy port should be set to 7890.**

import sys
import os
import uuid
import json
import asyncio
import subprocess
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- FastAPI 应用设置 ---
app = FastAPI(title="API for Pixiv Image Crawler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 任务存储 ---
tasks = {}

# --- 请求模型 ---
class CrawlRequest(BaseModel):
    user_id: str = None           
    pixiv_user_id: str = None    
    cookie: str

# --- 结果文件存储目录 ---
RESULTS_DIR = Path(__file__).parent / '.task_results'
LOGS_DIR = Path(__file__).parent / '.task_logs'
RESULTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

def get_result_file(task_id: str) -> str:
    """获取任务结果文件的路径"""
    return str(RESULTS_DIR / f"{task_id}.json")

def get_log_file(task_id: str) -> str:
    """获取任务日志文件的路径"""
    return str(LOGS_DIR / f"{task_id}.log")

async def monitor_task(task_id: str, process: subprocess.Popen):
    """
    监控子进程，定期检查结果文件。
    """
    result_file = get_result_file(task_id)
    timeout = 3600
    elapsed = 0
    check_interval = 1

    while elapsed < timeout:
        await asyncio.sleep(check_interval)
        elapsed += check_interval

        if os.path.exists(result_file):
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    result = json.load(f)
                tasks[task_id].update(result)
                return
            except json.JSONDecodeError:
                continue
        
        if process.poll() is not None:
            tasks[task_id]['status'] = 'failed'
            tasks[task_id]['logs'].append('Subprocess exited without generating result')
            return

    tasks[task_id]['status'] = 'failed'
    tasks[task_id]['logs'].append(f'Task timeout after {timeout} seconds')
    process.terminate() # 先尝试优雅终止
    try:
        process.wait(timeout=5)  # 等待 5 秒
    except subprocess.TimeoutExpired:
        process.kill()  # 强制杀死

async def run_spider_task(task_id: str, user_id: str, cookie: str, mode: str):
    """在子进程中启动爬虫"""
    result_file = get_result_file(task_id)
    python_exe = sys.executable
    worker_script = os.path.join(os.path.dirname(__file__), 'worker.py')
    
    try:
        process = subprocess.Popen(
            [python_exe, worker_script, task_id, user_id, cookie, mode, result_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
    except Exception as e:
        tasks[task_id]['status'] = 'failed'
        tasks[task_id]['logs'].append(f'Failed to start subprocess: {str(e)}')
        return

    asyncio.create_task(monitor_task(task_id, process))

# --- API Endpoints ---
@app.post("/api/v1/crawl/start/{mode}")
async def start_crawl(mode: str, request: CrawlRequest):
    """启动一个爬虫任务"""
    if mode not in ['image', 'data']:
        raise HTTPException(status_code=400, detail="模式必须是 'image' 或 'data'")
    
    # ========== 参数验证 ==========
    if not request.pixiv_user_id:
        raise HTTPException(status_code=400, detail="pixiv_user_id 是必需的")
    
    if not request.pixiv_user_id.isdigit():
        raise HTTPException(
            status_code=400, 
            detail=f"pixiv_user_id 必须是数字，收到: {request.pixiv_user_id}"
        )
    
    if not request.cookie or len(request.cookie) < 50:
        raise HTTPException(status_code=400, detail="Cookie 无效或太短")
    
    # ========== 打印调试信息 ==========
    print(f"\n{'='*60}")
    print(f"[FastAPI] 收到爬虫请求:")
    print(f"  登录用户: {request.user_id or '未提供'}")
    print(f"  目标 Pixiv 用户: {request.pixiv_user_id}")
    print(f"  模式: {mode}")
    print(f"  Cookie 长度: {len(request.cookie)}")
    print(f"{'='*60}\n")
    
    # ========== 创建任务 ==========
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "running",
        "mode": mode,
        "user_id": request.user_id,              # 登录用户（可选）
        "pixiv_user_id": request.pixiv_user_id,  # Pixiv 用户（必需）
        "logs": [],
        "results": [],
        "images": []
    }
    
    # ========== 启动爬虫 ==========
    asyncio.create_task(run_spider_task(
        task_id, 
        request.pixiv_user_id,  # ← 直接用 pixiv_user_id
        request.cookie, 
        mode
    ))
    
    return {"status": "started", "task_id": task_id}

@app.get("/api/v1/crawl/status/{task_id}")
async def get_status(task_id: str):
    """获取任务状态"""
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务未找到")
    return task

# 新增：日志查询接口
@app.get("/api/v1/crawl/logs/{task_id}")
async def get_logs(task_id: str, tail: int = 50):
    """
    获取任务的实时日志
    
    参数:
        task_id: 任务 ID
        tail: 返回最后 N 行日志（默认 50）
    """
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务未找到")
    
    log_file = get_log_file(task_id)
    
    if not os.path.exists(log_file):
        # 日志文件还没被创建
        return {"task_id": task_id, "logs": [], "total_lines": 0}
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
        
        # 返回最后 N 行
        log_lines = all_lines[-tail:] if len(all_lines) > tail else all_lines
        
        return {
            "task_id": task_id,
            "logs": [line.rstrip('\n') for line in log_lines],
            "total_lines": len(all_lines)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")
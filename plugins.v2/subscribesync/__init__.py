"""
MoviePilot V2 插件：订阅同步
===========================
功能：
1) 拉取源 MP 订阅列表，按用户分组展示
2) 拉取目标 SA telegram 订阅任务列表
3) 同步订阅：MP → SA（电视剧查询季集数 + 电影填模板）
4) mp同步sa：对比 MP 与 SA 订阅（按 tmdbid），SA 已有的取消 MP 订阅，MP 有而 SA 无的推送到 SA
5) 定时任务 + 手动触发
6) Telegram 命令控制
"""

import json
import re
import time
import base64
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.plugins import _PluginBase
from app.log import logger
from app.schemas.types import NotificationType
from app.schemas.types import EventType
from app.db.subscribe_oper import SubscribeOper
from app.core.event import eventmanager, Event


# ===== 常量 =====
STATE_MAP = {
    "N": ("新建", "info"),
    "R": ("订阅中", "primary"),
    "P": ("已暂停", "warning"),
    "S": ("已完成", "success"),
}
TYPE_MAP = {
    "电影": "电影", "电视剧": "电视剧",
    "movie": "电影", "tv": "电视剧", "series": "电视剧",
}
TV_TYPES = {"tv", "series", "电视剧", "剧集"}
MOVIE_TYPES = {"movie", "电影", "mov"}

BROWSER_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1"
)

SYNC_CONCURRENCY = 5  # 并发查询 SA 季集数


# ===== 工具函数 =====
def normalize_poster(poster: str) -> str:
    if not poster:
        return ""
    if poster.startswith("http://") or poster.startswith("https://"):
        return poster
    return "https://image.tmdb.org/t/p/w500" + poster


def mask_secret(s: str) -> str:
    if not s:
        return ""
    if len(s) <= 12:
        return s[:2] + "***"
    return s[:8] + "***" + s[-4:]


def classify_type(raw_type):
    t = (raw_type or "").strip().lower()
    if t in TV_TYPES:
        return "tv"
    if t in MOVIE_TYPES:
        return "movie"
    return None


def safe_filename(name: str) -> str:
    name = (name or "unknown").strip()
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    return name[:80] if len(name) > 80 else name


def decode_jwt_exp(token: str):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        return data.get("exp")
    except Exception:
        return None


def is_cloudflare_challenge(text: str) -> bool:
    if not text:
        return False
    return ("Just a moment" in text) or ("cf-mitigated" in text) or ("challenge-platform" in text)


# ===== 主插件类 =====
class SubscribeSync(_PluginBase):
    # 插件元数据
    plugin_name = "订阅同步"
    plugin_version = "1.5.6"
    plugin_author = "AutoBuilder"
    author_url = "https://github.com"
    plugin_description = (
        "订阅同步插件：拉取 MP 订阅与 SA telegram 订阅，按 tmdbid 对齐同步，"
        "支持定时执行与 Telegram 命令控制。"
    )
    plugin_config_prefix = "SubscribeSync_"

    # 插件级别：1 = 用户级（常驻），2 = 系统级
    plugin_level = 1

    # -------- 默认配置 --------
    @staticmethod
    def default_config() -> Dict[str, Any]:
        return {
            "enabled": False,
            # 源 MP 配置（留空则读取本地数据库）
            "mp_base_url": "",
            "mp_api_token": "",
            "mp_timeout": 15,
            # 目标 SA 配置
            "sa_base_url": "https://sa.lxb.icu",
            "sa_username": "",
            "sa_password": "",
            "sa_timeout": 20,
            "sa_cookie": "",
            # 任务调度
            "task_cron": "0 * * * *",
            "task_enabled": False,
            "task_order": ["mp", "sa", "sync", "mp_sync_sa"],
            "enabled_tasks": {"mp": True, "sa": True, "sync": True, "mp_sync_sa": True},
            # Telegram
            "tg_bot_token": "",
            "tg_chat_id": "",
            # 其他
            "sync_concurrency": 5,
            "notify_new_sub": True,
        }

    # ==================== 生命周期 ====================

    def init_plugin(self, config: dict = None):
        """初始化插件：读取配置、加载持久化数据。"""
        # 防止配置为空导致覆盖已有配置
        if not config:
            stored = self.get_config()
            config = stored if stored else self.default_config()
        self._config = config

        self._mp_base = self._config.get("mp_base_url", "").rstrip("/")
        self._mp_token = self._config.get("mp_api_token", "")
        self._mp_timeout = float(self._config.get("mp_timeout", 15))

        self._sa_base = self._config.get("sa_base_url", "").rstrip("/")
        self._sa_user = self._config.get("sa_username", "")
        self._sa_pass = self._config.get("sa_password", "")
        self._sa_timeout = float(self._config.get("sa_timeout", 20))
        self._sa_cookie = self._config.get("sa_cookie", "")

        self._task_cron = self._config.get("task_cron", "0 * * * *")
        self._task_enabled = self._config.get("task_enabled", False)
        self._task_order = self._config.get("task_order", ["mp", "sa", "sync", "mp_sync_sa"])
        self._enabled_tasks = self._config.get(
            "enabled_tasks", {"mp": True, "sa": True, "sync": True, "mp_sync_sa": True}
        )

        self._tg_token = self._config.get("tg_bot_token", "")
        self._tg_chat = self._config.get("tg_chat_id", "")
        self._notify_new = self._config.get("notify_new_sub", True)
        self._sync_concurrency = int(self._config.get("sync_concurrency", 5))

        # 运行时状态
        self._running = False
        self._sa_session = self._load_sa_session()
        self._cache = {"mp": None, "sa": None, "sync": None}
        self._enabled = self._config.get("enabled", False)

        self._load_cache()
        logger.info(f"[SubscribeSync] 插件初始化完成，启用状态: {self._enabled}")

        # 处理手动触发开关（任务完成后自动重置，调用 API 方法确保发送通知）
        triggers = {
            "run_sync_mp": ("同步 MP 订阅", lambda: self._api_sync_mp()),
            "run_sync_sa": ("同步 SA 订阅", lambda: self._api_sync_sa()),
            "run_sync": ("开始同步", lambda: self._api_sync()),
            "run_mp_sync_sa": ("mp同步sa", lambda: self._api_mp_sync_sa()),
        }
        for key, (label, func) in triggers.items():
            if self._config.get(key):
                logger.info(f"[SubscribeSync] 手动触发：{label}")
                self._start_manual_task(key, label, func)

    def _start_manual_task(self, key: str, label: str, func):
        """启动手动任务并在完成后重置开关。"""
        def _run():
            try:
                func()
            except Exception as e:
                logger.error(f"[SubscribeSync] 手动任务「{label}」异常：{e}")
            finally:
                self._reset_switch(key)

        threading.Thread(target=_run, daemon=True).start()

    @eventmanager.register(EventType.SubscribeAdded)
    def _on_subscribe_added(self, event: Event):
        """监听 MP 新增订阅事件，自动触发同步。"""
        if not self._enabled:
            return
        try:
            sub_data = event.event_data or {}
            sub_name = sub_data.get("name", "") or (sub_data.get("mediainfo", {}) if isinstance(sub_data.get("mediainfo"), dict) else {}).get("title", "未知")
            logger.info(f"[SubscribeSync] 检测到新增订阅「{sub_name}」，5 秒后自动刷新")
        except Exception:
            logger.info("[SubscribeSync] 检测到新增订阅，5 秒后自动刷新")

        # 延迟 5 秒执行，避免频繁触发
        self._debounce_sync()

    @eventmanager.register(EventType.PluginAction)
    def _on_plugin_action(self, event: Event):
        """处理 TG 菜单命令触发的 PluginAction 事件。"""
        if not self._enabled:
            return
        action = event.event_data.get("action") if event.event_data else None
        if not action:
            return

        action_map = {
            "sync_mp": ("同步 MP 订阅", self._api_sync_mp),
            "sync_sa": ("同步 SA 订阅", self._api_sync_sa),
            "sync": ("开始同步", self._api_sync),
            "mp_sync_sa": ("mp同步sa", self._api_mp_sync_sa),
            "run_sequence": ("执行任务序列", self._api_run_sequence),
        }
        if action in action_map:
            label, func = action_map[action]
            logger.info(f"[SubscribeSync] TG 命令触发：{label}")
            threading.Thread(target=lambda f=func: f(), daemon=True).start()

    # 防抖：避免短时间内多次触发
    _debounce_timer = None

    def _debounce_sync(self):
        """防抖触发完整 4 步序列：同步 MP → 同步 SA → 开始同步 → mp同步sa。"""
        if self._debounce_timer:
            self._debounce_timer.cancel()

        def _do():
            try:
                self._sync_mp()
            except Exception as e:
                logger.error(f"[SubscribeSync] 自动刷新 MP 订阅失败：{e}")

            try:
                self._sync_sa()
            except Exception as e:
                logger.error(f"[SubscribeSync] 自动刷新 SA 订阅失败：{e}")

            try:
                self._sync()
            except Exception as e:
                logger.error(f"[SubscribeSync] 自动同步失败：{e}")

            try:
                self._mp_sync_sa()
            except Exception as e:
                logger.error(f"[SubscribeSync] 自动 mp同步sa 失败：{e}")

        self._debounce_timer = threading.Timer(5, _do)
        self._debounce_timer.daemon = True
        self._debounce_timer.start()

    def _reset_switch(self, key: str):
        """仅重置单个开关字段，不覆盖其他配置。"""
        try:
            stored = self.get_config() or {}
            stored[key] = False
            self.update_config(stored)
            self._config[key] = False
            logger.info(f"[SubscribeSync] 手动触发开关 {key} 已重置")
        except Exception as e:
            logger.warning(f"[SubscribeSync] 重置开关 {key} 失败：{e}")

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        """停止所有后台任务。"""
        self._running = False
        logger.info("[SubscribeSync] 插件服务已停止")

    # ==================== 配置表单 ====================

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回配置页面 Vuetify JSON 与默认数据模型。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "task_enabled", "label": "启用定时任务"},
                                    },
                                ],
                            },
                        ],
                    },
                    # ---- 源 MP 配置 ----
                    {
                        "component": "VSubheader",
                        "props": {"class": "mt-5 mb-2"},
                        "content": [{"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "content": [{"component": "text", "props": {}, "text": "源 MoviePilot 配置"}]}],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "mp_base_url",
                                            "label": "MP 地址（可选）",
                                            "placeholder": "远程 MP 地址，留空则读取本地数据库",
                                            "hint": "仅同步其他 MP 实例时才需填写",
                                            "persistent-hint": True,
                                        },
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "mp_api_token",
                                            "label": "API Token（可选）",
                                            "placeholder": "远程 MP 的 API Token",
                                            "type": "password",
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    # ---- 目标 SA 配置 ----
                    {
                        "component": "VSubheader",
                        "props": {"class": "mt-5 mb-2"},
                        "content": [{"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "content": [{"component": "text", "props": {}, "text": "目标 SA 配置"}]}],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "sa_base_url", "label": "SA 地址", "placeholder": "https://sa.lxb.icu"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "sa_username", "label": "SA 账号"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "sa_password",
                                            "label": "SA 密码",
                                            "type": "password",
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "sa_cookie",
                                            "label": "Cloudflare Cookie（可选）",
                                            "placeholder": "cf_clearance=xxx",
                                        },
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "sa_timeout",
                                            "label": "超时（秒）",
                                            "type": "number",
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    # ---- Telegram 配置 ----
                    {
                        "component": "VSubheader",
                        "props": {"class": "mt-5 mb-2"},
                        "content": [{"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "content": [{"component": "text", "props": {}, "text": "Telegram 通知（可选）"}]}],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "tg_bot_token",
                                            "label": "Bot Token",
                                            "type": "password",
                                            "placeholder": "从 @BotFather 获取",
                                        },
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "tg_chat_id",
                                            "label": "Chat ID",
                                        },
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify_new_sub", "label": "新订阅通知"},
                                    },
                                ],
                            },
                        ],
                    },
                    # ---- 定时任务 ----
                    {
                        "component": "VSubheader",
                        "props": {"class": "mt-5 mb-2"},
                        "content": [{"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "content": [{"component": "text", "props": {}, "text": "定时任务"}]}],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "task_cron",
                                            "label": "Cron 表达式",
                                            "placeholder": "0 * * * *",
                                            "hint": "分 时 日 月 周，示例：每小时=0 * * * *，每天8点=0 8 * * *",
                                            "persistent-hint": True,
                                        },
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "task_order",
                                            "label": "执行顺序",
                                            "multiple": True,
                                            "chips": True,
                                            "items": [
                                                {"title": "同步 MP 订阅", "value": "mp"},
                                                {"title": "同步 SA 订阅", "value": "sa"},
                                                {"title": "开始同步", "value": "sync"},
                                                {"title": "mp同步sa", "value": "mp_sync_sa"},
                                            ],
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled_tasks.mp", "label": "启用 MP 同步"}},
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled_tasks.sa", "label": "启用 SA 同步"}},
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled_tasks.sync", "label": "启用 同步"}},
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled_tasks.mp_sync_sa", "label": "启用 mp同步sa"}},
                                ],
                            },
                        ],
                    },
                    # ---- 手动触发 ----
                    {
                        "component": "VSubheader",
                        "props": {"class": "mt-5 mb-2"},
                        "content": [{"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "content": [{"component": "text", "props": {}, "text": "手动触发（保存后执行一次）"}]}],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mb-3"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "run_sync_mp", "label": "同步 MP 订阅"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "run_sync_sa", "label": "同步 SA 订阅"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "run_sync", "label": "开始同步"},
                                    },
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "run_mp_sync_sa", "label": "mp同步sa"},
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ], {
            "enabled": False,
            "mp_base_url": "",
            "mp_api_token": "",
            "mp_timeout": 15,
            "sa_base_url": "https://sa.lxb.icu",
            "sa_username": "",
            "sa_password": "",
            "sa_timeout": 20,
            "sa_cookie": "",
            "task_cron": "0 * * * *",
            "task_enabled": False,
            "task_order": ["mp", "sa", "sync", "mp_sync_sa"],
            "enabled_tasks": {"mp": True, "sa": True, "sync": True, "mp_sync_sa": True},
            "tg_bot_token": "",
            "tg_chat_id": "",
            "sync_concurrency": 5,
            "notify_new_sub": True,
            "run_sync_mp": False,
            "run_sync_sa": False,
            "run_sync": False,
            "run_mp_sync_sa": False,
        }

    # ==================== 详情页 ====================

    def _get_page_cache(self, key: str) -> dict:
        """读取缓存数据。"""
        try:
            data = self.get_data(f"cache_{key}")
            return json.loads(data) if data else {}
        except Exception:
            return {}

    def get_page(self) -> List[dict]:
        """返回详情页面（展示缓存数据）。"""
        mp_cache = self._get_page_cache("mp") or {}
        sa_cache = self._get_page_cache("sa") or {}
        sync_cache = self._get_page_cache("sync") or {}

        mp_data = mp_cache.get("data", {})
        mp_updated = mp_cache.get("updated_at", "从未")
        mp_items = mp_data.get("items", [])
        mp_count = len(mp_items)

        sa_data = sa_cache.get("data", {})
        sa_updated = sa_cache.get("updated_at", "从未")
        sa_items = sa_data.get("items", [])
        sa_count = len(sa_items)

        sync_updated = sync_cache.get("updated_at", "从未")
        sync_data = sync_cache.get("data", {})
        sync_ok = sync_data.get("success", 0) if isinstance(sync_data, dict) else 0
        sync_total = sync_data.get("total", 0) if isinstance(sync_data, dict) else 0

        # 构建 MP 订阅表格数据
        mp_table_items = []
        for it in mp_items[:50]:  # 最多展示 50 条
            mp_table_items.append({
                "名称": it.get("name", "?"),
                "类型": it.get("type", "?"),
                "用户": it.get("username", "?"),
            })

        return [
            {
                "component": "VContainer",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCard",
                                        "content": [
                                            {
                                                "component": "VCardTitle",
                                                "content": [{"component": "text", "props": {}, "text": "数据概览"}],
                                            },
                                            {
                                                "component": "VCardText",
                                                "content": [
                                                    {
                                                        "component": "VRow",
                                                        "content": [
                                                            {
                                                                "component": "VCol",
                                                                "props": {"cols": 12, "md": 4},
                                                                "content": [
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-h6"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"MP 订阅缓存：{mp_updated}"}],
                                                                    },
                                                                    {"component": "VDivider", "props": {}},
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-body-1"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"共 {mp_count} 条，{len(mp_data.get('users', []))} 位用户"}],
                                                                    },
                                                                    {
                                                                        "component": "VTable",
                                                                        "props": {"items": mp_table_items, "hover": True, "density": "compact"},
                                                                        "content": [
                                                                            {
                                                                                "component": "colgroup",
                                                                                "content": [
                                                                                    {"component": "col", "props": {"style": "width:50%"}},
                                                                                    {"component": "col", "props": {"style": "width:25%"}},
                                                                                    {"component": "col", "props": {"style": "width:25%"}},
                                                                                ],
                                                                            },
                                                                        ],
                                                                    },
                                                                ],
                                                            },
                                                            {
                                                                "component": "VCol",
                                                                "props": {"cols": 12, "md": 4},
                                                                "content": [
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-h6"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"SA 订阅缓存：{sa_updated}"}],
                                                                    },
                                                                    {"component": "VDivider", "props": {}},
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-body-1"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"共 {sa_count} 条"}],
                                                                    },
                                                                ],
                                                            },
                                                            {
                                                                "component": "VCol",
                                                                "props": {"cols": 12, "md": 4},
                                                                "content": [
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-h6"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"同步缓存：{sync_updated}"}],
                                                                    },
                                                                    {"component": "VDivider", "props": {}},
                                                                    {
                                                                        "component": "span",
                                                                        "props": {"class": "text-body-1"},
                                                                        "content": [{"component": "text", "props": {}, "text": f"上次同步：成功 {sync_ok} / 共 {sync_total}"}],
                                                                    },
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ]

    # ==================== API 端点 ====================

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/sync_mp",
                "endpoint": self._api_sync_mp,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "同步 MP 订阅",
            },
            {
                "path": "/sync_sa",
                "endpoint": self._api_sync_sa,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "同步 SA 订阅",
            },
            {
                "path": "/sync",
                "endpoint": self._api_sync,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "开始同步（电视剧+电影）",
            },
            {
                "path": "/mp_sync_sa",
                "endpoint": self._api_mp_sync_sa,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "mp同步sa（对比并推送）",
            },
            {
                "path": "/run_sequence",
                "endpoint": self._api_run_sequence,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行任务序列",
            },
            {
                "path": "/data/mp",
                "endpoint": self._api_data_mp,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取 MP 缓存数据",
            },
            {
                "path": "/data/sa",
                "endpoint": self._api_data_sa,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取 SA 缓存数据",
            },
            {
                "path": "/data/sync",
                "endpoint": self._api_data_sync,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取同步缓存数据",
            },
            {
                "path": "/sa_status",
                "endpoint": self._api_sa_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "SA 登录状态",
            },
            {
                "path": "/sa_login",
                "endpoint": self._api_sa_login,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "SA 登录",
            },
            {
                "path": "/sa_logout",
                "endpoint": self._api_sa_logout,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "SA 登出",
            },
            {
                "path": "/unsubscribe",
                "endpoint": self._api_mp_unsubscribe,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "取消 MP 订阅",
            },
        ]

    # ==================== 定时服务 ====================

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时任务。"""
        if not self._enabled or not self._task_enabled:
            return []
        return [
            {
                "id": "SubscribeSync_scheduler",
                "name": "订阅同步定时任务",
                "trigger": CronTrigger.from_crontab(self._task_cron),
                "func": self._run_sequence,
                "kwargs": {},
            },
        ]

    # ==================== 远程命令 ====================

    def get_command(self) -> List[Dict[str, Any]]:
        """注册 Telegram 命令（复用 MP 已配置的 Telegram 机器人）。"""
        return [
            {"cmd": "/sync_mp", "event": EventType.PluginAction, "desc": "同步 MP 订阅", "data": {"action": "sync_mp"}},
            {"cmd": "/sync_sa", "event": EventType.PluginAction, "desc": "同步 SA 订阅", "data": {"action": "sync_sa"}},
            {"cmd": "/sync", "event": EventType.PluginAction, "desc": "开始同步", "data": {"action": "sync"}},
            {"cmd": "/mp_sync_sa", "event": EventType.PluginAction, "desc": "mp同步sa", "data": {"action": "mp_sync_sa"}},
            {"cmd": "/run_sequence", "event": EventType.PluginAction, "desc": "执行任务序列", "data": {"action": "run_sequence"}},
        ]

    # ==================== 核心业务逻辑 ====================

    # -------- 配置读写 --------
    def _reload_config(self):
        """重新读取配置（在手动触发操作前调用确保配置最新）。"""
        cfg = self.get_config() or {}
        if cfg:
            self._config = cfg
            self._mp_base = cfg.get("mp_base_url", "").rstrip("/")
            self._mp_token = cfg.get("mp_api_token", "")
            self._mp_timeout = float(cfg.get("mp_timeout", 15))
            self._sa_base = cfg.get("sa_base_url", "").rstrip("/")
            self._sa_user = cfg.get("sa_username", "")
            self._sa_pass = cfg.get("sa_password", "")
            self._sa_timeout = float(cfg.get("sa_timeout", 20))
            self._sa_cookie = cfg.get("sa_cookie", "")
            self._task_cron = cfg.get("task_cron", "0 * * * *")
            self._task_enabled = cfg.get("task_enabled", False)
            self._task_order = cfg.get("task_order", ["mp", "sa", "sync", "mp_sync_sa"])
            self._enabled_tasks = cfg.get(
                "enabled_tasks", {"mp": True, "sa": True, "sync": True, "mp_sync_sa": True}
            )
            self._tg_token = cfg.get("tg_bot_token", "")
            self._tg_chat = cfg.get("tg_chat_id", "")
            self._notify_new = cfg.get("notify_new_sub", True)
            self._sync_concurrency = int(cfg.get("sync_concurrency", 5))
            self._enabled = cfg.get("enabled", False)

    # -------- 数据持久化 --------
    def _load_sa_session(self) -> dict:
        try:
            data = self.get_data("sa_session")
            return json.loads(data) if data else {}
        except Exception:
            return {}

    def _save_sa_session(self, session: dict):
        try:
            self.save_data("sa_session", json.dumps(session, ensure_ascii=False))
        except Exception:
            pass

    def _load_cache(self):
        for k in ("mp", "sa", "sync"):
            try:
                data = self.get_data(f"cache_{k}")
                self._cache[k] = json.loads(data) if data else None
            except Exception:
                self._cache[k] = None

    def _save_cache(self):
        for k in ("mp", "sa", "sync"):
            if self._cache.get(k) is not None:
                try:
                    self.save_data(f"cache_{k}", json.dumps(self._cache[k], ensure_ascii=False))
                except Exception:
                    pass

    # -------- HTTP 请求（同步） --------
    def _http_get(self, url: str, headers: dict = None, timeout: float = 20, params: dict = None) -> httpx.Response:
        """同步 GET 请求。"""
        h = headers or {}
        h.setdefault("User-Agent", BROWSER_UA)
        with httpx.Client(timeout=timeout, verify=False, follow_redirects=True) as client:
            return client.get(url, headers=h, params=params or {})

    def _http_post(self, url: str, headers: dict = None, json_data: dict = None,
                   data: dict = None, files: dict = None, timeout: float = 20) -> httpx.Response:
        """同步 POST 请求。"""
        h = headers or {}
        h.setdefault("User-Agent", BROWSER_UA)
        kwargs = {}
        if json_data is not None:
            kwargs["json"] = json_data
        elif data is not None:
            kwargs["data"] = data
        elif files is not None:
            kwargs["files"] = files
        with httpx.Client(timeout=timeout, verify=False, follow_redirects=True) as client:
            return client.post(url, headers=h, **kwargs)

    def _http_delete(self, url: str, headers: dict = None, timeout: float = 15) -> httpx.Response:
        """同步 DELETE 请求。"""
        h = headers or {}
        h.setdefault("User-Agent", BROWSER_UA)
        with httpx.Client(timeout=timeout, verify=False) as client:
            return client.delete(url, headers=h)

    # -------- SA 登录 --------
    def _do_sa_login(self, username: str, password: str, cookie: str = "") -> dict:
        """登录 SA 获取 Token 和 Cookie。"""
        base_headers = {
            "Accept": "application/json",
            "Origin": self._sa_base,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        if cookie:
            base_headers["Cookie"] = cookie

        attempts = [
            (f"{self._sa_base}/api/v1/login", "form"),
            (f"{self._sa_base}/api/v1/login", "multipart"),
            (f"{self._sa_base}/api/v1/login/access-token", "form"),
            (f"{self._sa_base}/api/v1/login/access-token", "multipart"),
        ]
        last_err = "登录失败"

        for url, mode in attempts:
            logger.info(f"[SubscribeSync] 尝试 SA 登录：{url} ({mode})，账号：{username}")
            headers = dict(base_headers)
            kwargs = {}
            if mode == "form":
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                kwargs["data"] = {"username": username, "password": password}
            else:
                kwargs["files"] = {"username": (None, username), "password": (None, password)}

            try:
                if mode == "form":
                    r = self._http_post(url, headers=headers, data=kwargs["data"], timeout=self._sa_timeout)
                else:
                    r = self._http_post(url, headers=headers, files=kwargs["files"], timeout=self._sa_timeout)
                srv_cookie = "; ".join([f"{k}={v}" for k, v in r.cookies.items()])
            except Exception as e:
                last_err = f"请求失败：{e}"
                continue

            if r.status_code == 403 and is_cloudflare_challenge(r.text):
                logger.error("[SubscribeSync] SA 登录被 Cloudflare 拦截")
                return {"ok": False, "error": "被 Cloudflare 拦截，请在配置中填入 cf_clearance Cookie"}
            if r.status_code == 401:
                logger.error("[SubscribeSync] SA 登录 401：账号密码错误")
                return {"ok": False, "error": "账号或密码错误（401）"}
            if r.status_code != 200:
                last_err = f"登录端点 {url} 返回 {r.status_code}"
                logger.info(f"[SubscribeSync] {last_err}，尝试下一个端点")
                continue

            # 解析 Token
            try:
                data = r.json()
                token = (data.get("access_token") or data.get("token") or "").strip()
                if token.lower().startswith("bearer "):
                    token = token[7:].strip()
            except Exception:
                token = ""

            if not token and r.cookies.get("token"):
                token = r.cookies.get("token").strip()
                if token.lower().startswith("bearer "):
                    token = token[7:].strip()

            if not token:
                candidate = (r.text or "").strip().strip('"').strip("'")
                if candidate.count(".") == 2:
                    token = candidate.strip()
                    if token.lower().startswith("bearer "):
                        token = token[7:].strip()

            if not token:
                ctype = r.headers.get("content-type", "")
                snippet = (r.text or "")[:400]
                last_err = f"登录端点 {url} 未返回 token（content-type: {ctype}）"
                logger.info(f"[SubscribeSync] {last_err}，响应片段: {snippet}")
                continue

            exp = decode_jwt_exp(token)
            cookie_out = srv_cookie or cookie
            exp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exp)) if exp else "未知"
            logger.info(f"[SubscribeSync] SA 登录成功，Token: {mask_secret(token)}，过期: {exp_str}")
            return {"ok": True, "token": token, "exp": exp, "cookie": cookie_out}

        return {"ok": False, "error": last_err}

    def _ensure_sa_token(self) -> dict:
        """确保有可用的 SA Token，过期则自动重新登录。"""
        s = self._sa_session
        token = s.get("token", "")
        exp = s.get("token_exp")
        cookie = s.get("cookie", "")
        username = s.get("username", "") or self._sa_user
        password = s.get("password", "") or self._sa_pass

        now = int(time.time())
        if token and exp and (exp - now) > 300:
            return {"ok": True, "token": token, "cookie": cookie}

        if not username or not password:
            logger.warning("[SubscribeSync] SA 未配置账号密码，无法自动登录")
            return {"ok": False, "error": "未配置 SA 账号密码"}

        if not token:
            logger.info("[SubscribeSync] 本地无有效 SA Token，准备自动登录...")
        else:
            exp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exp))
            logger.info(f"[SubscribeSync] SA Token 即将过期（{exp_str}），自动重新登录...")

        res = self._do_sa_login(username, password, cookie)
        if not res["ok"]:
            return {"ok": False, "error": res["error"]}

        self._sa_session.update({
            "token": res["token"],
            "cookie": res["cookie"],
            "token_exp": res["exp"],
            "username": username,
            "password": password,
            "save_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        self._save_sa_session(self._sa_session)
        return {"ok": True, "token": res["token"], "cookie": res["cookie"]}

    # -------- 同步 MP 订阅 --------
    def _sync_mp(self) -> dict:
        """拉取 MP 的订阅列表（优先从本地数据库读取，无 Token 时直读本地）。"""
        logger.info("[SubscribeSync] 正在拉取 MP 订阅列表...")

        # 优先尝试从本地 MP 数据库直接读取
        if not self._mp_token or not self._mp_base:
            logger.info("[SubscribeSync] 未配置远程 MP Token，从本地数据库读取订阅")
            return self._sync_mp_local()

        # 回退到远程 API 方式
        url = f"{self._mp_base}/api/v1/subscribe/list?token={self._mp_token}"
        try:
            r = self._http_get(url, timeout=self._mp_timeout)
        except Exception as e:
            logger.warning(f"[SubscribeSync] 远程 API 不可达，尝试本地数据库：{e}")
            return self._sync_mp_local()

        if r.status_code == 401:
            logger.warning("[SubscribeSync] MP API Token 校验失败（401），尝试本地数据库")
            return self._sync_mp_local()
        if r.status_code != 200:
            logger.error(f"[SubscribeSync] MP 接口返回 {r.status_code}")
            return {"error": f"MP 返回 HTTP {r.status_code}", "total": 0, "users": [], "items": []}

        try:
            data = r.json()
        except Exception:
            logger.error("[SubscribeSync] MP 返回非 JSON 数据")
            return {"error": "MP 返回非 JSON", "total": 0, "users": [], "items": []}

        if isinstance(data, dict):
            data = data.get("data", data.get("items", []))
        if not isinstance(data, list):
            data = []

        items = []
        for sub in data:
            items.append(self._build_mp_item_from_dict(sub))

        logger.info(f"[SubscribeSync] ✓ MP 订阅远程拉取完成：{len(items)} 条")
        return self._cache_mp_result(items)

    def _sync_mp_local(self) -> dict:
        """直接从本地 MP 数据库读取订阅列表。"""
        try:
            subs = SubscribeOper().list()
        except Exception as e:
            logger.error(f"[SubscribeSync] 本地数据库读取失败：{e}")
            return {"error": str(e), "total": 0, "users": [], "items": []}

        items = []
        for sub in subs:
            items.append(self._build_mp_item_from_model(sub))

        logger.info(f"[SubscribeSync] ✓ 本地 MP 订阅读取完成：{len(items)} 条")
        return self._cache_mp_result(items)

    def _build_mp_item_from_model(self, sub) -> dict:
        """将 Subscribe ORM 模型转为统一字典。"""
        state = sub.state or "N"
        state_label, state_type = STATE_MAP.get(state, (state or "未知", "secondary"))
        mtype = sub.type or ""
        type_label = TYPE_MAP.get(mtype, mtype or "未知")
        total_ep = int(sub.total_episode or 0)
        lack_ep = int(sub.lack_episode or 0)
        got_ep = (total_ep - lack_ep) if total_ep else 0
        return {
            "id": sub.id,
            "name": sub.name or "未知",
            "year": sub.year or "",
            "type": type_label,
            "raw_type": mtype,
            "season": sub.season or "",
            "state": state,
            "state_label": state_label,
            "state_type": state_type,
            "username": sub.username or "默认",
            "poster": normalize_poster(sub.poster or ""),
            "backdrop": normalize_poster(sub.backdrop or ""),
            "total_episode": total_ep,
            "lack_episode": lack_ep,
            "got_episode": got_ep,
            "date": sub.date or "",
            "description": sub.description or "",
            "tmdbid": str(sub.tmdbid or ""),
            "doubanid": str(sub.doubanid or ""),
            "imdbid": sub.imdbid or "",
            "tvdbid": sub.tvdbid or "",
            "note": sub.note or "",
        }

    def _build_mp_item_from_dict(self, sub: dict) -> dict:
        """将远程 API 返回的字典转为统一字典。"""
        state = sub.get("state") or "N"
        state_label, state_type = STATE_MAP.get(state, (state or "未知", "secondary"))
        mtype = sub.get("type") or ""
        type_label = TYPE_MAP.get(mtype, mtype or "未知")
        total_ep = int(sub.get("total_episode") or 0)
        lack_ep = int(sub.get("lack_episode") or 0)
        got_ep = (total_ep - lack_ep) if total_ep else 0
        return {
            "id": sub.get("id"),
            "name": sub.get("name") or "未知",
            "year": sub.get("year") or "",
            "type": type_label,
            "raw_type": mtype,
            "season": sub.get("season") or "",
            "state": state,
            "state_label": state_label,
            "state_type": state_type,
            "username": sub.get("username") or "默认",
            "poster": normalize_poster(sub.get("poster")),
            "backdrop": normalize_poster(sub.get("backdrop")),
            "total_episode": total_ep,
            "lack_episode": lack_ep,
            "got_episode": got_ep,
            "date": sub.get("date") or "",
            "description": sub.get("description") or "",
            "tmdbid": str(sub.get("tmdbid") or ""),
            "doubanid": str(sub.get("doubanid") or ""),
            "imdbid": sub.get("imdbid") or "",
            "tvdbid": sub.get("tvdbid") or "",
            "note": sub.get("note") or "",
        }

    def _cache_mp_result(self, items: List[dict]) -> dict:
        """缓存 MP 订阅数据并检测新订阅通知。"""
        # 分组
        groups: Dict[str, list] = {}
        for it in items:
            groups.setdefault(it["username"], []).append(it)
        users = [{"username": u, "count": len(v)} for u, v in groups.items()]

        # 新订阅 TG 通知
        if self._tg_token and self._tg_chat and self._notify_new:
            old = self._cache.get("mp")
            if old:
                old_items = old.get("data", {}).get("items", [])
                old_ids = {str(it.get("tmdbid") or "") for it in old_items}
                old_ids.discard("")
                for it in items:
                    tid = str(it.get("tmdbid") or "")
                    if tid and tid not in old_ids:
                        self._new_sub_notify(it)

        self._cache["mp"] = {
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": {"items": items, "users": users, "source": "本地数据库" if (not self._mp_token or not self._mp_base) else self._mp_base},
        }
        self._save_cache()
        logger.info(f"[SubscribeSync] MP 订阅已缓存（{len(items)} 条）")
        return {"total": len(items), "source": self._mp_base or "本地数据库", "users": users, "items": items}

    # -------- 同步 SA 订阅 --------
    def _sync_sa(self) -> dict:
        """拉取 SA telegram 订阅任务列表。"""
        auth = self._ensure_sa_token()
        if not auth["ok"]:
            return {"error": auth["error"], "total": 0, "items": []}

        token = auth["token"]
        cookie = auth["cookie"]
        logger.info(f"[SubscribeSync] 正在请求 SA telegram 订阅（Token: {mask_secret(token)}）...")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        if cookie:
            headers["Cookie"] = cookie

        try:
            r = self._http_get(f"{self._sa_base}/api/v1/telegram_subscribe/tasks", headers=headers, timeout=self._sa_timeout)
        except Exception as e:
            logger.error(f"[SubscribeSync] SA 请求失败：{e}")
            return {"error": str(e), "total": 0, "items": []}

        # 401/403 自动重新登录重试
        if r.status_code in (401, 403):
            if is_cloudflare_challenge(r.text):
                logger.error("[SubscribeSync] SA 被 Cloudflare 拦截")
                return {"error": "被 Cloudflare 拦截", "total": 0, "items": []}
            logger.warning(f"[SubscribeSync] SA 返回 {r.status_code}，尝试重新登录重试...")
            res = self._do_sa_login(
                self._sa_session.get("username", "") or self._sa_user,
                self._sa_session.get("password", "") or self._sa_pass,
                self._sa_session.get("cookie", ""),
            )
            if not res["ok"]:
                return {"error": f"重登录失败：{res['error']}", "total": 0, "items": []}
            self._sa_session.update({"token": res["token"], "cookie": res["cookie"], "token_exp": res["exp"]})
            self._save_sa_session(self._sa_session)
            headers["Authorization"] = f"Bearer {res['token']}"
            if res["cookie"]:
                headers["Cookie"] = res["cookie"]
            try:
                r = self._http_get(f"{self._sa_base}/api/v1/telegram_subscribe/tasks", headers=headers, timeout=self._sa_timeout)
            except Exception as e:
                logger.error(f"[SubscribeSync] SA 重试失败：{e}")
                return {"error": str(e), "total": 0, "items": []}
            if r.status_code in (401, 403):
                logger.error(f"[SubscribeSync] SA 重试后仍被拒绝（{r.status_code}）")
                return {"error": f"重试后仍被拒绝（{r.status_code}）", "total": 0, "items": []}

        if r.status_code != 200:
            logger.error(f"[SubscribeSync] SA 返回 {r.status_code}")
            return {"error": f"SA 返回 HTTP {r.status_code}", "total": 0, "items": []}

        try:
            data = r.json()
        except Exception:
            logger.error("[SubscribeSync] SA 返回非 JSON")
            return {"error": "SA 返回非 JSON", "total": 0, "items": []}

        items = data.get("data") if isinstance(data, dict) else data
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            items = []
        items = [self._normalize_sa_item(it) for it in items if isinstance(it, dict)]

        self._cache["sa"] = {
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": {"items": items, "source": self._sa_base},
        }
        self._save_cache()
        logger.info(f"[SubscribeSync] SA 订阅已缓存（{len(items)} 条）")
        return {"total": len(items), "source": self._sa_base, "items": items}

    def _normalize_sa_item(self, sub: dict) -> dict:
        """将 SA 任务转换为与 MP 一致的字段结构。"""
        state = sub.get("state") or "N"
        state_label, state_type = STATE_MAP.get(state, (state or "未知", "secondary"))
        mtype = sub.get("type") or ""
        type_label = TYPE_MAP.get(mtype, mtype or "未知")
        total_ep = int(sub.get("total_episode") or 0)
        lack_ep = int(sub.get("lack_episode") or 0)
        got_ep = (total_ep - lack_ep) if total_ep else 0
        return {
            "id": sub.get("id"),
            "name": sub.get("name") or sub.get("title") or "未知",
            "year": sub.get("year") or "",
            "type": type_label,
            "raw_type": mtype,
            "season": sub.get("season") or "",
            "state": state,
            "state_label": state_label,
            "state_type": state_type,
            "username": sub.get("username") or "默认",
            "poster": normalize_poster(sub.get("poster") or sub.get("poster_path") or sub.get("backdrop_path") or sub.get("backdrop")),
            "backdrop": normalize_poster(sub.get("backdrop") or sub.get("backdrop_path")),
            "total_episode": total_ep,
            "lack_episode": lack_ep,
            "got_episode": got_ep,
            "date": sub.get("date") or sub.get("created") or "",
            "description": sub.get("description") or sub.get("note") or "",
            "vote": sub.get("vote") or 0,
            "tmdbid": str(sub.get("tmdbid") or sub.get("tmdb_id") or ""),
        }

    # -------- 同步：MP → SA（电视剧查季集数，电影填模板） --------
    def _build_filled_template(self, it: dict, kind: str, sa_data) -> dict:
        """按模板填充一条订阅 JSON（字段与 Docker 版 app.py 完全对齐）。"""
        return {
            "season_episode_count": sa_data if isinstance(sa_data, dict) else {},
            "message_include_words": [],
            "title": it.get("name") or it.get("title") or "",
            "backdrop_path": normalize_poster(it.get("poster")),
            "channel_type": "channel_115",
            "overview": "",
            "subscribe_url": "",
            "transfered_episodes": [],
            "parent_id": "3468594882933687738",
            "poster_path": normalize_poster(it.get("backdrop")),
            "end_words": "全集|完结|全\\d+集",
            "last_update": "",
            "year": int(it.get("year") or 0),
            "type": kind,
            "invalid_urls": [],
            "rule_id": "7783544a-b426-469c-b4db-8a2454dbe661",
            "id": "",
            "season": 1,
            "message_exclude_words": [],
            "last_urls": [],
            "start_episode": 1,
            "channels": [
                "hdhive_1", "Channel_Shares_115", "gimy115", "QukanMovie",
                "oneonefivewpfx", "vip115hot", "ysxb48", "Movie888035",
                "Lsp115", "yingshiziyuanpindao",
            ],
            "tmdbid": str(it.get("tmdbid") or it.get("tmdb_id") or ""),
            "latest_episode": 0,
            "total_episode_count": int(it.get("total_episode") or 0),
            "rating": {},
        }

    def _query_sa_season_count(self, token: str, cookie: str, name: str, tvdbid) -> Optional[dict]:
        """查询 SA 电视剧季集数。"""
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        if cookie:
            headers["Cookie"] = cookie
        try:
            r = self._http_get(
                f"{self._sa_base}/api/v1/tmdbinfo/tmdb_tv_season_episode_count",
                headers=headers,
                params={"tmdbid": tvdbid},
                timeout=self._sa_timeout,
            )
        except Exception as e:
            logger.error(f"[SubscribeSync] 「{name}」(tvdbid={tvdbid}) 查询季集数失败：{e}")
            return None
        if r.status_code != 200:
            logger.error(f"[SubscribeSync] 「{name}」(tvdbid={tvdbid}) 返回 {r.status_code}")
            return None
        try:
            sa_data = r.json().get("data")
        except Exception:
            return None
        if not sa_data:
            logger.warning(f"[SubscribeSync] 「{name}」SA 未返回季集数")
        return sa_data

    def _sync(self) -> dict:
        """同步：拉取 MP 订阅 → 查询 SA 季集数 / 电影填模板。"""
        logger.info("[SubscribeSync] 开始同步（电视剧+电影）...")

        # 1) 拉取 MP 订阅（本地数据库或远程 API）
        mp_result = self._sync_mp()
        if "error" in mp_result:
            return {"error": mp_result["error"], "success": 0, "total": 0, "items": []}
        mp_items = mp_result.get("items", [])

        # 分类
        tv_items, movie_items, other = [], [], 0
        for it in mp_items:
            kind = classify_type(it.get("type") or it.get("raw_type"))
            if kind == "tv":
                tv_items.append(it)
            elif kind == "movie":
                movie_items.append(it)
            else:
                other += 1
        logger.info(
            f"[SubscribeSync] MP 订阅共 {len(mp_items)} 条：电视剧 {len(tv_items)}，"
            f"电影 {len(movie_items)}，跳过 {other}"
        )

        # 2) 获取 SA Token（仅电视剧需要）
        token, cookie = "", ""
        if tv_items:
            auth = self._ensure_sa_token()
            if not auth["ok"]:
                return {"error": auth["error"], "success": 0, "total": 0, "items": []}
            token = auth["token"]
            cookie = auth["cookie"]

        # 3) 处理每条订阅
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_one(it):
            name = it.get("name") or it.get("title") or "未知"
            kind = classify_type(it.get("type"))
            if kind is None:
                return None

            sa_data = None
            if kind == "tv":
                tvdbid = it.get("tvdbid") or it.get("tvdb_id")
                if not tvdbid:
                    tvdbid = it.get("tmdbid") or it.get("tmdb_id")
                    if not tvdbid:
                        logger.warning(f"[SubscribeSync] 「{name}」无 tvdbid/tmdbid，跳过")
                        return None
                    logger.warning(f"[SubscribeSync] 「{name}」用 tmdbid={tvdbid} 回退")
                sa_data = self._query_sa_season_count(token, cookie, name, tvdbid)

            filled = self._build_filled_template(it, kind, sa_data)
            return {
                "name": name,
                "poster": normalize_poster(it.get("poster")),
                "backdrop": normalize_poster(it.get("backdrop")),
                "year": it.get("year") or "",
                "type": kind,
                "tmdbid": str(it.get("tmdbid") or it.get("tmdb_id") or ""),
                "json": filled,
            }

        results = []
        with ThreadPoolExecutor(max_workers=self._sync_concurrency) as pool:
            futures = {pool.submit(process_one, it): it for it in tv_items + movie_items}
            for f in as_completed(futures):
                try:
                    rv = f.result()
                    if rv:
                        results.append(rv)
                except Exception:
                    pass

        ok = len(results)
        total = len(tv_items) + len(movie_items)
        self._cache["sync"] = {
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": {"success": ok, "total": total, "items": results},
        }
        self._save_cache()
        logger.info(f"[SubscribeSync] 同步完成：{ok}/{total} 条")
        return {"success": ok, "total": total, "items": results}

    # -------- mp同步sa：对比并推送 --------
    def _mp_sync_sa(self) -> dict:
        """对比 MP 与 SA 订阅（按 tmdbid）：
        - SA 已有 → 取消 MP 订阅
        - MP 有而 SA 无 → 推送到 SA
        """
        logger.info("[SubscribeSync] 开始 mp同步sa ...")

        # 1) 拉取 MP 订阅（本地数据库或远程 API）
        mp_result = self._sync_mp()
        if "error" in mp_result:
            return {"error": mp_result["error"], "success": False}
        mp_items = mp_result.get("items", [])
        if not mp_items:
            logger.warning("[SubscribeSync] MP 无订阅，跳过")
            return {"success": True, "cancel": 0, "cancel_total": 0, "add": 0, "add_total": 0}
        logger.info(f"[SubscribeSync] MP 共有 {len(mp_items)} 条订阅，tmdbid: {[str(it.get('tmdbid') or '?') for it in mp_items]}")

        # 2) 拉取 SA 订阅
        auth = self._ensure_sa_token()
        if not auth["ok"]:
            return {"error": auth["error"], "success": False}
        token = auth["token"]
        cookie = auth["cookie"]

        sa_items = self._fetch_sa_tasks(token, cookie)
        if sa_items is None:
            return {"error": "拉取 SA 任务失败", "success": False}
        logger.info(f"[SubscribeSync] SA 共有 {len(sa_items)} 条订阅，tmdbid: {[str(it.get('tmdbid') or '') for it in sa_items]}")

        sa_tmdbids = {str(it.get("tmdbid") or "") for it in sa_items if it.get("tmdbid")}
        sa_tmdbids.discard("")

        # 3) 分类
        to_cancel, to_add, skipped = [], [], []
        for mp in mp_items:
            t = str(mp.get("tmdbid") or mp.get("tmdb_id") or "")
            if not t:
                skipped.append(mp.get("name") or "未知")
                continue
            if t in sa_tmdbids:
                to_cancel.append(mp)
            else:
                to_add.append(mp)

        logger.info(
            f"[SubscribeSync] 比对结果：MP {len(mp_items)} 条，"
            f"SA已有 {len(to_cancel)} 条（取消MP），"
            f"MP独有 {len(to_add)} 条（推送SA），跳过 {len(skipped)}"
        )

        # 4) 取消 MP 订阅
        cancel_ok, cancel_fail = [], []
        for mp in to_cancel:
            name = mp.get("name") or mp.get("title") or "未知"
            sub_id = mp.get("id")
            tmdbid = str(mp.get("tmdbid") or mp.get("tmdb_id") or "")
            logger.info(f"[SubscribeSync] 取消 MP 订阅：{name}（SA 已有）")
            sc = self._mp_delete_subscribe(sub_id, tmdbid)
            if sc in (200, 204):
                cancel_ok.append(name)
                logger.info(f"[SubscribeSync] ✓ 已取消 MP 订阅：{name}")
            else:
                cancel_fail.append(name)
                logger.error(f"[SubscribeSync] ✗ 取消失败：{name}（{sc}）")

        # 5) 推送 SA（含 401/403 自动重登录 + 详细日志）
        add_ok, add_fail, add_skip = [], [], []
        sa_before_count = len(sa_items)  # 推送前的 SA 任务数
        if to_add:
            _push_token = token
            _push_cookie = cookie

            for i, it in enumerate(to_add, 1):
                name = it.get("name") or it.get("title") or "未知"
                kind = classify_type(it.get("type"))
                if kind is None:
                    add_skip.append(name)
                    logger.warning(f"[SubscribeSync] 推送「{name}」跳过：类型 {it.get('type')} 无法识别")
                    continue

                tmdbid_val = str(it.get("tmdbid") or it.get("tmdb_id") or "")
                year_val = int(it.get("year") or 0)
                logger.info(
                    f"[SubscribeSync] ({i}/{len(to_add)}) 推送「{name}」"
                    f" kind={kind} tmdbid={tmdbid_val} year={year_val} ..."
                )

                sa_data = None
                if kind == "tv":
                    tvdbid = it.get("tvdbid") or it.get("tvdb_id")
                    if not tvdbid:
                        tvdbid = tmdbid_val
                    if tvdbid:
                        sa_data = self._query_sa_season_count(_push_token, _push_cookie, name, tvdbid)
                        logger.info(f"[SubscribeSync] 「{name}」SA 季集数查询结果：{str(sa_data)[:200]}")

                filled = self._build_filled_template(it, kind, sa_data)
                # 打印完整模板 JSON
                logger.info(f"[SubscribeSync] 推送「{name}」完整模板：{json.dumps(filled, ensure_ascii=False)}")

                save_headers = {
                    "Authorization": f"Bearer {_push_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Dest": "empty",
                }
                if _push_cookie:
                    save_headers["Cookie"] = _push_cookie

                push_url = f"{self._sa_base}/api/v1/telegram_subscribe/save_telegram_subscribe_task"
                try:
                    r = self._http_post(push_url, headers=save_headers, json_data=filled, timeout=self._sa_timeout)
                except Exception as e:
                    add_fail.append(name)
                    logger.error(f"[SubscribeSync] 推送「{name}」网络异常：{e}")
                    continue

                # 401/403 → 重新登录刷新 token，重试一次
                if r.status_code in (401, 403):
                    logger.warning(f"[SubscribeSync] 推送「{name}」返回 {r.status_code}，尝试重新登录...")
                    res = self._do_sa_login(
                        self._sa_session.get("username", "") or self._sa_user,
                        self._sa_session.get("password", "") or self._sa_pass,
                        self._sa_session.get("cookie", ""),
                    )
                    if res["ok"]:
                        _push_token = res["token"]
                        _push_cookie = res["cookie"]
                        self._sa_session.update({"token": res["token"], "cookie": res["cookie"], "token_exp": res["exp"]})
                        self._save_sa_session(self._sa_session)
                        save_headers["Authorization"] = f"Bearer {_push_token}"
                        if _push_cookie:
                            save_headers["Cookie"] = _push_cookie
                        try:
                            r = self._http_post(push_url, headers=save_headers, json_data=filled, timeout=self._sa_timeout)
                        except Exception as e:
                            add_fail.append(name)
                            logger.error(f"[SubscribeSync] 推送「{name}」重试异常：{e}")
                            continue
                    else:
                        logger.error(f"[SubscribeSync] 重登录失败：{res['error']}")

                resp_body = r.text or ""
                # 先判断 HTTP 状态
                if r.status_code not in (200, 201):
                    add_fail.append(name)
                    logger.error(f"[SubscribeSync] ✗ 推送 SA 失败：{name}（HTTP {r.status_code}）：{resp_body}")
                    continue
                # 再判断 SA 返回的 body（SA 可能 200 但 body 里 success=false）
                try:
                    resp_json = json.loads(resp_body) if resp_body else {}
                except Exception:
                    resp_json = {}
                sa_success = resp_json.get("success")
                sa_code = resp_json.get("code")
                sa_msg = resp_json.get("msg") or resp_json.get("message") or resp_json.get("detail") or ""
                resp_data = resp_json.get("data")
                if sa_success is False or (isinstance(sa_code, int) and sa_code not in (0, 200, 201)):
                    add_fail.append(name)
                    logger.error(
                        f"[SubscribeSync] ✗ 推送 SA 被 SA 拒绝：{name}，"
                        f"success={sa_success} code={sa_code} msg={sa_msg} body={resp_body[:500]}"
                    )
                    continue
                # 成功：SA 返回的新任务里应带有 id
                new_id = (resp_data or {}).get("id") if isinstance(resp_data, dict) else None
                add_ok.append(name)
                logger.info(
                    f"[SubscribeSync] ✓ 已推送 SA：{name}，SA 新任务 id={new_id or '未知'}，"
                    f"完整响应：{resp_body}"
                )

        # 推送后验证：重新拉取 SA 任务，检查数量变化
        logger.info(f"[SubscribeSync] 推送前 SA 任务数 = {sa_before_count}，推送成功 = {len(add_ok)}，重新拉取验证...")
        verify_items = self._fetch_sa_tasks(token, cookie)
        if verify_items is not None:
            logger.info(f"[SubscribeSync] 推送后 SA 任务数 = {len(verify_items)}（变化：{len(verify_items) - sa_before_count}）")
            # 列出推送后 SA 中所有 task 的 tmdbid
            verify_tmdbids = [str(it.get("tmdbid") or "") for it in verify_items if it.get("tmdbid")]
            logger.info(f"[SubscribeSync] SA 中现有 tmdbid: {verify_tmdbids}")
        else:
            logger.error("[SubscribeSync] 验证失败：无法重新拉取 SA 任务")

        # 汇总日志
        lines = ["=" * 50, "mp同步sa 汇总", "=" * 50]
        if to_cancel:
            lines.append(f"取消 MP 订阅：{len(cancel_ok)}/{len(to_cancel)}")
            if cancel_ok:
                lines.append("  成功：" + "、".join(cancel_ok))
            if cancel_fail:
                lines.append("  失败：" + "、".join(cancel_fail))
        else:
            lines.append("取消 MP 订阅：无")
        if skipped:
            lines.append(f"跳过（无tmdbid）{len(skipped)}：" + "、".join(skipped))
        add_attempted = len(to_add) - len(add_skip)
        lines.append(f"推送 SA：{len(add_ok)}/{add_attempted}")
        if add_ok:
            lines.append("  成功：" + "、".join(add_ok))
        if add_fail:
            lines.append("  失败：" + "、".join(add_fail))
        if add_skip:
            lines.append("  跳过：" + "、".join(add_skip))
        lines.append("=" * 50)
        for line in lines:
            logger.info(f"[SubscribeSync] {line}")

        return {
            "success": True,
            "cancel": len(cancel_ok), "cancel_total": len(to_cancel),
            "cancel_ok": cancel_ok, "cancel_fail": cancel_fail,
            "add": len(add_ok), "add_total": len(to_add),
            "add_ok": add_ok, "add_fail": add_fail, "add_skip": add_skip,
            "skipped": skipped,
        }

    def _fetch_sa_tasks(self, token: str, cookie: str) -> Optional[List[dict]]:
        """拉取 SA telegram 订阅任务列表（含 401/403 自动重登录）。"""
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        if cookie:
            headers["Cookie"] = cookie
        try:
            r = self._http_get(f"{self._sa_base}/api/v1/telegram_subscribe/tasks", headers=headers, timeout=self._sa_timeout)
        except Exception as e:
            logger.error(f"[SubscribeSync] 拉取 SA 任务失败：{e}")
            return None

        # 401/403 自动重新登录重试
        if r.status_code in (401, 403):
            if is_cloudflare_challenge(r.text):
                logger.error("[SubscribeSync] SA 被 Cloudflare 拦截")
                return None
            logger.warning(f"[SubscribeSync] SA 拉取任务返回 {r.status_code}，尝试重新登录重试...")
            res = self._do_sa_login(
                self._sa_session.get("username", "") or self._sa_user,
                self._sa_session.get("password", "") or self._sa_pass,
                self._sa_session.get("cookie", ""),
            )
            if not res["ok"]:
                logger.error(f"[SubscribeSync] SA 重登录失败：{res['error']}")
                return None
            self._sa_session.update({"token": res["token"], "cookie": res["cookie"], "token_exp": res["exp"]})
            self._save_sa_session(self._sa_session)
            headers["Authorization"] = f"Bearer {res['token']}"
            if res["cookie"]:
                headers["Cookie"] = res["cookie"]
            try:
                r = self._http_get(f"{self._sa_base}/api/v1/telegram_subscribe/tasks", headers=headers, timeout=self._sa_timeout)
            except Exception as e:
                logger.error(f"[SubscribeSync] SA 重试失败：{e}")
                return None
            if r.status_code in (401, 403):
                logger.error(f"[SubscribeSync] SA 重试后仍被拒绝（{r.status_code}）")
                return None

        if r.status_code != 200:
            logger.error(f"[SubscribeSync] SA 返回 {r.status_code}：{(r.text or '')[:300]}")
            return None
        try:
            data = r.json()
        except Exception:
            logger.error("[SubscribeSync] SA 返回非 JSON")
            return None
        logger.info(f"[SubscribeSync] SA 原始响应前 500 字符：{json.dumps(data, ensure_ascii=False)[:500]}")
        items = data.get("data") if isinstance(data, dict) else data
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            logger.warning(f"[SubscribeSync] SA 任务列表格式异常，items 类型={type(items)}")
            items = []
        result = [self._normalize_sa_item(it) for it in items if isinstance(it, dict)]
        logger.info(f"[SubscribeSync] SA 任务解析完成：{len(result)} 条，tmdbid: {[str(it.get('tmdbid') or '?') for it in result]}")
        return result

    def _mp_delete_subscribe(self, sub_id, tmdbid: str) -> int:
        """删除 MP 订阅，优先本地数据库。"""
        # 本地数据库删除
        if not self._mp_token or not self._mp_base:
            return self._mp_delete_local(sub_id, tmdbid)

        # 远程 API 删除
        if sub_id:
            try:
                r = self._http_delete(
                    f"{self._mp_base}/api/v1/subscribe/{sub_id}?token={self._mp_token}",
                    headers={"Accept": "application/json"},
                    timeout=self._mp_timeout,
                )
                if r.status_code in (200, 204):
                    return r.status_code
            except Exception:
                pass
        if tmdbid:
            try:
                r = self._http_delete(
                    f"{self._mp_base}/api/v1/subscribe/media/tmdb:{tmdbid}?token={self._mp_token}",
                    headers={"Accept": "application/json"},
                    timeout=self._mp_timeout,
                )
                return r.status_code
            except Exception:
                pass
        return 404

    def _mp_delete_local(self, sub_id, tmdbid: str) -> int:
        """本地数据库删除订阅。"""
        oper = SubscribeOper()
        if sub_id:
            oper.delete(sub_id)
            logger.info(f"[SubscribeSync] 本地删除订阅 id={sub_id}")
            return 200
        if tmdbid:
            subs = oper.list_by_tmdbid(int(tmdbid))
            for s in subs:
                oper.delete(s.id)
            logger.info(f"[SubscribeSync] 本地删除 tmdbid={tmdbid} 的 {len(subs)} 条订阅")
            return 200
        return 404

    # -------- 任务序列 --------
    def _run_sequence(self):
        """按顺序执行全部任务。"""
        self._reload_config()
        logger.info("[SubscribeSync] ▶ 开始执行任务序列")
        results = []
        task_map = {
            "mp": ("同步 MP 订阅", self._sync_mp),
            "sa": ("同步 SA 订阅", self._sync_sa),
            "sync": ("开始同步", self._sync),
            "mp_sync_sa": ("mp同步sa", self._mp_sync_sa),
        }

        for key in self._task_order:
            if not self._enabled_tasks.get(key, True):
                logger.info(f"[SubscribeSync] 任务「{key}」已禁用，跳过")
                results.append((key, "跳过"))
                continue
            try:
                task_map[key][1]()
                logger.info(f"[SubscribeSync] ✓ 任务「{key}」完成")
                results.append((key, "成功"))
            except Exception as e:
                logger.error(f"[SubscribeSync] ✗ 任务「{key}」失败：{e}")
                results.append((key, f"失败：{e}"))

        logger.info("[SubscribeSync] ✓ 任务序列执行完毕")
        self._seq_summary(results)

    # -------- 消息通知（MP 内置通道 + 独立 TG Bot 降级） --------
    def _notify(self, title: str, text: str, image: str = ""):
        """发送通知：优先通过 MP 配置的消息通道，失败时降级到独立 TG Bot。"""
        # 1) MP 内置消息通道（写法与 p115strgmsub 等可用插件完全一致）
        try:
            self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text=text,
            )
            logger.info(f"[SubscribeSync] MP 内置通知已发送: {title}")
            return
        except Exception as e:
            logger.error(f"[SubscribeSync] MP 内置通知异常: {e}", exc_info=True)


        # 2) 降级到独立 Telegram Bot
        if not self._tg_token or not self._tg_chat:
            logger.warning("[SubscribeSync] MP 通知失败，且未配置独立 TG Bot，通知丢失")
            return
        try:
            if image and (image.startswith("http://") or image.startswith("https://")):
                self._http_post(
                    f"https://api.telegram.org/bot{self._tg_token}/sendPhoto",
                    json_data={
                        "chat_id": self._tg_chat,
                        "photo": image,
                        "caption": text,
                        "parse_mode": "HTML",
                    },
                    timeout=15,
                )
            else:
                self._http_post(
                    f"https://api.telegram.org/bot{self._tg_token}/sendMessage",
                    json_data={
                        "chat_id": self._tg_chat,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
            logger.info(f"[SubscribeSync] 独立 TG Bot 通知已发送: {title}")
        except Exception as e:
            logger.error(f"[SubscribeSync] 独立 TG Bot 通知发送失败：{e}", exc_info=True)

    def _seq_summary(self, results):
        """发送任务序列汇总通知。"""
        task_names = {"mp": "同步 MP", "sa": "同步 SA", "sync": "开始同步", "mp_sync_sa": "mp同步sa"}
        lines = ["📋 任务序列执行完毕"]
        for key, status in results:
            label = task_names.get(key, key)
            if status == "跳过":
                lines.append(f"⏭ {label}：已禁用")
            elif status.startswith("失败"):
                lines.append(f"❌ {label}：{status}")
            else:
                lines.append(f"✅ {label}：完成")
        self._notify("订阅同步 - 任务序列", "\n".join(lines))

    def _new_sub_notify(self, it: dict):
        """新订阅通知。"""
        name = it.get("name") or it.get("title") or "未知"
        year = str(it.get("year") or "")
        mtype = it.get("type") or ""
        tmdbid = str(it.get("tmdbid") or it.get("tmdb_id") or "")
        desc = (it.get("description") or "")[:200]
        poster = it.get("poster") or ""

        text = (
            f"🆕 新订阅\n"
            f"{name} ({year})\n"
            f"类型：{mtype}  tmdbid：{tmdbid}"
        )
        if desc:
            text += f"\n简介：{desc}"
        self._notify("订阅同步 - 新订阅", text, poster)

    # -------- API 端点处理函数 --------

    def _api_sync_mp(self, **kwargs) -> dict:
        """API：同步 MP 订阅。"""
        self._reload_config()
        result = self._sync_mp()
        self._notify("订阅同步", f"✅ 同步 MP 订阅 完成\n共 {result.get('total', 0)} 条")
        return {"success": True, "data": result}

    def _api_sync_sa(self, **kwargs) -> dict:
        """API：同步 SA 订阅。"""
        self._reload_config()
        result = self._sync_sa()
        total = result.get("total", 0)
        self._notify("订阅同步", f"✅ 同步 SA 订阅 完成\n共 {total} 条，来源：{result.get('source', '')}")
        return {"success": True, "data": result}

    def _api_sync(self, **kwargs) -> dict:
        """API：开始同步。"""
        self._reload_config()
        result = self._sync()
        ok = result.get("success", 0)
        total = result.get("total", 0)
        self._notify("订阅同步", f"✅ 开始同步 完成\n成功 {ok} / 共 {total} 条")
        return {"success": True, "data": result}

    def _api_mp_sync_sa(self, **kwargs) -> dict:
        """API：mp同步sa。"""
        self._reload_config()
        result = self._mp_sync_sa()
        cancel = result.get("cancel", 0)
        cancel_total = result.get("cancel_total", 0)
        add = result.get("add", 0)
        add_total = result.get("add_total", 0)
        lines = [
            f"✅ mp同步sa 完成",
            f"取消 MP 订阅：{cancel}/{cancel_total}",
            f"推送 SA：{add}/{add_total}",
        ]
        cancel_ok = result.get("cancel_ok") or []
        cancel_fail = result.get("cancel_fail") or []
        if cancel_ok:
            lines.append("取消成功：" + "、".join(cancel_ok))
        if cancel_fail:
            lines.append("取消失败：" + "、".join(cancel_fail))
        add_ok = result.get("add_ok") or []
        add_fail = result.get("add_fail") or []
        if add_ok:
            lines.append("推送成功：" + "、".join(add_ok))
        if add_fail:
            lines.append("推送失败：" + "、".join(add_fail))
        self._notify("订阅同步", "\n".join(lines))
        return {"success": True, "data": result}

    def _api_run_sequence(self, **kwargs) -> dict:
        """API：执行任务序列（异步启动）。"""
        t = threading.Thread(target=self._run_sequence, daemon=True)
        t.start()
        return {"success": True, "message": "任务序列已在后台启动"}

    def _api_data_mp(self, **kwargs) -> dict:
        """API：获取 MP 缓存数据。"""
        if self._cache.get("mp") is None:
            self._load_cache()
        c = self._cache.get("mp")
        if c is None:
            return {"items": [], "users": [], "source": "", "updated_at": None}
        return {
            "items": c["data"]["items"],
            "users": c["data"]["users"],
            "source": c["data"]["source"],
            "updated_at": c["updated_at"],
        }

    def _api_data_sa(self, **kwargs) -> dict:
        """API：获取 SA 缓存数据。"""
        if self._cache.get("sa") is None:
            self._load_cache()
        c = self._cache.get("sa")
        if c is None:
            return {"items": [], "source": "", "updated_at": None}
        return {
            "items": c["data"]["items"],
            "source": c["data"]["source"],
            "updated_at": c["updated_at"],
        }

    def _api_data_sync(self, **kwargs) -> dict:
        """API：获取同步缓存数据。"""
        if self._cache.get("sync") is None:
            self._load_cache()
        c = self._cache.get("sync")
        if c is None:
            return {"success": 0, "total": 0, "items": [], "updated_at": None}
        return {
            "success": c["data"]["success"],
            "total": c["data"]["total"],
            "items": c["data"]["items"],
            "updated_at": c["updated_at"],
        }

    def _api_sa_status(self, **kwargs) -> dict:
        """API：SA 登录状态。"""
        s = self._sa_session
        if s.get("token"):
            exp = s.get("token_exp")
            exp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(exp)) if exp else "未知"
            return {
                "logged_in": True,
                "username": s.get("username") or self._sa_user,
                "token_masked": mask_secret(s.get("token", "")),
                "token_exp": exp_str,
                "save_time": s.get("save_time"),
            }
        return {"logged_in": False, "has_env": bool(self._sa_user and self._sa_pass)}

    def _api_sa_login(self, **kwargs) -> dict:
        """API：手动 SA 登录。"""
        username = (kwargs.get("username") or self._sa_user or "").strip()
        password = kwargs.get("password") or self._sa_pass or ""
        cookie = (kwargs.get("cookie") or self._sa_cookie or "").strip()

        if not username or not password:
            return {"success": False, "message": "账号和密码不能为空"}

        res = self._do_sa_login(username, password, cookie)
        if not res["ok"]:
            return {"success": False, "message": res["error"]}

        self._sa_session.update({
            "token": res["token"],
            "cookie": res["cookie"],
            "token_exp": res["exp"],
            "username": username,
            "password": password,
            "save_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        self._save_sa_session(self._sa_session)
        return {"success": True, "message": "登录成功", "username": username}

    def _api_sa_logout(self, **kwargs) -> dict:
        """API：SA 登出。"""
        self._sa_session = {}
        self._save_sa_session(self._sa_session)
        return {"success": True, "message": "已登出"}

    def _api_mp_unsubscribe(self, **kwargs) -> dict:
        """API：取消 MP 订阅。"""
        sub_id = kwargs.get("id")
        tmdbid = str(kwargs.get("tmdbid") or "")
        name = (kwargs.get("name") or "").strip()

        if not sub_id and not tmdbid:
            return {"success": False, "message": "缺少订阅标识"}

        # 本地或远程删除
        status = self._mp_delete_subscribe(sub_id, tmdbid)
        if status in (200, 204):
            by = f"id={sub_id}" if sub_id else f"tmdbid={tmdbid}"
            logger.info(f"[SubscribeSync] 已取消 MP 订阅：{name}（{by}）")
            return {"success": True, "message": f"已取消：{name}"}

        return {"success": False, "message": f"取消失败"}

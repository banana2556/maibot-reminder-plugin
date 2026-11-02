"""定时提醒插件：到点唤醒麦麦在聊天里说话，提醒保存在插件数据目录的 reminders.json，重启后继续生效。

两种提醒：
- 被要求的提醒（set_reminder）：标准 5 段 cron，一次性或重复，到点一定提醒。
- 顺手记下（remember_followup）：群友随口说到自己之后的安排时，麦麦自己记一个一次性提醒，到时说不说由麦麦决定。
  单独做成工具而不是 set_reminder 的参数，否则模型会把它当成「是不是正式提醒请求」二选一而放弃。
到点时通过 Maisaka 主动任务唤醒麦麦，由麦麦结合人设和上下文自己说出来；离线期间错过的提醒在下次启动后补发一次。
模型和用户看到的文字（工具说明、唤醒意图、指令回复、WebUI、配置）一律用繁体中文，保持简短。
"""

from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import asyncio
import json

from maibot_sdk import API, Command, Field, MaiBotPlugin, PluginConfigBase, Tool

# ponytail: 轮询到期提醒，最多晚 POLL_SECONDS 秒触发；需要秒级准时再改成按最近的到期时间 sleep
POLL_SECONDS = 10
TIME_FORMAT = "%Y-%m-%d %H:%M"
WEEKDAYS = "一二三四五六日"
# 分 时 日 月 星期 各段的取值范围；星期 0 和 7 都是周日
CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
# 2 月 29 日这类表达式最多四年才触发一次
SEARCH_DAYS = 366 * 5
FOLLOWUP = "順手記下"
COMMANDS = [
    {"usage": "/reminders", "description": "列出目前聊天待觸發的提醒"},
    {"usage": "/reminder_cancel <id>", "description": "取消目前聊天的一則提醒"},
]
TOOLS = [
    {"name": "set_reminder", "description": "有人請麥麥提醒時設定，可一次或重複"},
    {"name": "remember_followup", "description": "群友聊到之後的安排時麥麥順手記下，可在設定中關閉"},
    {"name": "list_reminders / cancel_reminder", "description": "麥麥查看、取消目前聊天的提醒"},
]


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否啟用插件")
    config_version: str = Field(default="1.1.0", description="設定版本")


class FollowupSectionConfig(PluginConfigBase):
    """顺手记下配置。"""

    __ui_label__ = FOLLOWUP
    __ui_icon__ = "bell"
    __ui_order__ = 1

    enabled: bool = Field(default=True, description="允許麥麥順手記下群友之後的安排；關閉後麥麥看不到這個工具")
    max_per_chat: int = Field(default=3, description="每個聊天同時待觸發的順手記下上限")


class ReminderConfig(PluginConfigBase):
    """定时提醒插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    followup: FollowupSectionConfig = Field(default_factory=FollowupSectionConfig)


def parse_cron(expr: str) -> List[Set[int]]:
    """把 5 段 cron 解析成每段允许的取值集合，支持 * 、a-b、a,b 和 /步长。"""
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron {expr!r} 必須是 5 段：分 時 日 月 星期")
    fields: List[Set[int]] = []
    for part, (low, high) in zip(parts, CRON_RANGES, strict=True):
        values: Set[int] = set()
        for item in part.split(","):
            body, _, step = item.partition("/")
            try:
                if body == "*":
                    start, end = low, high
                elif "-" in body:
                    start, end = (int(value) for value in body.split("-", 1))
                else:
                    start = int(body)
                    # 「5/10」表示从 5 开始每 10 个取一次
                    end = high if step else start
                values.update(range(start, end + 1, int(step) if step else 1))
            except ValueError:
                raise ValueError(f"無法解析 cron 段 {item!r}") from None
            if not low <= start <= end <= high:
                raise ValueError(f"cron 段 {item!r} 超出範圍 {low}-{high}")
        fields.append(values)
    if 7 in fields[4]:
        fields[4].add(0)
    return fields


def matches_day(fields: List[Set[int]], day: date) -> bool:
    """按标准 cron 语义判断日期：日和星期都被限定时满足其一即可，否则两者都要满足。"""
    _, _, days, months, weekdays = fields
    if day.month not in months:
        return False
    day_ok = day.day in days
    weekday_ok = day.isoweekday() % 7 in weekdays
    if len(days) == 31 or set(range(7)) <= weekdays:
        return day_ok and weekday_ok
    return day_ok or weekday_ok


def next_run(fields: List[Set[int]], start: datetime) -> datetime:
    """返回不早于 start 的第一个匹配时刻（整分钟）。"""
    day = start.date()
    for _ in range(SEARCH_DAYS):
        if matches_day(fields, day):
            for hour in sorted(fields[1]):
                for minute in sorted(fields[0]):
                    candidate = datetime.combine(day, time(hour, minute))
                    if candidate >= start:
                        return candidate
        day += timedelta(days=1)
    raise ValueError("這個 cron 五年內都不會觸發")


def parse_at(now: datetime, at: str) -> datetime:
    """把 HH:MM（下一个该时刻）或 YYYY-MM-DD HH:MM 换算成时刻，已经过去时抛出 ValueError。"""
    at = at.strip()
    try:
        moment = datetime.strptime(at, TIME_FORMAT)
    except ValueError:
        try:
            clock = datetime.strptime(at, "%H:%M").time()
        except ValueError:
            raise ValueError(f"無法解析時間 {at!r}，請用 HH:MM 或 YYYY-MM-DD HH:MM") from None
        moment = datetime.combine(now.date(), clock)
        if moment <= now:
            moment += timedelta(days=1)
    if moment <= now:
        raise ValueError(f"{moment.strftime(TIME_FORMAT)} 已經過去")
    return moment


def describe(moment: datetime) -> str:
    return f"{moment.strftime(TIME_FORMAT)} 星期{WEEKDAYS[moment.weekday()]}"


def make_reminder(
    stream_id: str, content: str, due: datetime, *, cron: str = "", recurring: bool = False, auto: bool = False
) -> Dict[str, Any]:
    """生成一条提醒记录；没有聊天或内容为空时抛出 ValueError。"""
    if not stream_id:
        raise ValueError("無法確定目前聊天")
    if not content.strip():
        raise ValueError("提醒內容不能為空")
    return {
        "id": uuid4().hex[:6],
        "stream_id": stream_id,
        "cron": cron.strip(),
        "recurring": recurring,
        "auto": auto,
        "content": content.strip(),
        "due": due.isoformat(timespec="seconds"),
    }


class ReminderPlugin(MaiBotPlugin):
    """定时提醒插件。"""

    config_model = ReminderConfig

    def __init__(self) -> None:
        super().__init__()
        self._reminders: List[Dict[str, Any]] = []
        self._loop_task: Optional[asyncio.Task] = None

    @property
    def _path(self) -> Path:
        return self.ctx.paths.data_dir / "reminders.json"

    async def on_load(self) -> None:
        """读回已保存的提醒、按配置开关「顺手记下」工具并开始轮询。"""
        if self._path.exists():
            self._reminders = json.loads(self._path.read_text(encoding="utf-8"))
        await self._apply_followup_switch()
        self._loop_task = asyncio.create_task(self._run())

    async def on_unload(self) -> None:
        """停止轮询，提醒已在每次变更时落盘。"""
        if self._loop_task is not None:
            self._loop_task.cancel()

    async def on_config_update(self, scope: str, config_data: dict[str, object], version: str) -> None:
        """配置热更新后重新应用「顺手记下」开关，其余配置每次使用时直接读取。"""
        del config_data, version
        if scope == "self":
            await self._apply_followup_switch()

    async def _apply_followup_switch(self) -> None:
        # 关闭时直接禁用工具，麦麦在 Planner 里就看不到它，不会白白调用一次再被拒绝
        component = self.ctx.component
        toggle = component.enable_component if self.config.followup.enabled else component.disable_component
        result = await toggle(f"{self.ctx.plugin_id}.remember_followup", "tool")
        if not result["success"]:
            self.ctx.logger.warning(f"切换 remember_followup 工具失败：{result.get('error')}")

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(POLL_SECONDS)
            await self.fire_due(datetime.now())

    async def fire_due(self, now: datetime) -> None:
        """唤醒麦麦发出所有到期提醒：重复的排到下一个匹配时刻，一次性的删除；调用异常的留到下次轮询重试。"""
        due = [reminder for reminder in self._reminders if datetime.fromisoformat(reminder["due"]) <= now]
        for reminder in due:
            scheduled = datetime.fromisoformat(reminder["due"])
            if reminder["auto"]:
                # 没人要求过，说不说、怎么说都交给麦麦
                intent = f"你之前順手記下：{reminder['content']}\n現在到時間了，覺得合適就自然地提起或關心一下，不合適也可以不說。"
            else:
                # Maisaka 会自行决定是否回复，意图里要明确要求现在就提醒
                intent = f"到了設定的提醒時間，請現在就在聊天裡發出這則提醒：{reminder['content']}"
            late_minutes = int((now - scheduled).total_seconds() // 60)
            if late_minutes >= 1:
                intent += f"\n原定 {describe(scheduled)}，已經晚了 {late_minutes} 分鐘。"
            try:
                result = await self.ctx.maisaka.proactive.trigger(
                    reminder["stream_id"], intent, reason=FOLLOWUP if reminder["auto"] else "定時提醒"
                )
            except Exception:
                self.ctx.logger.exception(f"提醒 {reminder['id']} 触发异常，稍后重试")
                continue
            if not result["success"]:
                self.ctx.logger.warning(
                    f"提醒 {reminder['id']} 无法触发，已删除：{result.get('error')}；内容：{reminder['content']}"
                )
            if reminder not in self._reminders:  # 触发期间已被取消
                continue
            if result["success"] and reminder["recurring"]:
                following = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                reminder["due"] = next_run(parse_cron(reminder["cron"]), following).isoformat(timespec="seconds")
            else:
                self._reminders.remove(reminder)
        if due:
            self._save()

    def _save(self) -> None:
        # 先写临时文件再替换，写到一半崩溃也不会损坏已有提醒
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._reminders, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def _add(self, reminder: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        self._reminders.append(reminder)
        self._save()
        return {
            "success": True,
            "id": reminder["id"],
            "next_run": describe(datetime.fromisoformat(reminder["due"])),
            "now": describe(now),
        }

    def _cancel(self, reminder_id: str, stream_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """取消并返回提醒；给了 stream_id 时只能取消该聊天的。"""
        for reminder in self._reminders:
            if reminder["id"] == reminder_id and stream_id in (None, reminder["stream_id"]):
                self._reminders.remove(reminder)
                self._save()
                return reminder
        return None

    def _rows(self, stream_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """按下次提醒时间排好的展示行；给了 stream_id 时只列该聊天的。"""
        reminders = sorted(self._reminders, key=lambda reminder: reminder["due"])
        return [
            {
                "id": reminder["id"],
                "stream_id": reminder["stream_id"],
                "kind": FOLLOWUP if reminder["auto"] else "被要求",
                "schedule": f"重複 · {reminder['cron']}" if reminder["recurring"] else "一次",
                "next_run": describe(datetime.fromisoformat(reminder["due"])),
                "content": reminder["content"],
            }
            for reminder in reminders
            if stream_id in (None, reminder["stream_id"])
        ]

    # ===== 工具 =====

    @Tool(
        "set_reminder",
        description="有人請你提醒時使用，到時你會被喚醒，在目前聊天發出提醒。cron 是伺服器本地時間的 5 段表達式"
        "（分 時 日 月 星期），recurring=false 時只在下一次符合時提醒一次。",
        parameters={
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "要提醒誰、提醒什麼"},
                "cron": {"type": "string"},
                "recurring": {"type": "boolean"},
            },
            "required": ["content", "cron", "recurring"],
            "additionalProperties": False,
        },
        visibility="visible",
    )
    async def handle_set(
        self, content: str, cron: str, recurring: bool, stream_id: str = "", **kwargs: Any
    ) -> Dict[str, Any]:
        """设置被要求的提醒；参数错误作为失败结果交给模型改正。"""
        del kwargs
        now = datetime.now()
        try:
            if type(recurring) is not bool:
                raise ValueError("recurring 必須是布林值")
            # 从当前这一分钟算起，模型按当前时刻写出的一次性 cron 也能立即触发
            due = next_run(parse_cron(cron), now.replace(second=0, microsecond=0))
            reminder = make_reminder(stream_id, content, due, cron=cron, recurring=recurring)
        except ValueError as exc:
            return {"success": False, "error": str(exc), "now": describe(now)}
        return self._add(reminder, now)

    @Tool(
        "remember_followup",
        description="群友隨口說到自己之後的安排時，即使沒明確要求也可以順手記下提醒。at 是伺服器本地時間 HH:MM 或 "
        "YYYY-MM-DD HH:MM，選你到時想問的時刻，比如事情開始或結束的時候。只記一次，不用說出來；記下的同時照常回覆或不回覆都行。",
        parameters={
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "注意到的事和相關的人"},
                "at": {"type": "string"},
            },
            "required": ["content", "at"],
            "additionalProperties": False,
        },
        visibility="visible",
    )
    async def handle_followup(self, content: str, at: str, stream_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        """顺手记下一件事；超过每个聊天的上限时拒绝。"""
        del kwargs
        now = datetime.now()
        limit = self.config.followup.max_per_chat
        try:
            if not self.config.followup.enabled:
                raise ValueError("順手記下已在設定中關閉")
            if sum(r["auto"] and r["stream_id"] == stream_id for r in self._reminders) >= limit:
                raise ValueError(f"這個聊天已經記了 {limit} 件事，先不要再記了")
            moment = parse_at(now, at)
            cron = f"{moment.minute} {moment.hour} {moment.day} {moment.month} *"
            reminder = make_reminder(stream_id, content, moment, cron=cron, auto=True)
        except ValueError as exc:
            return {"success": False, "error": str(exc), "now": describe(now)}
        return self._add(reminder, now)

    @Tool(
        "list_reminders",
        description="列出目前聊天還沒觸發的提醒",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
    )
    async def handle_list(self, stream_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        """只列出当前聊天的提醒。"""
        del kwargs
        return {"success": True, "now": describe(datetime.now()), "reminders": self._rows(stream_id)}

    @Tool(
        "cancel_reminder",
        description="依 id 取消目前聊天的一則提醒",
        parameters={
            "type": "object",
            "properties": {"reminder_id": {"type": "string"}},
            "required": ["reminder_id"],
            "additionalProperties": False,
        },
    )
    async def handle_cancel(self, reminder_id: str, stream_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        """只能取消当前聊天的提醒。"""
        del kwargs
        reminder = self._cancel(reminder_id, stream_id)
        if reminder is None:
            return {"success": False, "error": f"目前聊天沒有 id 為 {reminder_id} 的提醒"}
        return {"success": True, "cancelled": reminder["content"]}

    # ===== 聊天指令 =====

    @Command("reminders", description="列出目前聊天待觸發的提醒", pattern=r"^/reminders$")
    async def handle_list_command(self, stream_id: str = "", **kwargs: Any):
        """列出当前聊天的提醒。"""
        del kwargs
        rows = self._rows(stream_id)
        if rows:
            lines = [f"[{row['id']}] {row['next_run']} · {row['kind']} · {row['schedule']}\n{row['content']}" for row in rows]
            text = "⏰ 待觸發的提醒：\n" + "\n".join(lines) + "\n取消：/reminder_cancel <id>"
        else:
            text = "這個聊天沒有待觸發的提醒"
        await self.ctx.send.text(text, stream_id)
        return True, f"列出了 {len(rows)} 則提醒", True

    @Command(
        "reminder_cancel",
        description="取消目前聊天的一則提醒",
        pattern=r"^/reminder_cancel\s+(?P<reminder_id>\w+)$",
    )
    async def handle_cancel_command(self, stream_id: str = "", **kwargs: Any):
        """按 id 取消当前聊天的提醒。"""
        reminder_id = kwargs["matched_groups"]["reminder_id"]
        reminder = self._cancel(reminder_id, stream_id)
        text = f"已取消：{reminder['content']}" if reminder else f"這個聊天沒有 id 為 {reminder_id} 的提醒"
        await self.ctx.send.text(text, stream_id)
        return reminder is not None, text, True

    # ===== WebUI 页面 API =====

    @API("webui_summary", description="讀取所有聊天待觸發的提醒、指令和工具說明", version="1")
    async def webui_summary(self) -> Dict[str, Any]:
        """WebUI 显示真实聊天名称：群名，私聊显示「某某的私聊」。"""
        streams = await self.ctx.chat.get_all_streams(platform="all_platforms")
        names: Dict[str, str] = {}
        if isinstance(streams, list):
            for stream in streams:
                person = stream["user_cardname"] or stream["user_nickname"]
                names[stream["stream_id"]] = stream["group_name"] or f"{person}的私聊"
        else:
            self.ctx.logger.warning(f"读取聊天列表失败，提醒列表改为显示聊天 ID：{streams.get('error')}")
        rows = [{**row, "chat": names.get(row["stream_id"], row["stream_id"])} for row in self._rows()]
        return {
            "now": describe(datetime.now()),
            "total": len(rows),
            "followup_count": sum(row["kind"] == FOLLOWUP for row in rows),
            "recurring_count": sum(row["schedule"] != "一次" for row in rows),
            "reminders": rows,
            "commands": COMMANDS,
            "tools": TOOLS,
        }

    @API("webui_cancel", description="取消任意聊天的一則提醒", version="1")
    async def webui_cancel(self, reminder_id: str) -> Dict[str, Any]:
        """WebUI 管理用，不限聊天。"""
        if self._cancel(reminder_id) is None:
            raise ValueError("提醒不存在或已經觸發，請重新整理頁面")
        return {"cancelled": reminder_id}


def create_plugin() -> ReminderPlugin:
    """创建定时提醒插件实例。"""

    return ReminderPlugin()

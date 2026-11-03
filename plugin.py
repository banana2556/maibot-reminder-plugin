"""定时提醒插件：到点唤醒麦麦在聊天里说话，提醒保存在插件数据目录的 reminders.json，重启后继续生效。

两种提醒：
- 被要求的提醒（reminder_set）：标准 5 段 cron，一次性或重复，到点一定提醒。
- 顺手记下（reminder_followup）：群友随口说到自己之后的安排时，麦麦自己记一个一次性提醒，到时说不说由麦麦决定。
  单独做成工具而不是 reminder_set 的参数，否则模型会把它当成「是不是正式提醒请求」二选一而放弃。
到点时通过 Maisaka 主动任务唤醒麦麦，由麦麦结合人设和上下文自己说出来；离线期间错过的提醒在下次启动后补发一次。
提醒可以带一条目标消息（msg_id）：到点前把它以真实 ID 放回上下文，麦麦就能用 attach_at @ 对方（attach_at 按消息找发送者）；
@ 人和回复哪条消息是两回事，回复目标和是否引用仍由麦麦和 Host 的引用规则决定。
被要求的提醒尽量 @ 人，顺手记下的由麦麦自己判断要不要 @。
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
    {"name": "reminder_set", "description": "有人請麥麥提醒時設定，可一次或重複"},
    {"name": "reminder_followup", "description": "群友聊到之後的安排時麥麥順手記下，可在設定中關閉"},
    {"name": "reminder_list / reminder_cancel", "description": "麥麥查看、取消目前聊天的提醒"},
]


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否啟用插件")
    config_version: str = Field(default="1.2.0", description="設定版本")


class ReminderSectionConfig(PluginConfigBase):
    """提醒配置。"""

    __ui_label__ = "提醒"
    __ui_icon__ = "clock"
    __ui_order__ = 1

    drift_minutes: int = Field(
        default=1, ge=0, description="提醒飄移（分鐘）：麥麥思考和回覆需要時間，提前這麼多分鐘開始對話"
    )


class FollowupSectionConfig(PluginConfigBase):
    """顺手记下配置。"""

    __ui_label__ = FOLLOWUP
    __ui_icon__ = "bell"
    __ui_order__ = 2

    enabled: bool = Field(default=True, description="允許麥麥順手記下群友之後的安排；關閉後麥麥看不到這個工具")
    max_per_chat: int = Field(default=3, description="每個聊天同時待觸發的順手記下上限")


class ReminderConfig(PluginConfigBase):
    """定时提醒插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    reminder: ReminderSectionConfig = Field(default_factory=ReminderSectionConfig)
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
    stream_id: str,
    content: str,
    due: datetime,
    *,
    cron: str = "",
    recurring: bool = False,
    auto: bool = False,
    target: Optional[Dict[str, Any]] = None,
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
        "target": target,
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
        result = await toggle(f"{self.ctx.plugin_id}.reminder_followup", "tool")
        if not result["success"]:
            self.ctx.logger.warning(f"切换 reminder_followup 工具失败：{result.get('error')}")

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(POLL_SECONDS)
            await self.fire_due(datetime.now())

    async def _target(self, msg_id: str, stream_id: str) -> Optional[Dict[str, Any]]:
        """查出 msg_id 的发送者和原文，没给 msg_id 时返回 None，查不到时抛出 ValueError 让模型改正。"""
        if not msg_id:
            return None
        message = await self.ctx.message.get_by_id(msg_id, stream_id=stream_id)
        if not isinstance(message, dict) or "success" in message:  # None 为查无此消息，带 success 的是调用失败
            raise ValueError(f"找不到訊息 {msg_id}")
        user_info = message["message_info"]["user_info"]
        return {
            "msg_id": message["message_id"],
            "name": user_info["user_cardname"] or user_info["user_nickname"],
            "user_info": user_info,
            "text": message.get("processed_plain_text") or "",
        }

    async def _restore_target(self, reminder: Dict[str, Any]) -> Optional[str]:
        """把目标消息以真实 ID 放回上下文供 attach_at 解析，原消息可能已离开 Maisaka 历史或经过重启；成功时返回对方名字。"""
        target = reminder.get("target")  # 1.1 及更早保存的提醒没有 target
        if not target:
            return None
        result = await self.ctx.maisaka.context.append(
            reminder["stream_id"],
            [{"type": "text", "content": target["text"] or reminder["content"]}],
            message_id=target["msg_id"],
            replyable=True,
            user_info=target["user_info"],
            source_kind="reminder_target",
        )
        if not result["success"]:
            self.ctx.logger.warning(f"提醒 {reminder['id']} 的目标消息放回上下文失败，这次不 @：{result.get('error')}")
            return None
        return target["name"]

    async def fire_due(self, now: datetime) -> None:
        """唤醒麦麦发出所有到期提醒：重复的排到下一个匹配时刻，一次性的删除；调用异常的留到下次轮询重试。

        提前 drift_minutes 分钟唤醒，抵消麦麦思考和回复的耗时。
        """
        drift = timedelta(minutes=self.config.reminder.drift_minutes)
        due = [reminder for reminder in self._reminders if datetime.fromisoformat(reminder["due"]) - drift <= now]
        for reminder in due:
            scheduled = datetime.fromisoformat(reminder["due"])
            try:
                name = await self._restore_target(reminder)
                if reminder["auto"]:
                    # 没人要求过，说不说、要不要 @ 都交给麦麦
                    intent = f"你之前順手記下：{reminder['content']}\n現在到時間了，覺得合適就自然地提起或關心一下，不合適也可以不說。"
                    if name:
                        intent += f"\n需要的話可以用 attach_at @ {name}（上面是對方之前的訊息）。"
                else:
                    # Maisaka 会自行决定是否回复，意图里要明确要求现在就提醒
                    intent = f"到了設定的提醒時間，請現在就在聊天裡發出這則提醒：{reminder['content']}"
                    if name:
                        intent += f"\n發送時用 attach_at @ {name}（上面是對方之前的訊息）。"
                late_minutes = int((now - scheduled).total_seconds() // 60)
                if late_minutes >= 1:
                    intent += f"\n原定 {describe(scheduled)}，已經晚了 {late_minutes} 分鐘。"
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
                # 提前唤醒时 now 早于原定时刻，下一次要从原定时刻之后算起，否则同一时刻会再触发一次
                following = max(scheduled, now.replace(second=0, microsecond=0)) + timedelta(minutes=1)
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
                "target": (reminder.get("target") or {}).get("name") or "（無）",
            }
            for reminder in reminders
            if stream_id in (None, reminder["stream_id"])
        ]

    # ===== 工具 =====

    @Tool(
        "reminder_set",
        description="有人請你提醒時使用，到時你會被喚醒，在目前聊天發出提醒。cron 是伺服器本地時間的 5 段表達式"
        "（分 時 日 月 星期），recurring=false 時只在下一次符合時提醒一次。",
        parameters={
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "要提醒誰、提醒什麼"},
                "cron": {"type": "string"},
                "recurring": {"type": "boolean"},
                "msg_id": {"type": "string", "description": "要 @ 的人的一則訊息，通常是請你提醒的那則"},
            },
            "required": ["content", "cron", "recurring", "msg_id"],
            "additionalProperties": False,
        },
        visibility="visible",
    )
    async def handle_set(
        self, content: str, cron: str, recurring: bool, msg_id: str = "", stream_id: str = "", **kwargs: Any
    ) -> Dict[str, Any]:
        """设置被要求的提醒；参数错误作为失败结果交给模型改正。"""
        del kwargs
        now = datetime.now()
        try:
            if type(recurring) is not bool:
                raise ValueError("recurring 必須是布林值")
            # 从当前这一分钟算起，模型按当前时刻写出的一次性 cron 也能立即触发
            due = next_run(parse_cron(cron), now.replace(second=0, microsecond=0))
            target = await self._target(msg_id, stream_id)
            reminder = make_reminder(stream_id, content, due, cron=cron, recurring=recurring, target=target)
        except ValueError as exc:
            return {"success": False, "error": str(exc), "now": describe(now)}
        return self._add(reminder, now)

    @Tool(
        "reminder_followup",
        description="順手記下未來的具體安排，不必等群友說『提醒我』。文字、圖片或截圖出現清楚的未來日期時間，"
        "且對話表明有人要參與或關心此事（例如引用朋友的活動截圖說『快開始了』）時，優先呼叫 reminder_followup，"
        "不要只回覆而漏記；同一件事已記下就不要重複。圖片上的聊天時間戳、倒數計數或不確定的時間不是安排，勿記。"
        "at 是伺服器本地時間 HH:MM 或 YYYY-MM-DD HH:MM；事情開始或結束時再關心。記下時照常回覆或不回覆，不必宣布提醒。",
        parameters={
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "注意到的事和相關的人"},
                "at": {"type": "string"},
                "msg_id": {"type": "string", "description": "相關的人的一則訊息，可不填"},
            },
            "required": ["content", "at"],
            "additionalProperties": False,
        },
        visibility="visible",
    )
    async def handle_followup(
        self, content: str, at: str, msg_id: str = "", stream_id: str = "", **kwargs: Any
    ) -> Dict[str, Any]:
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
            target = await self._target(msg_id, stream_id)
            reminder = make_reminder(stream_id, content, moment, cron=cron, auto=True, target=target)
        except ValueError as exc:
            return {"success": False, "error": str(exc), "now": describe(now)}
        return self._add(reminder, now)

    @Tool(
        "reminder_list",
        description="查看、列出目前聊天還沒觸發的提醒和定時排程",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
    )
    async def handle_list(self, stream_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        """只列出当前聊天的提醒。"""
        del kwargs
        return {"success": True, "now": describe(datetime.now()), "reminders": self._rows(stream_id)}

    @Tool(
        "reminder_cancel",
        description="依 id 取消、刪除目前聊天的一則提醒或定時排程",
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

    # 组件按全名注册不分类型，指令不能和 reminder_cancel 工具同名
    @Command(
        "reminder_cancel_command",
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

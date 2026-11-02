"""定时提醒插件：按标准 5 段 cron 在当前聊天提醒，一次性或重复均可（如每天、工作日、每月 1 号）。

除了被要求时设置，麦麦也可以在聊天里注意到群友之后的安排时自己记一个一次性提醒（auto），到时像朋友一样自然地提起。
是否记由麦麦在平常的思考里自己判断，插件不额外注入提示或按关键词触发。
到点时通过 Maisaka 主动任务唤醒麦麦，由麦麦结合人设和上下文自己说出提醒，而不是发送固定文本。
提醒保存在插件数据目录的 reminders.json，重启后继续生效；离线期间错过的提醒在下次启动后补发一次并说明晚了多久。
"""

from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import asyncio
import json

from maibot_sdk import MaiBotPlugin, Tool

# ponytail: 轮询到期提醒，最多晚 POLL_SECONDS 秒触发；需要秒级准时再改成按最近的到期时间 sleep
POLL_SECONDS = 10
TIME_FORMAT = "%Y-%m-%d %H:%M"
WEEKDAYS = "一二三四五六日"
# 分 时 日 月 星期 各段的取值范围；星期 0 和 7 都是周日
CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
# 2 月 29 日这类表达式最多四年才触发一次
SEARCH_DAYS = 366 * 5
# 每个聊天同时待触发的自主提醒上限，避免麦麦记太多、到点轮番追问
MAX_AUTO_PER_CHAT = 3


def parse_cron(expr: str) -> List[Set[int]]:
    """把 5 段 cron 解析成每段允许的取值集合，支持 * 、a-b、a,b 和 /步长。"""
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron {expr!r} 必须是 5 段：分 时 日 月 星期")
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
                raise ValueError(f"无法解析 cron 段 {item!r}") from None
            if not low <= start <= end <= high:
                raise ValueError(f"cron 段 {item!r} 超出范围 {low}-{high}")
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
    raise ValueError("这个 cron 五年内都不会触发")


def describe(moment: datetime) -> str:
    return f"{moment.strftime(TIME_FORMAT)} 星期{WEEKDAYS[moment.weekday()]}"


def new_reminder(
    now: datetime, stream_id: str, content: str, cron: str, recurring: bool, auto: bool = False
) -> Dict[str, Any]:
    """校验模型给的参数并生成一条提醒记录，参数不合法时抛出 ValueError。"""
    if not stream_id:
        raise ValueError("无法确定当前聊天，不能设置提醒")
    if not content.strip():
        raise ValueError("提醒内容不能为空")
    if type(recurring) is not bool or type(auto) is not bool:
        raise ValueError("recurring 和 auto 必须是布尔值")
    if auto and recurring:
        raise ValueError("自己记的提醒只能是一次性的，recurring 请设为 false")
    # 从当前这一分钟算起，模型按当前时刻写出的一次性 cron 也能立即触发
    due = next_run(parse_cron(cron), now.replace(second=0, microsecond=0))
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

    def __init__(self) -> None:
        super().__init__()
        self._reminders: List[Dict[str, Any]] = []
        self._loop_task: Optional[asyncio.Task] = None

    @property
    def _path(self) -> Path:
        return self.ctx.paths.data_dir / "reminders.json"

    async def on_load(self) -> None:
        """读回已保存的提醒并开始轮询。"""
        if self._path.exists():
            self._reminders = json.loads(self._path.read_text(encoding="utf-8"))
        self._loop_task = asyncio.create_task(self._run())

    async def on_unload(self) -> None:
        """停止轮询，提醒已在每次变更时落盘。"""
        if self._loop_task is not None:
            self._loop_task.cancel()

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
                intent = (
                    f"你之前在聊天里注意到：{reminder['content']}\n现在到了这个时间。如果觉得合适，就像朋友一样"
                    "自然地提起这个话题或关心一下，需要时可以 @ 相关的人；话题已经过去或不合适的话，也可以不说。"
                )
            else:
                # Maisaka 会自行决定是否回复，意图里要明确要求现在就提醒
                intent = f"到了设定的提醒时间，请现在就在聊天里发出这条提醒：{reminder['content']}"
            late_minutes = int((now - scheduled).total_seconds() // 60)
            if late_minutes >= 1:
                intent += f"\n这条提醒原定 {describe(scheduled)}，已经晚了 {late_minutes} 分钟，提醒时说明一下。"
            try:
                result = await self.ctx.maisaka.proactive.trigger(
                    reminder["stream_id"], intent, reason="自己记的提醒" if reminder["auto"] else "定时提醒"
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

    # ===== 工具 =====

    @Tool(
        "set_reminder",
        description="设置定时提醒：到点时你会被唤醒，在当前聊天里说话。cron 是服务器本地时间的标准 5 段表达式"
        "「分 时 日 月 星期」（星期 0 或 7 为周日）。一次性提醒把分、时、日、月都写死并设 recurring=false，"
        "如今天 14:30 写 \"30 14 <日> <月> *\"，几分钟后的提醒按当前时间算出具体时刻。重复提醒设 recurring=true，"
        "如每天 9 点 \"0 9 * * *\"、工作日 9 点 \"0 9 * * 1-5\"、每月 1 号 10 点 \"0 10 1 * *\"。"
        "没人要求时也可以用：群友聊到之后要做的事（如今晚要面试、周末要搬家），你作为朋友真的在意、到时想问问的话，"
        "可以自己记一个一次性提醒（auto=true），到时自然地提起或关心一下；大部分闲聊不需要记，记了也不必说出来。"
        "返回的 next_run 是实际的下次提醒时间，被要求设置时请核对无误再告诉对方。"
        "查看或取消提醒用 list_reminders / cancel_reminder。",
        parameters={
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "被要求时写要提醒谁、提醒什么，如「提醒小明三点开会」；"
                    "自己记的写注意到的事和相关的人，如「小明说今晚 8 点要去面试」",
                },
                "cron": {"type": "string", "description": "5 段 cron：分 时 日 月 星期"},
                "recurring": {"type": "boolean", "description": "true 为按 cron 重复提醒，false 为只提醒一次"},
                "auto": {"type": "boolean", "description": "没人要求、你自己记的提醒为 true，只能一次性"},
            },
            "required": ["content", "cron", "recurring"],
            "additionalProperties": False,
        },
        visibility="visible",
    )
    async def handle_set(
        self, content: str, cron: str, recurring: bool, auto: bool = False, stream_id: str = "", **kwargs: Any
    ) -> Dict[str, Any]:
        """设置提醒；参数错误作为失败结果交给模型改正。"""
        del kwargs
        now = datetime.now()
        try:
            pending_auto = sum(r["auto"] and r["stream_id"] == stream_id for r in self._reminders)
            if auto and pending_auto >= MAX_AUTO_PER_CHAT:
                raise ValueError(f"这个聊天里已经自己记了 {pending_auto} 件事，先不要再记了")
            reminder = new_reminder(now, stream_id, content, cron, recurring, auto)
        except ValueError as exc:
            return {"success": False, "error": str(exc), "now": describe(now)}
        self._reminders.append(reminder)
        self._save()
        return {
            "success": True,
            "id": reminder["id"],
            "next_run": describe(datetime.fromisoformat(reminder["due"])),
            "now": describe(now),
        }

    @Tool(
        "list_reminders",
        description="列出当前聊天里还没触发的定时提醒",
        parameters={"type": "object", "properties": {}, "additionalProperties": False},
    )
    async def handle_list(self, stream_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        """只列出当前聊天的提醒。"""
        del kwargs
        reminders = [
            {
                "id": reminder["id"],
                "content": reminder["content"],
                "cron": reminder["cron"],
                "recurring": reminder["recurring"],
                "auto": reminder["auto"],
                "next_run": describe(datetime.fromisoformat(reminder["due"])),
            }
            for reminder in self._reminders
            if reminder["stream_id"] == stream_id
        ]
        return {"success": True, "now": describe(datetime.now()), "reminders": reminders}

    @Tool(
        "cancel_reminder",
        description="按 id 取消当前聊天里的一条定时提醒，id 可用 list_reminders 查询",
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
        for reminder in self._reminders:
            if reminder["id"] == reminder_id and reminder["stream_id"] == stream_id:
                self._reminders.remove(reminder)
                self._save()
                return {"success": True, "cancelled": reminder["content"]}
        return {"success": False, "error": f"当前聊天没有 id 为 {reminder_id} 的提醒"}


def create_plugin() -> ReminderPlugin:
    """创建定时提醒插件实例。"""

    return ReminderPlugin()

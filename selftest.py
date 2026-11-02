"""自检：cron 解析与下次时刻、设置/列出/取消、到期唤醒、重复顺延、迟到说明、自己记的提醒和落盘恢复。在 MaiBot 根目录运行：

uv run python plugins/reminder/selftest.py
"""

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import asyncio
import logging
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin import MAX_AUTO_PER_CHAT, ReminderPlugin, describe, next_run, parse_cron  # noqa: E402

NOW = datetime(2026, 11, 2, 12, 0)  # 星期一


def check_cron() -> None:
    cases = {
        "0 12 2 11 *": datetime(2026, 11, 2, 12, 0),  # 从当前这一分钟算起
        "30 14 2 11 *": datetime(2026, 11, 2, 14, 30),
        "0 9 2 11 *": datetime(2027, 11, 2, 9, 0),  # 今年已过，只能是明年
        "0 9 * * 1-5": datetime(2026, 11, 3, 9, 0),
        "0 9 * * 6": datetime(2026, 11, 7, 9, 0),
        "0 9 * * 0": datetime(2026, 11, 8, 9, 0),
        "0 9 * * 7": datetime(2026, 11, 8, 9, 0),
        "0 10 1 * *": datetime(2026, 12, 1, 10, 0),
        "0 9 13 * 5": datetime(2026, 11, 6, 9, 0),  # 日和星期都限定时满足其一：周五先到
        "0 0 29 2 *": datetime(2028, 2, 29, 0, 0),
        "*/15 9-10 * * *": datetime(2026, 11, 3, 9, 0),
        "5/20 12 * * *": datetime(2026, 11, 2, 12, 5),
        "0,45 12 * * *": datetime(2026, 11, 2, 12, 0),
    }
    for expr, expected in cases.items():
        assert next_run(parse_cron(expr), NOW) == expected, (expr, next_run(parse_cron(expr), NOW))
    assert next_run(parse_cron("*/15 9-10 * * *"), datetime(2026, 11, 2, 9, 1)) == datetime(2026, 11, 2, 9, 15)
    for expr in ("0 9 * *", "60 * * * *", "a * * * *", "*/0 * * * *", "0 9 5-3 * *", "0 9 30 2 *"):
        try:
            next_run(parse_cron(expr), NOW)
        except ValueError:
            continue
        raise AssertionError(f"应当拒绝 {expr!r}")


def make_plugin(data_dir: Path, outcomes: list) -> tuple[ReminderPlugin, list]:
    """outcomes 依次作为每次唤醒的结果：True/False 为 success，异常实例则抛出。"""
    triggered: list = []

    async def trigger(stream_id: str, intent: str, **kwargs: object) -> dict:
        outcome = outcomes.pop(0) if outcomes else True
        if isinstance(outcome, Exception):
            raise outcome
        triggered.append((stream_id, intent))
        return {"success": outcome}

    plugin = ReminderPlugin()
    plugin._ctx = SimpleNamespace(
        maisaka=SimpleNamespace(proactive=SimpleNamespace(trigger=trigger)),
        paths=SimpleNamespace(data_dir=data_dir),
        logger=logging.getLogger("selftest"),
    )
    return plugin, triggered


async def check_lifecycle(data_dir: Path) -> None:
    outcomes: list = []
    plugin, triggered = make_plugin(data_dir, outcomes)
    await plugin.on_load()

    for args in (("开会", "0 9 *", False, "g1"), ("开会", "0 9 1 1 *", False, ""), (" ", "0 9 1 1 *", False, "g1")):
        assert not (await plugin.handle_set(*args[:3], stream_id=args[3]))["success"], args
    once = await plugin.handle_set("提醒小明开会", "0 9 1 1 *", False, stream_id="g1")
    daily = await plugin.handle_set("提醒大家喝水", "0 9 * * *", True, stream_id="g1")
    other = await plugin.handle_set("别的群", "0 9 1 1 *", False, stream_id="g2")
    assert once["success"] and daily["success"] and other["success"]
    new_year = datetime.fromisoformat(plugin._reminders[0]["due"])
    assert (new_year.month, new_year.day, new_year.hour) == (1, 1, 9) and once["next_run"] == describe(new_year)
    assert [r["id"] for r in (await plugin.handle_list(stream_id="g1"))["reminders"]] == [once["id"], daily["id"]]
    assert not (await plugin.handle_cancel(other["id"], stream_id="g1"))["success"], "不能取消别的聊天的提醒"

    # 元旦 9 点：一次性的准点触发，每天的已经晚了好几天要说明；唤醒异常的留着下次重试
    outcomes.extend([RuntimeError("rpc 断开"), True, True])
    await plugin.fire_due(new_year)
    assert [stream for stream, _ in triggered] == ["g1", "g2"] and len(plugin._reminders) == 2
    assert "提醒大家喝水" in triggered[0][1] and "晚了" in triggered[0][1]
    assert "别的群" in triggered[1][1] and "晚了" not in triggered[1][1]
    assert plugin._reminders[1]["due"] == (new_year + timedelta(days=1)).isoformat()

    await plugin.fire_due(new_year)
    assert "提醒小明开会" in triggered[-1][1] and [r["id"] for r in plugin._reminders] == [daily["id"]]

    # 自己记的提醒：只能一次性，每个聊天有上限且不占被要求的提醒；到点时由麦麦决定说不说
    assert not (await plugin.handle_set("小明今晚面试", "0 20 * * *", True, auto=True, stream_id="g3"))["success"]
    for index in range(MAX_AUTO_PER_CHAT):
        assert (await plugin.handle_set(f"小明今晚面试{index}", "0 20 1 1 *", False, auto=True, stream_id="g3"))["success"]
    assert not (await plugin.handle_set("再记一个", "0 20 1 1 *", False, auto=True, stream_id="g3"))["success"]
    assert (await plugin.handle_set("再记一个", "0 20 1 1 *", False, auto=True, stream_id="g4"))["success"]
    assert (await plugin.handle_set("提醒大家交报告", "0 20 1 1 *", False, stream_id="g3"))["success"]
    await plugin.fire_due(new_year.replace(hour=20))
    intents = [intent for stream, intent in triggered if stream == "g3"]
    assert sum("也可以不说" in intent and "小明今晚面试" in intent for intent in intents) == MAX_AUTO_PER_CHAT
    assert [intent for intent in intents if "请现在就" in intent] == [
        "到了设定的提醒时间，请现在就在聊天里发出这条提醒：提醒大家交报告"
    ]
    assert [r["id"] for r in plugin._reminders] == [daily["id"]]
    await plugin.on_unload()

    # 重启后从文件读回；无法唤醒（如聊天流不存在）的提醒即使是重复的也删除
    restored, _ = make_plugin(data_dir, [False])
    await restored.on_load()
    assert restored._reminders == plugin._reminders
    await restored.fire_due(new_year + timedelta(days=1))
    assert restored._reminders == []
    await restored.on_unload()


if __name__ == "__main__":
    check_cron()
    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(check_lifecycle(Path(tmp)))
    print("reminder selftest OK")

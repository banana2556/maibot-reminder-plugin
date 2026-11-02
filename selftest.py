"""自检：cron 与时刻解析、被要求的提醒、顺手记下及其开关和上限、到期唤醒、重复顺延、迟到说明、
聊天指令、WebUI 接口和落盘恢复。在 MaiBot 根目录运行：

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
from plugin import ReminderPlugin, describe, next_run, parse_at, parse_cron  # noqa: E402

NOW = datetime(2026, 11, 2, 12, 0)  # 星期一
FOLLOWUP_TOOL = "luca.reminder.remember_followup"


def check_parsing() -> None:
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

    assert parse_at(NOW, "17:00") == datetime(2026, 11, 2, 17, 0)
    assert parse_at(NOW, "09:00") == datetime(2026, 11, 3, 9, 0), "今天已过的 HH:MM 取明天"
    assert parse_at(NOW, "2026-11-05 20:00") == datetime(2026, 11, 5, 20, 0)
    for at in ("2026-11-01 20:00", "晚上", ""):
        try:
            parse_at(NOW, at)
        except ValueError:
            continue
        raise AssertionError(f"应当拒绝 {at!r}")


def make_plugin(data_dir: Path, outcomes: list, config: dict | None = None) -> tuple[ReminderPlugin, SimpleNamespace]:
    """outcomes 依次作为每次唤醒的结果：True/False 为 success，异常实例则抛出。返回插件和调用记录。"""
    calls = SimpleNamespace(triggered=[], sent=[], toggles=[])

    async def trigger(stream_id: str, intent: str, **kwargs: object) -> dict:
        outcome = outcomes.pop(0) if outcomes else True
        if isinstance(outcome, Exception):
            raise outcome
        calls.triggered.append((stream_id, intent))
        return {"success": outcome}

    async def send_text(text: str, stream_id: str) -> bool:
        calls.sent.append((stream_id, text))
        return True

    def toggle(enabled: bool):
        async def run(name: str, component_type: str) -> dict:
            calls.toggles.append((name, component_type, enabled))
            return {"success": True}

        return run

    async def get_all_streams(platform: str) -> list:
        assert platform == "all_platforms"
        return [
            {"stream_id": "g1", "group_name": "摸鱼群", "user_cardname": None, "user_nickname": "x"},
            {"stream_id": "p1", "group_name": None, "user_cardname": None, "user_nickname": "小明"},
        ]

    plugin = ReminderPlugin()
    plugin.set_plugin_config(config or {})
    plugin._ctx = SimpleNamespace(
        plugin_id="luca.reminder",
        maisaka=SimpleNamespace(proactive=SimpleNamespace(trigger=trigger)),
        send=SimpleNamespace(text=send_text),
        component=SimpleNamespace(enable_component=toggle(True), disable_component=toggle(False)),
        chat=SimpleNamespace(get_all_streams=get_all_streams),
        paths=SimpleNamespace(data_dir=data_dir),
        logger=logging.getLogger("selftest"),
    )
    return plugin, calls


async def check_requested(data_dir: Path) -> None:
    outcomes: list = []
    plugin, calls = make_plugin(data_dir, outcomes)
    await plugin.on_load()
    assert calls.toggles == [(FOLLOWUP_TOOL, "tool", True)]

    for args in (("开会", "0 9 *", False, "g1"), ("开会", "0 9 1 1 *", False, ""), (" ", "0 9 1 1 *", False, "g1")):
        assert not (await plugin.handle_set(*args[:3], stream_id=args[3]))["success"], args
    assert not (await plugin.handle_set("开会", "0 9 1 1 *", "false", stream_id="g1"))["success"]
    once = await plugin.handle_set("提醒小明开会", "0 9 1 1 *", False, stream_id="g1")
    daily = await plugin.handle_set("提醒大家喝水", "0 9 * * *", True, stream_id="g1")
    other = await plugin.handle_set("别的群", "0 9 1 1 *", False, stream_id="g2")
    assert once["success"] and daily["success"] and other["success"]
    new_year = datetime.fromisoformat(plugin._reminders[0]["due"])
    assert (new_year.month, new_year.day, new_year.hour) == (1, 1, 9) and once["next_run"] == describe(new_year)
    assert {r["id"] for r in (await plugin.handle_list(stream_id="g1"))["reminders"]} == {once["id"], daily["id"]}
    assert not (await plugin.handle_cancel(other["id"], stream_id="g1"))["success"], "不能取消别的聊天的提醒"

    # 元旦 9 点：一次性的准点触发，每天的已经晚了好几天要说明；唤醒异常的留着下次重试
    outcomes.extend([RuntimeError("rpc 断开"), True, True])
    await plugin.fire_due(new_year)
    assert [stream for stream, _ in calls.triggered] == ["g1", "g2"] and len(plugin._reminders) == 2
    assert "請現在就" in calls.triggered[0][1] and "提醒大家喝水" in calls.triggered[0][1]
    assert "晚了" in calls.triggered[0][1] and "晚了" not in calls.triggered[1][1]
    assert plugin._reminders[1]["due"] == (new_year + timedelta(days=1)).isoformat()
    await plugin.fire_due(new_year)
    assert "提醒小明开会" in calls.triggered[-1][1] and [r["id"] for r in plugin._reminders] == [daily["id"]]
    await plugin.on_unload()

    # 重启后从文件读回；无法唤醒（如聊天流不存在）的提醒即使是重复的也删除
    restored, _ = make_plugin(data_dir, [False])
    await restored.on_load()
    assert restored._reminders == plugin._reminders
    await restored.fire_due(new_year + timedelta(days=1))
    assert restored._reminders == []
    await restored.on_unload()


async def check_followup(data_dir: Path) -> None:
    plugin, calls = make_plugin(data_dir, [], {"plugin": {"config_version": "1.1.0"}, "followup": {"max_per_chat": 2}})
    await plugin.on_load()
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    for index in range(2):
        assert (await plugin.handle_followup(f"小明说要去面试{index}", f"{tomorrow} 20:00", stream_id="g1"))["success"]
    assert not (await plugin.handle_followup("再记一个", f"{tomorrow} 20:00", stream_id="g1"))["success"], "超过上限"
    assert (await plugin.handle_followup("别的群不受影响", f"{tomorrow} 20:00", stream_id="g2"))["success"]
    assert (await plugin.handle_set("被要求的不占上限", f"0 20 {tomorrow[8:]} {int(tomorrow[5:7])} *", False,
                                    stream_id="g1"))["success"]
    assert not (await plugin.handle_followup("时间不对", "晚上", stream_id="g1"))["success"]

    await plugin.fire_due(datetime.strptime(f"{tomorrow} 20:00", "%Y-%m-%d %H:%M"))
    intents = [intent for stream, intent in calls.triggered if stream == "g1"]
    assert sum("你之前順手記下：小明说要去面试" in intent and "也可以不說" in intent for intent in intents) == 2
    assert sum("請現在就" in intent for intent in intents) == 1 and plugin._reminders == []

    # 配置热更新关闭顺手记下：禁用工具，处理器也拒绝
    plugin.set_plugin_config({"plugin": {"config_version": "1.1.0"}, "followup": {"enabled": False}})
    await plugin.on_config_update("self", {}, "1.1.0")
    assert calls.toggles[-1] == (FOLLOWUP_TOOL, "tool", False)
    assert not (await plugin.handle_followup("关了", f"{tomorrow} 20:00", stream_id="g1"))["success"]
    await plugin.on_unload()


async def check_commands_and_webui(data_dir: Path) -> None:
    plugin, calls = make_plugin(data_dir, [])
    await plugin.on_load()
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    first = await plugin.handle_set("提醒大家开会", "0 9 * * 1-5", True, stream_id="g1")
    second = await plugin.handle_followup("小明说今晚搬家", f"{tomorrow} 21:00", stream_id="p1")
    third = await plugin.handle_followup("未知聊天", f"{tomorrow} 22:00", stream_id="x9")

    assert await plugin.handle_list_command(stream_id="g1") == (True, "列出了 1 則提醒", True)
    assert calls.sent[-1][0] == "g1" and first["id"] in calls.sent[-1][1] and second["id"] not in calls.sent[-1][1]
    await plugin.handle_list_command(stream_id="g2")
    assert calls.sent[-1] == ("g2", "這個聊天沒有待觸發的提醒")
    cancel = {"matched_groups": {"reminder_id": second["id"]}}
    assert (await plugin.handle_cancel_command(stream_id="g1", **cancel))[0] is False, "不能取消别的聊天的提醒"
    assert (await plugin.handle_cancel_command(stream_id="p1", **cancel))[0] is True

    summary = await plugin.webui_summary()
    assert (summary["total"], summary["followup_count"], summary["recurring_count"]) == (2, 1, 1)
    assert {row["chat"] for row in summary["reminders"]} == {"摸鱼群", "x9"}
    assert summary["commands"] and summary["tools"]
    await plugin.webui_cancel(third["id"])
    try:
        await plugin.webui_cancel(third["id"])
    except ValueError:
        pass
    else:
        raise AssertionError("重复取消应当报错")
    assert [r["id"] for r in plugin._reminders] == [first["id"]]
    await plugin.on_unload()


async def main() -> None:
    for check in (check_requested, check_followup, check_commands_and_webui):
        with tempfile.TemporaryDirectory() as tmp:
            await check(Path(tmp))


if __name__ == "__main__":
    check_parsing()
    asyncio.run(main())
    print("reminder selftest OK")

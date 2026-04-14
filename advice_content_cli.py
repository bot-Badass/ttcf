"""CLI for initialising and managing the advice content plan.

Commands:
    python advice_content_cli.py init
    python advice_content_cli.py status
    python advice_content_cli.py reset-topic <topic_id>
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent
_PLAN_MD = _PROJECT_ROOT / "tiktok_content_plan.md"
_PLAN_JSON = _PROJECT_ROOT / "data" / "content_plan.json"
_PROMPT_DIR = _PROJECT_ROOT / "data" / "prompts"
_PROMPT_PATH = _PROMPT_DIR / "script_prompt.md"

_SCRIPT_PROMPT_CONTENT = """\
---
Ти — досвідчений TikTok-сценарист і юрист-консультант.
Напиши скрипт для короткого відео (45–55 секунд, 160–200 слів)
для серійного україномовного TikTok-каналу з юридичної освіти.

Це **Частина {{PART_CURRENT}} з {{PART_TOTAL}}** серії "{{SERIES_TITLE}}"

**Тема:**
{{TOPIC_BLOCK}}

**Структура скрипту:**

1. ГАЧОК (0–3 сек, 1–2 речення):
   - Перше слово — дія або питання, НЕ "Привіт" або "Сьогодні"
   - Хук має зупинити скролінг за 1 секунду

2. СИТУАЦІЯ (3–30 сек, 2–3 речення):
   - Конкретний персонаж з іменем, віком, містом
   - Реальна проблема яка може статись з глядачем
   - Відчуття близькості: "це може статися з вами"

3. ПРАВА / РІШЕННЯ (30–48 сек, 3–4 речення):
   - Що людина МАЄ ПРАВО зробити
   - Обов'язкове посилання на конкретну статтю або закон України
   - Практичний крок — що зробити прямо зараз

4. ВИСНОВОК + CTA (48–55 сек, 1–2 речення):
   - Що запам'ятати
   - Заклик: "Збережи це відео 📌" або "Перешли тому, кому потрібно"

Відповідай ТІЛЬКИ текстом скрипту, без заголовків, без пояснень, без хештегів.
---
"""

# Series configuration: (topic_num_start, topic_num_end, series_id, series_title)
_SERIES_CONFIG = [
    (1, 8, "A", "Мобілізація: знай своє право"),
    (9, 14, "B", "Виживання в ЄС"),
    (15, 16, "E", "Українці в США та Канаді"),
    (17, 20, "D", "Мій дім, мої права"),
    (21, 24, "C", "Сімейне право під час війни"),
    (25, 27, "F", "Соцвиплати та пенсії"),
    (28, 30, "G", "Колабораціонізм та зрада"),
    (31, 33, "H", "Документи та ідентичність"),
    (34, 36, "I", "Нові теми 2026"),
]


def _find_series(topic_num: int) -> tuple[str, str, int, int]:
    """Return (series_id, series_title, part_number, total_parts)."""
    for start, end, sid, title in _SERIES_CONFIG:
        if start <= topic_num <= end:
            return sid, title, topic_num - start + 1, end - start + 1
    raise ValueError(f"No series configured for topic number {topic_num}")


def _parse_topics(text: str) -> list[dict]:
    """Parse tiktok_content_plan.md into a list of raw topic dicts."""
    topics: list[dict] = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        m = re.match(r"\*\*Тема\s+(\d+):\s+(.+?)\*\*", lines[i])
        if m:
            num = int(m.group(1))
            title = m.group(2).strip()
            pain = audience = scenario = facts_str = ""
            j = i + 1
            while j < len(lines):
                stripped = lines[j].strip()
                if re.match(r"\*\*Тема\s+\d+", stripped):
                    break
                if stripped.startswith("- Біль:"):
                    pain = stripped[len("- Біль:"):].strip()
                elif stripped.startswith("- Аудиторія:"):
                    audience = stripped[len("- Аудиторія:"):].strip()
                elif stripped.startswith("- Сценарій:"):
                    scenario = stripped[len("- Сценарій:"):].strip()
                elif stripped.startswith("- Юрфакти:"):
                    facts_str = stripped[len("- Юрфакти:"):].strip()
                j += 1
            topics.append({
                "num": num,
                "title": title,
                "pain": pain,
                "audience": audience,
                "scenario": scenario,
                "facts_str": facts_str,
            })
            i = j
        else:
            i += 1
    return topics


def _build_plan(raw_topics: list[dict], existing_plan: dict | None) -> dict:
    """Build content_plan JSON from parsed topics, preserving existing statuses."""
    existing_by_id: dict[str, dict] = {}
    if existing_plan:
        for series in existing_plan.get("series", []):
            for t in series.get("topics", []):
                existing_by_id[t["topic_id"]] = t

    series_map: dict[str, dict] = {}
    for _, (start, end, sid, stitle) in enumerate(_SERIES_CONFIG):
        series_map[sid] = {
            "series_id": sid,
            "title": stitle,
            "total_parts": end - start + 1,
            "topics": [],
        }

    for raw in raw_topics:
        num = raw["num"]
        try:
            sid, stitle, part_num, total = _find_series(num)
        except ValueError:
            continue

        topic_id = f"{sid}{part_num}"
        legal_facts = [f.strip() for f in raw["facts_str"].split("; ") if f.strip()]

        existing = existing_by_id.get(topic_id, {})
        topic_entry = {
            "topic_id": topic_id,
            "part_number": part_num,
            "title": raw["title"],
            "hook_formula": raw["pain"] or raw["title"],
            "audience": raw["audience"],
            "scenario": raw["scenario"],
            "legal_facts": legal_facts,
            "status": existing.get("status", "pending"),
            "script_id": existing.get("script_id"),
            "script_saved_at": existing.get("script_saved_at"),
        }
        series_map[sid]["topics"].append(topic_entry)

    # Sort topics within each series by part_number
    for series_data in series_map.values():
        series_data["topics"].sort(key=lambda t: t["part_number"])

    # Order series in the sequence defined by _SERIES_CONFIG
    ordered_series = [series_map[sid] for _, _, sid, _ in _SERIES_CONFIG if sid in series_map]
    current_index = existing_plan.get("current_topic_index", 0) if existing_plan else 0

    return {
        "series": ordered_series,
        "current_topic_index": current_index,
    }


def cmd_init() -> None:
    if not _PLAN_MD.is_file():
        print(f"Error: {_PLAN_MD} not found.", file=sys.stderr)
        sys.exit(1)

    text = _PLAN_MD.read_text(encoding="utf-8")
    raw_topics = _parse_topics(text)

    existing_plan: dict | None = None
    if _PLAN_JSON.is_file():
        try:
            existing_plan = json.loads(_PLAN_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass

    plan = _build_plan(raw_topics, existing_plan)
    _PLAN_JSON.parent.mkdir(parents=True, exist_ok=True)
    _PLAN_JSON.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    _PROMPT_DIR.mkdir(parents=True, exist_ok=True)
    _PROMPT_PATH.write_text(_SCRIPT_PROMPT_CONTENT, encoding="utf-8")

    total_topics = sum(len(s["topics"]) for s in plan["series"])
    num_series = len(plan["series"])
    print(f"Initialized: {total_topics} topics across {num_series} series")
    print(f"  Plan:   {_PLAN_JSON}")
    print(f"  Prompt: {_PROMPT_PATH}")


def cmd_status() -> None:
    if not _PLAN_JSON.is_file():
        print("No content plan found. Run: python advice_content_cli.py init", file=sys.stderr)
        sys.exit(1)

    plan = json.loads(_PLAN_JSON.read_text(encoding="utf-8"))
    header = f"{'series':6} | {'title':45} | {'total':5} | {'pending':7} | {'rendered':8}"
    print(header)
    print("-" * len(header))
    for series in plan["series"]:
        topics = series["topics"]
        total = len(topics)
        pending = sum(1 for t in topics if t["status"] == "pending")
        rendered = sum(1 for t in topics if t["status"] == "rendered")
        print(
            f"{series['series_id']:6} | {series['title'][:45]:45} | "
            f"{total:5} | {pending:7} | {rendered:8}"
        )


def cmd_reset_topic(topic_id: str) -> None:
    if not _PLAN_JSON.is_file():
        print("No content plan found. Run: python advice_content_cli.py init", file=sys.stderr)
        sys.exit(1)

    plan = json.loads(_PLAN_JSON.read_text(encoding="utf-8"))
    found = False
    for series in plan["series"]:
        for topic in series["topics"]:
            if topic["topic_id"] == topic_id:
                topic["status"] = "pending"
                topic["script_id"] = None
                topic["script_saved_at"] = None
                found = True
                break
        if found:
            break

    if not found:
        print(f"Topic not found: {topic_id}", file=sys.stderr)
        sys.exit(1)

    _PLAN_JSON.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Reset topic {topic_id} to pending.")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__.strip())
        sys.exit(1)

    command = args[0]
    if command == "init":
        cmd_init()
    elif command == "status":
        cmd_status()
    elif command == "reset-topic":
        if len(args) < 2:
            print("Usage: python advice_content_cli.py reset-topic <topic_id>", file=sys.stderr)
            sys.exit(1)
        cmd_reset_topic(args[1])
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

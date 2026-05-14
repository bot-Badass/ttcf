#!/usr/bin/env python3
"""
CTA Overlay Preview — dontpaniclaw
Генерує 4 варіанти CTA-завершення відео через ffmpeg.
Запуск: python3 cta_preview.py
Результати: /tmp/cta_test/
"""

import subprocess, os

OUT = "/tmp/cta_test"
os.makedirs(OUT, exist_ok=True)

W, H, DUR, FPS = 1080, 1920, 7, 30
FONT = "/System/Library/Fonts/Supplemental/Arial Unicode.ttf"
FONT_BOLD = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
T0 = 4  # секунда коли з'являється CTA (остані 3 сек)

# escape comma for ffmpeg filter expressions
EN = f"gte(t\\,{T0})"
EN_FADE = f"gte(t\\,{T0})"

RED   = "0xFF2D2D"
RED2  = "0xFF5555"
WHITE = "white"
GRAY  = "0xAAAAAA"
BLACK = "black"

# Фон — імітація відео (темний фон + плейсхолдер)
BG_SOURCE = f"color=c=0x282828:s={W}x{H}:r={FPS}:d={DUR}"

def dt(text, x, y, size, color=WHITE, bold=False, alpha=1.0, en=None):
    font = FONT_BOLD if bold else FONT
    col = color if alpha == 1.0 else f"{color}@{alpha}"
    enable = en if en else EN
    return (
        f"drawtext=text='{text}':fontfile={font}:fontcolor={col}"
        f":fontsize={size}:x={x}:y={y}:enable={enable}"
    )

def db(x, y, w, h, color, en=None):
    enable = en if en else EN
    return f"drawbox=x={x}:y={y}:w={w}:h={h}:color={color}:t=fill:enable={enable}"


# --- Варіант 0: без CTA (референс) ---
V0_BG = dt("[ відео контент ]", "(w-text_w)/2", "(h-text_h)/2", 50, WHITE, alpha=0.15, en="1")
VARIANTS = {}

VARIANTS["0_reference_no_cta"] = [V0_BG]


# --- Варіант 1: Нижня плашка ---
VARIANTS["1_bottom_bar"] = [
    V0_BG,
    db(0, 1580, W, 340, f"{BLACK}@0.92"),          # темна плашка
    db(0, 1580, W, 6, RED),                          # червона лінія зверху
    dt("Підписуйся", 70, 1622, 68, WHITE, bold=True),
    dt("Ч.2/4 — виходить завтра", 70, 1715, 48, GRAY),
    dt("@dontpaniclaw", 70, 1795, 44, RED2),
]


# --- Варіант 2: Pill-бейдж по центру ---
PILL_X, PILL_Y, PILL_W, PILL_H = 140, 1700, 800, 110
VARIANTS["2_pill_badge"] = [
    V0_BG,
    db(PILL_X, PILL_Y, PILL_W, PILL_H, f"{RED}@0.95"),
    dt("Ч.2/4  —  завтра на каналі", "(w-text_w)/2", PILL_Y + 30, 46, WHITE, bold=True),
]


# --- Варіант 3: Затемнення + великий текст ---
VARIANTS["3_dark_overlay"] = [
    V0_BG,
    db(0, 0, W, H, f"{BLACK}@0.62"),                # затемнення
    dt("Підписуйся", "(w-text_w)/2", 700, 82, WHITE, bold=True),
    db("(w-600)/2", 806, 600, 5, f"{RED}@0.8"),      # лінія-роздільник
    dt("Ч.2/4", "(w-text_w)/2", 830, 96, RED, bold=True),
    dt("Як перевірити себе", "(w-text_w)/2", 960, 58, WHITE),
    dt("через реєстр ТЦК", "(w-text_w)/2", 1032, 58, WHITE),
    dt("@dontpaniclaw", "(w-text_w)/2", 1130, 50, RED2),
]


# --- Варіант 4: Бренд-карточка ---
VARIANTS["4_brand_card"] = [
    # Повністю замінює відео на брендований кадр
    db(0, 0, W, H, BLACK),                             # чорний фон
    db(0, 0, 14, H, RED),                              # червона смуга зліва
    dt("DONT PANIC LAW", 90, 660, 70, RED, bold=True),
    db(90, 762, 900, 4, f"{RED}@0.55"),                # розділювач
    dt("Ч.2  /  4", 90, 795, 50, f"{WHITE}@0.5"),
    dt("Як перевірити себе", 90, 870, 64, WHITE, bold=True),
    dt("в реєстрі ТЦК", 90, 950, 64, WHITE, bold=True),
    db(90, 1075, 520, 96, RED),                        # кнопка
    dt("Підписатись", 148, 1096, 54, WHITE, bold=True),
    dt("нові серії щотижня", 90, 1200, 44, GRAY),
]


def render(name, layers):
    vf = ",".join(layers)
    out = f"{OUT}/{name}.mp4"
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", BG_SOURCE,
        "-vf", vf,
        "-t", str(DUR),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p",
        out,
    ]
    print(f"  {name} ...", end=" ", flush=True)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode == 0:
        print("OK")
    else:
        print(f"FAIL\n{r.stderr[-800:]}")
    return r.returncode == 0


if __name__ == "__main__":
    print(f"Output → {OUT}\n")
    ok = sum(render(n, l) for n, l in VARIANTS.items())
    print(f"\n{ok}/{len(VARIANTS)} відео згенеровано.")
    if ok:
        print(f"\nВідкрити в Finder:")
        print(f"  open {OUT}")

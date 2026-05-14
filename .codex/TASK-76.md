# TASK-76 — Фікс: share CTA не з'являється + коригування Y-offset

## Задача

Після TASK-74 share CTA ("Поділись" @ 10s) не з'являється у відео взагалі. Subscribe CTA з'являється (але надто високо — фіксується через env var). Потрібно діагностувати і виправити share CTA, і підкоригувати дефолтний Y-offset для subscribe.

---

## Що реалізувати

### 1. Діагностика share CTA

Перед будь-якими змінами — надрукувати у `_build_myth_ffmpeg_command` (або `render_myth_story_video`) повну ffmpeg команду, яка будується для combining-кроку:

```python
LOGGER.info("myth ffmpeg command: %s", " ".join(cmd))
```

Перевірити в логах:
- чи `share_cta_path` не `None` для платформи `tiktok`
- чи input `[2]` з `-itsoffset 10.000 -i .../share_ty.mp4` є у команді
- чи filter_complex містить `[2:v]...colorkey...[share_ol]` і `[body_hooked][share_ol]overlay...enable='between(t,10`

Найімовірніші баги:
- `share_cta_path` = `None` через помилку в логіці платформи → share CTA взагалі не додається в команду
- Індекси inputs переплутані: `[2:v]` і `[3:v]` вказують не на ті файли
- `enable=` умова написана через timestamp input'у CTA (0..5s) замість output timeline (10..15s)

### 2. Якщо `-itsoffset` не працює для share CTA — замінити на `setpts`

Якщо виявлено, що `-itsoffset` поводиться непередбачувано (subscribe працює бо стоїть в кінці, share не працює бо стоїть в середині) — замінити підхід у filter_complex:

**Замість** (поточний підхід):
```
ffmpeg ... -itsoffset 10.000 -i share_ty.mp4 ...
filter_complex: [2:v]scale=1080:-1,colorkey=...[share_ol]
```

**Використати** (надійніший підхід — без itsoffset, зсув через setpts):
```
ffmpeg ... -i share_ty.mp4 ...   ← без itsoffset
filter_complex: [2:v]setpts=PTS+10/TB,scale=1080:-1,colorkey=0x00ff00:0.35:0.1[share_ol]
```

`setpts=PTS+{share_cta_start}/TB` зсуває timestamp кліпу всередині filter_complex — не залежить від поведінки `-itsoffset` при буферизації.

`enable='between(t,{t0},{t0+5})'` залишається без змін.

Аналогічно перевірити subscribe_cta — якщо він теж через `-itsoffset`, замінити на `setpts=PTS+{subscribe_start}/TB`.

### 3. `src/config.py` — скоригувати дефолти Y-offset

Обидва CTA мають з'являтись у верхній зоні відео (~200px від верху), але не за status bar.

Змінити дефолти:

```python
CTA_SHARE_Y_OFFSET: Final[int] = int(os.getenv("CTA_SHARE_Y_OFFSET", "-785"))
CTA_SUBSCRIBE_Y_OFFSET: Final[int] = int(os.getenv("CTA_SUBSCRIBE_Y_OFFSET", "-785"))
```

(Обидві кнопки з'являлись на ~50px від верху замість ~200px. Зсув на +150px: -935 → -785 для subscribe, -1032 → -785 для share — стартове значення, підбирається зором після рендеру.)

---

## Обмеження

- Не чіпати advice pipeline (`render_story_video`, `_build_ffmpeg_render_command`)
- Не змінювати `CTA_SUBSCRIBE_INSTAGRAM_Y_OFFSET` — позиція інсти ще не перевірена зором

---

## Тести

1. `make test` — всі тести зелені
2. Запустити рендер TikTok-відео:
   ```bash
   CTA_SUBSCRIBE_Y_OFFSET=-785 python myth_render.py \
     --script data/myth/bron-oblik/script.txt \
     --audio data/myth/bron-oblik/voiceover.wav \
     --channel law \
     --output /tmp/bron-oblik.mp4
   ```
3. Відкрити `/tmp/bron-oblik_tiktok.mp4` і перевірити зором:
   - @ ~10s: кнопка "Поділись" з'являється у верхній зоні (не за status bar)
   - @ кінець: кнопка "Слідкувати" з'являється на ~200px від верху
4. Якщо share CTA тепер видно але позиція не влаштовує — повідомити Y-координату для подальшого налаштування через env var

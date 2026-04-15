from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Callable

from src import config


LOGGER = logging.getLogger(__name__)


class RenderError(Exception):
    """Raised when ffmpeg-based rendering fails."""


CommandRunnerBoundary = Callable[[list[str], int], subprocess.CompletedProcess[str]]
DurationProbeBoundary = Callable[[Path], float]


def render_story_video(
    background_video_path: Path,
    audio_path: Path,
    output_path: Path,
    subtitle_path: Path | None = None,
    hook_text: str | None = None,
    part_number: int | None = None,
    total_parts: int | None = None,
    series_id: str | None = None,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
    *,
    ffmpeg_path: str = config.FFMPEG_PATH,
    run_command: CommandRunnerBoundary | None = None,
    probe_media_duration: DurationProbeBoundary | None = None,
) -> Path:
    _validate_render_inputs(
        background_video_path=background_video_path,
        audio_path=audio_path,
        subtitle_path=subtitle_path,
    )
    LOGGER.info(
        "Render started: background=%s audio=%s subtitle=%s output=%s",
        background_video_path,
        audio_path,
        subtitle_path,
        output_path,
    )
    runner = run_command or _run_command
    prober = probe_media_duration or _probe_media_duration
    duration_seconds = prober(audio_path)

    # If hook frame is enabled and hook_text provided, build hook + concat.
    use_hook = config.HOOK_FRAME_ENABLED and hook_text
    if use_hook:
        hook_path = output_path.parent / "hook_frame.mp4"
        body_path = output_path.parent / "body.mp4"
        try:
            _render_hook_frame(
                hook_text=hook_text,
                output_path=hook_path,
                ffmpeg_path=ffmpeg_path,
                run_command=runner,
                series_id=series_id,
                part_number=part_number,
                background_video_path=background_video_path,
                hook_bg_override=hook_bg_override,
                hook_accent_override=hook_accent_override,
                hook_brand_override=hook_brand_override,
            )
            _render_body(
                background_video_path=background_video_path,
                audio_path=audio_path,
                output_path=body_path,
                subtitle_path=subtitle_path,
                duration_seconds=duration_seconds,
                ffmpeg_path=ffmpeg_path,
                run_command=runner,
                part_number=part_number,
                total_parts=total_parts,
            )
            _concat_hook_and_body(
                hook_path=hook_path,
                body_path=body_path,
                output_path=output_path,
                ffmpeg_path=ffmpeg_path,
                run_command=runner,
            )
        except RenderError as exc:
            LOGGER.error(
                "Render failed: background=%s audio=%s output=%s reason=%s",
                background_video_path, audio_path, output_path, exc,
            )
            raise RenderError(f"FFmpeg render failed: {exc}") from exc
        finally:
            hook_path.unlink(missing_ok=True)
            body_path.unlink(missing_ok=True)
    else:
        command = _build_ffmpeg_render_command(
            background_video_path=background_video_path,
            audio_path=audio_path,
            output_path=output_path,
            subtitle_path=subtitle_path,
            duration_seconds=duration_seconds,
            ffmpeg_path=ffmpeg_path,
            part_number=part_number,
            total_parts=total_parts,
        )
        try:
            runner(command, config.FFMPEG_TIMEOUT_SECONDS)
        except RenderError as exc:
            LOGGER.error(
                "Render failed: background=%s audio=%s subtitle=%s output=%s reason=%s",
                background_video_path, audio_path, subtitle_path, output_path, exc,
            )
            raise RenderError(f"FFmpeg subtitle burn failed: {exc}") from exc

    LOGGER.info("Render succeeded: output=%s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Series colour themes — each series_id maps to a unique dark bg + accent.
# All backgrounds are dark so text is always readable.
# ---------------------------------------------------------------------------
_SERIES_THEMES: dict[str, dict[str, str]] = {
    "A": {"bg": "0x111827", "accent": "0xFBBF24"},  # темно-синій   + жовтий
    "B": {"bg": "0x1a0f2e", "accent": "0xA78BFA"},  # темно-фіолет  + ліловий
    "C": {"bg": "0x0f1f1a", "accent": "0x34D399"},  # темно-зелений + м'ята
    "D": {"bg": "0x1f1209", "accent": "0xFB923C"},  # темно-коричн  + помаранч
    "E": {"bg": "0x0f172a", "accent": "0x38BDF8"},  # глибокий синій + блакить
    "F": {"bg": "0x1a1220", "accent": "0xF472B6"},  # темно-пурпур  + рожевий
    "G": {"bg": "0x162012", "accent": "0x86EFAC"},  # темний хакі   + салатовий
    "H": {"bg": "0x1e1a10", "accent": "0xFDE68A"},  # темно-охра    + пастельний жовтий
    "I": {"bg": "0x0e1e20", "accent": "0x5EEAD4"},  # темний морськ + бірюза
}
_SERIES_THEME_DEFAULT: dict[str, str] = {"bg": "0x111827", "accent": "0xFBBF24"}

# ---------------------------------------------------------------------------
# Part layout variants — each part_number gets a slightly different bar layout.
# Values: "left", "top", "left_corner", "both"
#   left        — вертикальна ліва смуга (класичний V2)
#   top         — горизонтальна смуга зверху + тонка ліва
#   left_corner — ліва смуга + короткий горизонтальний акцент знизу ліворуч
#   both        — ліва + права вертикальні смуги (рамка)
# ---------------------------------------------------------------------------
_PART_LAYOUTS: dict[int, str] = {
    1: "left",
    2: "top",
    3: "left_corner",
    4: "both",
    5: "left",   # якщо N=5 — повторює 1-й варіант
}


def _render_hook_frame(
    hook_text: str,
    output_path: Path,
    ffmpeg_path: str,
    run_command: CommandRunnerBoundary,
    series_id: str | None = None,
    part_number: int | None = None,
    background_video_path: Path | None = None,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
) -> None:
    """Route to V2 or classic hook frame based on HOOK_FRAME_LAYOUT config."""
    if config.HOOK_FRAME_LAYOUT.strip().lower() == "v2":
        _render_hook_frame_v2(
            hook_text=hook_text,
            output_path=output_path,
            ffmpeg_path=ffmpeg_path,
            run_command=run_command,
            series_id=series_id,
            part_number=part_number,
            background_video_path=background_video_path,
            hook_bg_override=hook_bg_override,
            hook_accent_override=hook_accent_override,
            hook_brand_override=hook_brand_override,
        )
    else:
        _render_hook_frame_classic(
            hook_text=hook_text,
            output_path=output_path,
            ffmpeg_path=ffmpeg_path,
            run_command=run_command,
        )


def _render_hook_frame_v2(
    hook_text: str,
    output_path: Path,
    ffmpeg_path: str,
    run_command: CommandRunnerBoundary,
    series_id: str | None = None,
    part_number: int | None = None,
    background_video_path: Path | None = None,
    hook_bg_override: str | None = None,
    hook_accent_override: str | None = None,
    hook_brand_override: str | None = None,
) -> None:
    """V2 hook frame: moving video background (darkened) + overlay text/bars.

    Using a real video background instead of a static colour solves TikTok's
    "low-quality / unoriginal static image" warning, because the first frame
    of the final video now has actual motion.

    Pipeline:
      1. Take the first HOOK_FRAME_DURATION seconds of the background video.
      2. Scale + pad to 1080x1920.
      3. Apply darkening overlay (eq brightness/contrast or colorchannelmixer).
      4. Apply all drawbox / drawtext decorations on top.
      Falls back to solid colour when background_video_path is None.
    """
    import tempfile, os as _os

    duration  = config.HOOK_FRAME_DURATION
    font_file = config.HOOK_FRAME_FONT_FILE
    font_opt  = f":fontfile='{font_file}'" if font_file else ""
    category  = config.HOOK_FRAME_CATEGORY.strip().upper()
    cat_fg    = config.HOOK_FRAME_CATEGORY_FG.strip()

    # --- Level 1: series colour theme ---
    sid = (series_id or "").upper()
    theme  = _SERIES_THEMES.get(sid, _SERIES_THEME_DEFAULT)
    bg     = theme["bg"]
    accent = theme["accent"]
    # Priority: channel override > .env explicit > series theme
    if hook_bg_override:
        bg = hook_bg_override
    elif config.HOOK_FRAME_BG_COLOR not in ("0x0a0a0a", "0x111827"):
        bg = config.HOOK_FRAME_BG_COLOR
    if hook_accent_override:
        accent = hook_accent_override
    elif config.HOOK_FRAME_ACCENT_COLOR not in ("0xFF3B30", "0xFBBF24"):
        accent = config.HOOK_FRAME_ACCENT_COLOR
    # Brand: override > config
    brand = hook_brand_override if hook_brand_override is not None else config.HOOK_FRAME_BRAND_LABEL.strip()
    cat_bg = config.HOOK_FRAME_CATEGORY_BG.strip() or accent

    # --- Level 2: part layout ---
    pn     = part_number or 1
    layout = _PART_LAYOUTS.get(pn, "left")

    # --- text wrapping ---
    # Finance layout uses wider lines (centred text looks better with fewer breaks)
    # Detect finance channel: brand override takes priority over config
    _brand_check = (hook_brand_override if hook_brand_override is not None else config.HOOK_FRAME_BRAND_LABEL or "").upper()
    is_finance_layout = _brand_check == "MONEY UA"
    chars_per_line = 16 if is_finance_layout else 12
    wrapped = _wrap_hook_text(hook_text, max_chars_per_line=chars_per_line)
    # NOTE: textfile= in ffmpeg does NOT support %% escaping unlike text=.
    # A bare % in textfile causes "Stray %" and silent text skip.
    # Solution: strip % entirely from hook text before writing to file.
    wrapped = wrapped.replace("%", "")
    # Write to a stable path next to output — avoids mkstemp path escaping issues.
    txt_path_str = str(output_path.parent / "_hook_text.txt")
    try:
        with open(txt_path_str, "w", encoding="utf-8") as f:
            f.write(wrapped)
        txt_esc = _escape_ffmpeg_filter_path(Path(txt_path_str))

        # Fixed grid
        X_LEFT      = 90
        FONTSIZE    = 96
        FONTSIZE_SM = 34
        FONTSIZE_BR = 28
        Y_BRAND     = 1820
        Y_PILL_MAP  = {"left": 260, "top": 320, "left_corner": 260, "both": 290}
        Y_PILL      = Y_PILL_MAP.get(layout, 260)
        PILL_H      = 52
        PILL_PAD_X  = 28
        Y_PILL_TXT  = Y_PILL + 14
        Y_MAIN      = Y_PILL + PILL_H + 28
        Y_SEP       = Y_MAIN + 330
        SEP_W       = 100

        # --- build overlay filter chain ---
        # Finance channel (MONEY UA brand) uses a different layout:
        # top+bottom bars + centred text + outline pill.
        # All other channels use the V2 left-bar layout.
        is_finance = is_finance_layout

        overlays: list[str] = []

        if is_finance:
            # === Finance layout: top+bottom bars + centred text + outline pill ===
            # Top bar: thick + thin
            overlays.append(f"drawbox=x=0:y=0:w=1080:h=10:color={accent}@1.0:t=fill")
            overlays.append(f"drawbox=x=0:y=10:w=1080:h=3:color={accent}@0.3:t=fill")
            # Bottom bar: thick + thin
            overlays.append(f"drawbox=x=0:y=1907:w=1080:h=3:color={accent}@0.3:t=fill")
            overlays.append(f"drawbox=x=0:y=1910:w=1080:h=10:color={accent}@1.0:t=fill")

            # Brand label centred top — larger and lower
            if brand:
                brand_esc = brand.replace("'", "\\\\'").replace(":", "\\\\:")
                overlays.append(
                    f"drawtext=text='{brand_esc}'{font_opt}"
                    f":fontsize=42:fontcolor={accent}"
                    f":x=(w-text_w)/2:y=120:fix_bounds=1"
                )

            # Separator line — centred, short
            Y_FIN_SEP = 580
            overlays.append(
                f"drawbox=x=490:y={Y_FIN_SEP}:w=100:h=5"
                f":color={accent}@1.0:t=fill"
            )

            # Main hook text — centred with fixed left margin
            # x=(w-text_w)/2 can fail with textfile= in some ffmpeg builds.
            # Use fixed x=60 with line_spacing so text sits visually centred.
            overlays.append(
                f"drawtext=textfile='{txt_esc}'{font_opt}"
                f":fontsize={FONTSIZE}:fontcolor=white"
                f":x=60:y={Y_FIN_SEP + 24}:line_spacing=14"
                f":borderw=3:bordercolor=black@0.6:fix_bounds=1"
            )

            # Category pill — outline style centred below text
            if category:
                pill_w = max(len(category) * 26 + PILL_PAD_X * 2, 160)
                pill_x = (1080 - pill_w) // 2
                Y_FIN_PILL = 1130
                t = 3
                overlays.append(f"drawbox=x={pill_x}:y={Y_FIN_PILL}:w={pill_w}:h={t}:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x={pill_x}:y={Y_FIN_PILL+PILL_H-t}:w={pill_w}:h={t}:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x={pill_x}:y={Y_FIN_PILL}:w={t}:h={PILL_H}:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x={pill_x+pill_w-t}:y={Y_FIN_PILL}:w={t}:h={PILL_H}:color={accent}@1.0:t=fill")
                cat_esc = category.replace("'", "\\\\'").replace(":", "\\\\:")
                overlays.append(
                    f"drawtext=text='{cat_esc}'{font_opt}"
                    f":fontsize={FONTSIZE_SM}:fontcolor={accent}"
                    f":x=(w-text_w)/2:y={Y_FIN_PILL + 14}:fix_bounds=1"
                )

        else:
            # === Standard V2 layout: left bar + pill + left-aligned text ===
            if layout == "left":
                overlays.append(f"drawbox=x=60:y=0:w=6:h=1920:color={accent}@1.0:t=fill")
            elif layout == "top":
                overlays.append(f"drawbox=x=0:y=0:w=1080:h=10:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x=0:y=0:w=8:h=960:color={accent}@0.6:t=fill")
            elif layout == "left_corner":
                overlays.append(f"drawbox=x=60:y=0:w=6:h=1920:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x=60:y=1760:w=200:h=8:color={accent}@0.8:t=fill")
            elif layout == "both":
                overlays.append(f"drawbox=x=60:y=0:w=6:h=1920:color={accent}@1.0:t=fill")
                overlays.append(f"drawbox=x=1008:y=0:w=12:h=1920:color={accent}@0.8:t=fill")

            if category:
                pill_w  = max(len(category) * 26 + PILL_PAD_X * 2, 160)
                overlays.append(
                    f"drawbox=x={X_LEFT}:y={Y_PILL}:w={pill_w}:h={PILL_H}"
                    f":color={cat_bg}@1.0:t=fill"
                )
                cat_esc = category.replace("'", "\\\\'").replace(":", "\\\\:")
                overlays.append(
                    f"drawtext=text='{cat_esc}'{font_opt}"
                    f":fontsize={FONTSIZE_SM}:fontcolor={cat_fg}"
                    f":x={X_LEFT + PILL_PAD_X}:y={Y_PILL_TXT}:fix_bounds=1"
                )

            overlays.append(
                f"drawtext=textfile='{txt_esc}'{font_opt}"
                f":fontsize={FONTSIZE}:fontcolor=white"
                f":x={X_LEFT}:y={Y_MAIN}:line_spacing=12"
                f":borderw=3:bordercolor=black@0.6:fix_bounds=1"
            )
            overlays.append(
                f"drawbox=x={X_LEFT}:y={Y_SEP}:w={SEP_W}:h=6:color={accent}@1.0:t=fill"
            )
            if brand:
                brand_esc = brand.replace("'", "\\\\'").replace(":", "\\\\:")
                overlays.append(
                    f"drawtext=text='{brand_esc}'{font_opt}"
                    f":fontsize={FONTSIZE_BR}:fontcolor=white@0.35"
                    f":x={X_LEFT}:y={Y_BRAND}:fix_bounds=1"
                )

        overlay_chain = ",".join(overlays)

        use_video_bg = (
            background_video_path is not None
            and background_video_path.is_file()
        )

        if use_video_bg:
            # --- VIDEO background path ---
            # 1. Trim + scale/pad background video to 1080x1920.
            # 2. Darken with eq (brightness -0.45, contrast 0.75).
            # 3. Add semi-transparent colour tint matching series theme.
            # 4. Apply overlay decorations.
            bg_esc = _escape_ffmpeg_filter_path(background_video_path)
            # Parse series bg hex to an RGB drawbox overlay for colour tint
            # bg is like "0x111827" — extract R G B for colorchannelmixer
            bg_hex = bg.lstrip("0x").lstrip("#").zfill(6)
            r = int(bg_hex[0:2], 16) / 255
            g = int(bg_hex[2:4], 16) / 255
            b = int(bg_hex[4:6], 16) / 255

            vf = (
                f"scale=1080:1920:force_original_aspect_ratio=decrease,"
                f"pad=1080:1920:(ow-iw)/2:(oh-ih)/2,"
                # darken the video significantly so text is readable
                f"eq=brightness=-0.42:contrast=0.72:saturation=0.5,"
                # colour tint: blend series bg colour at ~50% opacity via
                # a solid colour overlay drawn at 0.5 alpha
                f"drawbox=x=0:y=0:w=1080:h=1920"
                f":color={bg}@0.55:t=fill,"
                # decorations on top
                + overlay_chain
            )
            command = [
                ffmpeg_path, "-y",
                "-stream_loop", "-1",
                "-i", str(background_video_path),
                "-t", _format_duration(duration),
                "-vf", vf,
                "-c:v", config.OUTPUT_VIDEO_CODEC,
                "-preset", "fast",
                "-crf", str(config.OUTPUT_CRF),
                "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
                "-an",
                str(output_path),
            ]
        else:
            # --- FALLBACK: solid colour background (original behaviour) ---
            vf = overlay_chain
            command = [
                ffmpeg_path, "-y",
                "-f", "lavfi",
                "-i", f"color=c={bg}:size=1080x1920:rate=30:duration={duration}",
                "-vf", vf,
                "-c:v", config.OUTPUT_VIDEO_CODEC,
                "-preset", "fast",
                "-crf", str(config.OUTPUT_CRF),
                "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
                "-an",
                str(output_path),
            ]

        run_command(command, config.FFMPEG_TIMEOUT_SECONDS)
    finally:
        Path(txt_path_str).unlink(missing_ok=True)


def _render_hook_frame_classic(
    hook_text: str,
    output_path: Path,
    ffmpeg_path: str,
    run_command: CommandRunnerBoundary,
) -> None:
    """Original hook frame: dark bg + top/bottom accent bars + centred text."""
    # (unchanged from original _render_hook_frame)
    duration = config.HOOK_FRAME_DURATION
    bg = config.HOOK_FRAME_BG_COLOR
    fg = config.HOOK_FRAME_TEXT_COLOR
    accent = config.HOOK_FRAME_ACCENT_COLOR
    fontsize = config.HOOK_FRAME_FONT_SIZE
    font_file = config.HOOK_FRAME_FONT_FILE

    # Word-wrap: ffmpeg drawtext text= does NOT support newlines in list mode.
    # Solution: write text to a temp file and use textfile= instead.
    import tempfile, os
    wrapped = _wrap_hook_text(hook_text, max_chars_per_line=16)
    # textfile= reads real newlines from the file — no escaping needed.
    txt_fd, txt_path_str = tempfile.mkstemp(suffix=".txt")
    try:
        with os.fdopen(txt_fd, "w", encoding="utf-8") as f:
            f.write(wrapped)
        txt_path_esc = _escape_ffmpeg_filter_path(Path(txt_path_str))
        font_opt = f":fontfile='{font_file}'" if font_file else ""

        # Brand label (e.g. "MONEY UA") drawn in accent colour above main text.
        brand_label = config.HOOK_FRAME_BRAND_LABEL.strip()
        brand_drawtext = ""
        # Vertical shift: main text sits at (h-text_h)/2-60; brand label sits
        # ~100px above that → y_brand = (h-main_text_h)/2 - 160 (approximation).
        # We use a fixed y offset relative to center so it works at any font size.
        if brand_label:
            brand_escaped = brand_label.replace("'", "\\\\'").replace(":", "\\\\:")
            brand_drawtext = (
                f"drawtext=text='{brand_escaped}'{font_opt}"
                f":fontsize={max(fontsize // 3, 20)}:fontcolor={accent}"
                f":text_align=center"
                f":x=(w-text_w)/2:y=(h-text_h)/2-180"
                f":borderw=2:bordercolor=black@0.7"
                f":fix_bounds=1,"
            )

        drawtext = (
            # Top bar: thick + thin for visual weight
            f"drawbox=x=0:y=0:w=1080:h=12:color={accent}@1.0:t=fill,"
            f"drawbox=x=0:y=12:w=1080:h=3:color={accent}@0.4:t=fill,"
            # Bottom bar: same
            f"drawbox=x=0:y=1905:w=1080:h=3:color={accent}@0.4:t=fill,"
            f"drawbox=x=0:y=1908:w=1080:h=12:color={accent}@1.0:t=fill,"
            # Optional brand label in accent colour
            + brand_drawtext +
            # Main hook text: centred, shifted slightly above vertical centre
            f"drawtext=textfile='{txt_path_esc}'{font_opt}"
            f":fontsize={fontsize}:fontcolor={fg}"
            f":text_align=center"
            f":x=(w-text_w)/2:y=(h-text_h)/2-60"
            f":line_spacing=18"
            f":borderw=4:bordercolor=black@0.9"
            f":fix_bounds=1"
        )

        command = [
            ffmpeg_path, "-y",
            "-f", "lavfi",
            "-i", f"color=c={bg}:size=1080x1920:rate=30:duration={duration}",
            "-vf", drawtext,
            "-c:v", config.OUTPUT_VIDEO_CODEC,
            "-preset", "fast",
            "-crf", str(config.OUTPUT_CRF),
            "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
            "-an",
            str(output_path),
        ]
        run_command(command, config.FFMPEG_TIMEOUT_SECONDS)
    finally:
        Path(txt_path_str).unlink(missing_ok=True)


def _render_body(
    background_video_path: Path,
    audio_path: Path,
    output_path: Path,
    subtitle_path: Path | None,
    duration_seconds: float,
    ffmpeg_path: str,
    run_command: CommandRunnerBoundary,
    part_number: int | None = None,
    total_parts: int | None = None,
) -> None:
    """Render main video body (background + audio + subtitles) without hook."""
    command = _build_ffmpeg_render_command(
        background_video_path=background_video_path,
        audio_path=audio_path,
        output_path=output_path,
        subtitle_path=subtitle_path,
        duration_seconds=duration_seconds,
        ffmpeg_path=ffmpeg_path,
        part_number=part_number,
        total_parts=total_parts,
    )
    run_command(command, config.FFMPEG_TIMEOUT_SECONDS)


def _concat_hook_and_body(
    hook_path: Path,
    body_path: Path,
    output_path: Path,
    ffmpeg_path: str,
    run_command: CommandRunnerBoundary,
) -> None:
    """Concatenate hook frame (silent) + body (with audio) into final video.

    Hook has no audio stream — we pad it with silence using aevalsrc so the
    concat filter gets matching stream counts.
    """
    hook_dur = config.HOOK_FRAME_DURATION
    command = [
        ffmpeg_path, "-y",
        "-i", str(hook_path),
        "-i", str(body_path),
        "-filter_complex",
        (
            f"[0:v]setpts=PTS-STARTPTS[hv];"
            f"aevalsrc=0:c=stereo:s=44100:d={hook_dur}[ha];"
            f"[1:v]setpts=PTS-STARTPTS[bv];"
            f"[1:a]asetpts=PTS-STARTPTS[ba];"
            f"[hv][ha][bv][ba]concat=n=2:v=1:a=1[outv][outa]"
        ),
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", config.OUTPUT_VIDEO_CODEC,
        "-preset", config.OUTPUT_PRESET,
        "-crf", str(config.OUTPUT_CRF),
        "-pix_fmt", config.OUTPUT_PIXEL_FORMAT,
        "-c:a", config.OUTPUT_AUDIO_CODEC,
        "-movflags", config.OUTPUT_MOVFLAGS,
        str(output_path),
    ]
    run_command(command, config.FFMPEG_TIMEOUT_SECONDS)


def _validate_render_inputs(
    *,
    background_video_path: Path,
    audio_path: Path,
    subtitle_path: Path | None,
) -> None:
    _ensure_existing_file(background_video_path, "Background video file")
    _ensure_existing_file(audio_path, "Audio file")
    if subtitle_path is not None:
        _ensure_existing_file(subtitle_path, "Subtitle file")


def _ensure_existing_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise RenderError(f"{label} does not exist: {path}")


def _srt_to_ass(srt_path: Path) -> Path:
    """Convert SRT to ASS with correct style baked in.

    force_style in ffmpeg's subtitles= filter does NOT reliably apply
    Alignment to SRT files — ffmpeg ignores it and always renders SRT
    at the top-left. Converting to ASS first is the only reliable fix.

    The generated ASS file sits next to the SRT with a .ass extension.
    """
    import pysubs2  # already in requirements.txt

    subs = pysubs2.load(str(srt_path), encoding="utf-8")

    # Overwrite the default style with values from config.
    style = subs.styles.get("Default", pysubs2.SSAStyle())
    style.fontname = config.SUBTITLE_FONT_NAME
    style.fontsize = float(config.SUBTITLE_FONT_SIZE)
    style.bold = bool(config.SUBTITLE_BOLD)
    style.outline = float(config.SUBTITLE_OUTLINE_WIDTH)
    style.shadow = float(config.SUBTITLE_SHADOW_DEPTH)
    style.borderstyle = config.SUBTITLE_BORDER_STYLE
    style.alignment = config.SUBTITLE_ALIGNMENT
    style.marginv = config.SUBTITLE_MARGIN_V

    # pysubs2 uses &HAABBGGRR integers; parse the hex strings from config.
    def _parse_color(hex_str: str) -> int:
        # Accept &HAABBGGRR or &HBBGGRR (no alpha).
        h = hex_str.strip().lstrip("&H").lstrip("&h")
        return int(h, 16)

    try:
        style.primarycolor = pysubs2.Color(*_abgr_to_rgba(_parse_color(
            config.SUBTITLE_PRIMARY_COLOR)))
        style.outlinecolor = pysubs2.Color(*_abgr_to_rgba(_parse_color(
            config.SUBTITLE_OUTLINE_COLOR)))
        style.backcolor = pysubs2.Color(*_abgr_to_rgba(_parse_color(
            config.SUBTITLE_BACK_COLOR)))
    except Exception as exc:
        LOGGER.warning("Subtitle colour parse failed, using defaults: %s", exc)

    subs.styles["Default"] = style

    ass_path = srt_path.with_suffix(".ass")
    subs.save(str(ass_path))
    LOGGER.info("SRT converted to ASS: %s (alignment=%d)", ass_path, config.SUBTITLE_ALIGNMENT)
    return ass_path


def _abgr_to_rgba(abgr: int) -> tuple[int, int, int, int]:
    """Convert &HAABBGGRR integer to (r, g, b, a) tuple for pysubs2.Color."""
    a = (abgr >> 24) & 0xFF
    b = (abgr >> 16) & 0xFF
    g = (abgr >> 8) & 0xFF
    r = abgr & 0xFF
    return r, g, b, a


def _build_ffmpeg_render_command(
    *,
    background_video_path: Path,
    audio_path: Path,
    output_path: Path,
    subtitle_path: Path | None,
    duration_seconds: float,
    ffmpeg_path: str,
    part_number: int | None = None,
    total_parts: int | None = None,
) -> list[str]:
    command = [
        ffmpeg_path,
        "-y",
        "-stream_loop",
        "-1",
        "-i",
        str(background_video_path),
        "-i",
        str(audio_path),
        "-map_metadata",
        "-1",
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-t",
        _format_duration(duration_seconds),
        "-shortest",
    ]

    # Build video filter chain.
    vf_parts: list[str] = [
        "scale=1080:1920:force_original_aspect_ratio=decrease",
        "pad=1080:1920:(ow-iw)/2:(oh-ih)/2",
    ]

    if subtitle_path is not None:
        ass_path = _srt_to_ass(subtitle_path)
        escaped_path = _escape_ffmpeg_filter_path(ass_path)
        vf_parts.append(f"ass={escaped_path}")

    if part_number is not None and total_parts is not None:
        badge_text = f"{part_number}/{total_parts}"
        font_file = config.HOOK_FRAME_FONT_FILE
        font_opt = f":fontfile='{font_file}'" if font_file else ""
        # Top-right corner: 40px from right edge, 60px from top.
        # Semi-transparent dark pill background via box option.
        vf_parts.append(
            f"drawtext=text='{badge_text}'{font_opt}"
            f":fontsize=38:fontcolor=white"
            f":x=w-text_w-40:y=50"
            f":borderw=2:bordercolor=black@0.8"
            f":box=1:boxcolor=black@0.55:boxborderw=14"
        )

    command.extend(["-vf", ",".join(vf_parts)])
    command.extend(
        [
            "-c:v",
            config.OUTPUT_VIDEO_CODEC,
            "-preset",
            config.OUTPUT_PRESET,
            "-crf",
            str(config.OUTPUT_CRF),
            "-pix_fmt",
            config.OUTPUT_PIXEL_FORMAT,
            "-c:a",
            config.OUTPUT_AUDIO_CODEC,
            "-movflags",
            config.OUTPUT_MOVFLAGS,
            str(output_path),
        ]
    )
    return command


def _wrap_hook_text(text: str, max_chars_per_line: int = 18) -> str:
    """Word-wrap hook text so each line fits within 1080px at large font size.

    ffmpeg drawtext uses literal \\n as newline in the text= parameter.
    Splits on word boundaries; never cuts mid-word.
    """
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    current_len = 0

    for word in words:
        # +1 for the space between words
        needed = len(word) + (1 if current else 0)
        if current and current_len + needed > max_chars_per_line:
            lines.append(" ".join(current))
            current = [word]
            current_len = len(word)
        else:
            current.append(word)
            current_len += needed

    if current:
        lines.append(" ".join(current))

    # Return real newlines — text is written to a file and read via textfile=.
    return "\n".join(lines)


def _build_subtitle_force_style() -> str:
    # Commas in force_style must be escaped as \, because ffmpeg filter graph
    # parser treats bare commas as filter chain separators even inside option
    # values when the command is passed as a list (no shell quoting available).
    fields = [
        f"FontName={config.SUBTITLE_FONT_NAME}",
        f"FontSize={config.SUBTITLE_FONT_SIZE}",
        f"PrimaryColour={config.SUBTITLE_PRIMARY_COLOR}",
        f"OutlineColour={config.SUBTITLE_OUTLINE_COLOR}",
        f"BackColour={config.SUBTITLE_BACK_COLOR}",
        f"Bold={config.SUBTITLE_BOLD}",
        f"Outline={config.SUBTITLE_OUTLINE_WIDTH}",
        f"Shadow={config.SUBTITLE_SHADOW_DEPTH}",
        f"Alignment={config.SUBTITLE_ALIGNMENT}",
        f"MarginV={config.SUBTITLE_MARGIN_V}",
        f"BorderStyle={config.SUBTITLE_BORDER_STYLE}",
    ]
    return "\\,".join(fields)


def _probe_media_duration(media_path: Path) -> float:
    command = [
        config.FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(media_path),
    ]
    try:
        completed_process = _run_command(command, config.FFPROBE_TIMEOUT_SECONDS)
    except RenderError as exc:
        raise RenderError(f"FFprobe duration probe failed: {exc}") from exc

    stdout = completed_process.stdout.strip()
    try:
        return float(stdout)
    except ValueError as exc:
        raise RenderError(f"Unreadable media duration output: {stdout!r}") from exc


def _run_command(command: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        completed_process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise RenderError(f"Command not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RenderError(f"Command timed out: {command[0]}") from exc

    if completed_process.returncode != 0:
        stderr_output = completed_process.stderr.strip() or "Command returned non-zero exit code."
        raise RenderError(stderr_output)

    return completed_process


def _format_duration(duration_seconds: float) -> str:
    return f"{duration_seconds:.3f}"


def _escape_ffmpeg_filter_path(path: Path) -> str:
    return (
        path.as_posix()
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
        .replace(",", "\\,")
    )

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import math
import urllib.request
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps


CELL_WIDTH = 640
CELL_HEIGHT = 640
PADDING = 24
HEADER_HEIGHT = 88
FOOTER_HEIGHT = 36
MAX_IMAGES = 6
REQUEST_TIMEOUT_SECONDS = 20


def _load_image(url: str) -> Image.Image:
    with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        payload = response.read()
    return Image.open(io.BytesIO(payload)).convert("RGB")


def _fit_image(image: Image.Image) -> Image.Image:
    return ImageOps.fit(image, (CELL_WIDTH, CELL_HEIGHT), method=Image.Resampling.LANCZOS)


def _draw_badge(draw: ImageDraw.ImageDraw, index: int, left: int, top: int) -> None:
    badge_w = 54
    badge_h = 38
    badge_left = left + 18
    badge_top = top + 18
    draw.rounded_rectangle(
        (badge_left, badge_top, badge_left + badge_w, badge_top + badge_h),
        radius=12,
        fill=(0, 0, 0, 190),
    )
    font = ImageFont.load_default()
    label = str(index)
    label_box = draw.textbbox((0, 0), label, font=font)
    label_w = label_box[2] - label_box[0]
    label_h = label_box[3] - label_box[1]
    draw.text(
        (
            badge_left + (badge_w - label_w) / 2,
            badge_top + (badge_h - label_h) / 2 - 1,
        ),
        label,
        fill=(255, 255, 255),
        font=font,
    )


def build_collage(output_path: Path, urls: list[str], title: str | None = None) -> Path:
    trimmed_urls = []
    seen = set()
    for url in urls:
        candidate = str(url).strip()
        if not candidate or candidate in seen:
            continue
        trimmed_urls.append(candidate)
        seen.add(candidate)
        if len(trimmed_urls) >= MAX_IMAGES:
            break
    if not trimmed_urls:
        raise ValueError("No image URLs were provided for the collage.")

    images = [_fit_image(_load_image(url)) for url in trimmed_urls]
    columns = 2 if len(images) > 1 else 1
    rows = math.ceil(len(images) / columns)
    canvas_width = columns * CELL_WIDTH + (columns + 1) * PADDING
    canvas_height = HEADER_HEIGHT + rows * CELL_HEIGHT + (rows + 1) * PADDING + FOOTER_HEIGHT
    canvas = Image.new("RGB", (canvas_width, canvas_height), (248, 248, 248))
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default()

    title_text = title.strip() if title else "Approval Preview"
    subtitle = f"{len(images)} image{'s' if len(images) != 1 else ''}"
    draw.text((PADDING, 18), title_text, fill=(24, 24, 24), font=font)
    draw.text((PADDING, 46), subtitle, fill=(90, 90, 90), font=font)

    for idx, image in enumerate(images, start=1):
        row = (idx - 1) // columns
        column = (idx - 1) % columns
        left = PADDING + column * (CELL_WIDTH + PADDING)
        top = HEADER_HEIGHT + PADDING + row * (CELL_HEIGHT + PADDING)
        canvas.paste(image, (left, top))
        draw.rounded_rectangle(
            (left, top, left + CELL_WIDTH, top + CELL_HEIGHT),
            radius=18,
            outline=(225, 225, 225),
            width=2,
        )
        _draw_badge(draw, idx, left, top)

    if len(urls) > len(images):
        extra = len(urls) - len(images)
        draw.text(
            (PADDING, canvas_height - FOOTER_HEIGHT + 6),
            f"+{extra} more image{'s' if extra != 1 else ''} not shown",
            fill=(90, 90, 90),
            font=font,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path, format="PNG")
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a WhatsApp-ready collage from remote image URLs.")
    parser.add_argument("--output", required=True, help="Where to write the PNG collage.")
    parser.add_argument("--title", default="", help="Optional collage title.")
    parser.add_argument("--url", action="append", default=[], help="Image URL to include. Repeat for each item.")
    args = parser.parse_args()
    build_collage(Path(args.output), list(args.url), args.title or None)
    print(Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Synchronise accurate SRT/VTT text with word-level timings.

Two modes are supported:

1. If the subtitle file already embeds word timestamps (e.g. YouTube VTT with
   `<HH:MM:SS.mmm>` tags), those are used directly.
2. Otherwise, provide an ASR transcript (e.g. Vosk/videogrep JSON) that contains
   per-word start/end times. The script will align the high-quality subtitle
   text to the ASR timings and emit a JSON compatible with `transcribe.py`.

Usage examples:

    # Inline timed subtitles
    python3 srt_word_boundaries.py captions.vtt -o captions.json

    # Sentence-level subtitles + Vosk transcript
    python3 srt_word_boundaries.py captions.srt --asr-json vosk.json -o captions.json

    # Inspect alignment stats without writing a file
    python3 srt_word_boundaries.py captions.srt --asr-json vosk.json --dry-run
"""

from __future__ import annotations

import argparse
import html
import json
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Regular expressions & constants
# -----------------------------------------------------------------------------

TIMESTAMP_LINE_RE = re.compile(
    r"(?P<start>\d{1,2}:\d{2}:\d{2}(?:[.,]\d{1,3})?)\s*-->\s*"
    r"(?P<end>\d{1,2}:\d{2}:\d{2}(?:[.,]\d{1,3})?)"
)
TIMED_TOKEN_RE = re.compile(
    r"<(?P<ts>\d{1,2}:\d{2}:\d{2}(?:[.,]\d{1,3})?)>"
    r"<c[^>]*>(?P<word>.*?)</c>",
    re.DOTALL,
)
HAS_TIMED_TOKEN_RE = re.compile(r"<\d{1,2}:\d{2}:\d{2}(?:[.,]\d{1,3})?>")
WORD_TOKEN_RE = re.compile(
    r"[0-9A-Za-zÀ-ÖØ-öø-ÿßÄÖÜäöü]+(?:'[0-9A-Za-zÀ-ÖØ-öø-ÿßÄÖÜäöü]+)?",
    re.UNICODE,
)

DEFAULT_TIME_MARGIN = 1.0  # seconds
DEFAULT_MATCH_WINDOW = 80  # ASR words to scan per subtitle token
FUZZY_RATIO_THRESHOLD = 0.82
MIN_FALLBACK_MATCH_RATIO = 0.4


# -----------------------------------------------------------------------------
# Data containers
# -----------------------------------------------------------------------------

@dataclass
class SubtitleCue:
    start: float
    end: float
    lines: List[str]


@dataclass
class TimedToken:
    start: float
    text: str


@dataclass
class SRTWord:
    text: str
    norm: str


@dataclass
class ASRWord:
    text: str
    norm: str
    start: float
    end: float
    conf: float


# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------

def _clean_text(text: str) -> str:
    """Normalise subtitle text by unescaping HTML entities and removing tags."""
    text = html.unescape(text)
    text = re.sub(r"</?c[^>]*>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</?i>", "", text, flags=re.IGNORECASE)
    text = text.replace("\u2028", " ")
    return text.strip()


def _timestamp_to_seconds(value: str) -> float:
    """Convert HH:MM:SS.mmm or HH:MM:SS,mmm to seconds."""
    value = value.strip().replace(",", ".")
    parts = value.split(":")
    if len(parts) == 2:
        hours = 0
        minutes, seconds = parts
    elif len(parts) == 3:
        hours, minutes, seconds = parts
    else:
        raise ValueError(f"Unsupported timestamp format: {value!r}")
    hours_i = int(hours)
    minutes_i = int(minutes)
    seconds_f = float(seconds)
    return hours_i * 3600 + minutes_i * 60 + seconds_f


# -----------------------------------------------------------------------------
# Subtitle parsing & inline timing handling
# -----------------------------------------------------------------------------

def parse_subtitle_file(path: Path) -> List[SubtitleCue]:
    """Parse SRT/VTT file into cues preserving raw text lines."""
    if not path.exists():
        raise FileNotFoundError(path)

    with path.open("r", encoding="utf-8-sig") as handle:
        raw_lines = [line.rstrip("\n\r") for line in handle]

    cues: List[SubtitleCue] = []
    total = len(raw_lines)
    idx = 0

    while idx < total:
        line = raw_lines[idx]
        stripped = line.strip()

        if not stripped:
            idx += 1
            continue

        if stripped.upper() == "WEBVTT":
            idx += 1
            continue

        if stripped.startswith("NOTE"):
            idx += 1
            while idx < total and raw_lines[idx].strip():
                idx += 1
            continue

        if "-->" not in line:
            idx += 1
            continue

        match = TIMESTAMP_LINE_RE.search(line)
        if not match:
            idx += 1
            continue

        start = _timestamp_to_seconds(match.group("start"))
        end = _timestamp_to_seconds(match.group("end"))
        idx += 1

        while idx < total and raw_lines[idx].strip() == "":
            idx += 1

        text_lines: List[str] = []
        while idx < total:
            current_line = raw_lines[idx]
            stripped_line = current_line.strip()
            if not stripped_line:
                idx += 1
                break
            if "-->" in current_line:
                break
            text_lines.append(current_line)
            idx += 1

        processed_lines: List[str] = []
        for raw in text_lines:
            cleaned = raw.replace("\\N", "\n")
            processed_lines.extend(part for part in cleaned.split("\n") if part)

        cues.append(SubtitleCue(start=start, end=end, lines=processed_lines))

    return cues


def _split_text_into_words(text: str) -> List[str]:
    return [token for token in text.split() if token]


def _extract_timed_tokens(line: str, cue_start: float) -> List[TimedToken]:
    tokens: List[TimedToken] = []
    matches = list(TIMED_TOKEN_RE.finditer(line))

    if not matches:
        cleaned = _clean_text(line)
        if cleaned:
            tokens.append(TimedToken(start=cue_start, text=cleaned))
        return tokens

    prefix = line[: matches[0].start()]
    prefix_text = _clean_text(prefix)
    if prefix_text:
        tokens.append(TimedToken(start=cue_start, text=prefix_text))

    for match in matches:
        token_text = _clean_text(match.group("word"))
        if not token_text:
            continue
        token_start = _timestamp_to_seconds(match.group("ts"))
        tokens.append(TimedToken(start=token_start, text=token_text))

    return tokens


def build_word_entries_from_inline(cue: SubtitleCue, allow_plain_fallback: bool = True) -> List[dict]:
    timed_tokens: List[TimedToken] = []
    fallback_lines: List[str] = []

    for line in cue.lines:
        if not line.strip():
            continue
        if HAS_TIMED_TOKEN_RE.search(line):
            timed_tokens.extend(_extract_timed_tokens(line, cue.start))
        else:
            cleaned = _clean_text(line)
            if cleaned:
                fallback_lines.append(cleaned)

    if timed_tokens:
        tokens = sorted(timed_tokens, key=lambda item: item.start)
    elif fallback_lines and allow_plain_fallback:
        combined = " ".join(fallback_lines)
        tokens = [TimedToken(start=cue.start, text=combined)]
    else:
        return []

    words: List[dict] = []
    for index, token in enumerate(tokens):
        next_start = tokens[index + 1].start if index + 1 < len(tokens) else cue.end
        if next_start < token.start:
            next_start = token.start

        word_texts = _split_text_into_words(token.text)
        if not word_texts:
            continue

        duration = max(next_start - token.start, 0.0)
        segment_count = len(word_texts)
        step = duration / segment_count if segment_count > 0 else 0.0

        for offset, word_text in enumerate(word_texts):
            word_start = token.start + offset * step
            word_end = token.start + (offset + 1) * step if step > 0 else max(next_start, word_start)
            words.append(
                {
                    "word": word_text,
                    "start": round(word_start, 6),
                    "end": round(word_end, 6),
                    "conf": 1.0,
                }
            )

    if words and words[-1]["end"] < cue.end:
        words[-1]["end"] = round(cue.end, 6)

    return words


def cues_to_segments_with_inline_timing(cues: Sequence[SubtitleCue]) -> List[dict]:
    has_timed_tokens = any(
        HAS_TIMED_TOKEN_RE.search(line)
        for cue in cues
        for line in cue.lines
        if line
    )

    segments: List[dict] = []
    for cue in cues:
        contain_timed_tokens = any(HAS_TIMED_TOKEN_RE.search(line) for line in cue.lines if line)
        if has_timed_tokens and not contain_timed_tokens:
            continue

        word_entries = build_word_entries_from_inline(cue, allow_plain_fallback=not has_timed_tokens)
        if not word_entries:
            continue

        content = " ".join(word["word"] for word in word_entries)
        segments.append(
            {
                "content": content,
                "start": round(word_entries[0]["start"], 6),
                "end": round(word_entries[-1]["end"], 6),
                "words": word_entries,
            }
        )
    return segments


# -----------------------------------------------------------------------------
# Alignment against ASR word timings
# -----------------------------------------------------------------------------

def normalize_word(word: str) -> str:
    normalized = unicodedata.normalize("NFKC", word).lower()
    return "".join(ch for ch in normalized if ch.isalnum())


def extract_srt_words(cue: SubtitleCue) -> List[SRTWord]:
    words: List[SRTWord] = []
    for line in cue.lines:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        for match in WORD_TOKEN_RE.finditer(cleaned):
            token = match.group()
            norm = normalize_word(token)
            if not norm:
                continue
            words.append(SRTWord(text=token, norm=norm))
    return words


def load_asr_words(path: Path) -> List[ASRWord]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    collected: List[ASRWord] = []

    def consume_word(item: Dict) -> None:
        try:
            start = float(item["start"])
            end = float(item["end"])
            text = str(item["word"])
        except (KeyError, TypeError, ValueError):
            return
        norm = normalize_word(text)
        if not norm:
            return
        conf = item.get("conf", item.get("confidence", 1.0))
        try:
            conf_f = float(conf)
        except (TypeError, ValueError):
            conf_f = 1.0
        collected.append(ASRWord(text=text, norm=norm, start=start, end=end, conf=conf_f))

    def walk(payload: object) -> None:
        if isinstance(payload, list):
            for element in payload:
                if isinstance(element, dict):
                    if "words" in element and isinstance(element["words"], list):
                        for word in element["words"]:
                            if isinstance(word, dict):
                                consume_word(word)
                    elif {"word", "start", "end"}.issubset(element.keys()):
                        consume_word(element)
        elif isinstance(payload, dict):
            if "words" in payload and isinstance(payload["words"], list):
                for word in payload["words"]:
                    if isinstance(word, dict):
                        consume_word(word)
            if "result" in payload and isinstance(payload["result"], list):
                for word in payload["result"]:
                    if isinstance(word, dict):
                        consume_word(word)

    walk(data)
    collected.sort(key=lambda item: (item.start, item.end))
    return collected


def match_words(srt_words: Sequence[SRTWord], asr_window: Sequence[ASRWord], search_window: int) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    asr_len = len(asr_window)
    pointer = 0

    for srt_idx, srt_word in enumerate(srt_words):
        best_idx = None
        best_score = 0.0
        search_limit = min(asr_len, pointer + max(search_window, 1))

        for cand_idx in range(pointer, search_limit):
            candidate = asr_window[cand_idx]
            if srt_word.norm == candidate.norm:
                best_idx = cand_idx
                best_score = 1.0
                break
            score = SequenceMatcher(None, srt_word.norm, candidate.norm).ratio()
            if score >= FUZZY_RATIO_THRESHOLD and score > best_score:
                best_idx = cand_idx
                best_score = score

        if best_idx is not None:
            mapping[srt_idx] = best_idx
            pointer = best_idx + 1

    return mapping


def fill_range(target: List[Optional[Tuple[float, float]]], start_idx: int, end_idx: int, interval_start: float, interval_end: float) -> None:
    if start_idx > end_idx:
        return
    interval_start = float(interval_start)
    interval_end = float(interval_end)
    if interval_end < interval_start:
        interval_end = interval_start
    count = end_idx - start_idx + 1
    if count <= 0:
        return
    step = (interval_end - interval_start) / count if count else 0.0
    current = interval_start
    for idx in range(start_idx, end_idx + 1):
        next_time = current + step
        if next_time < current:
            next_time = current
        target[idx] = (current, next_time)
        current = next_time
    if target[end_idx] is not None:
        start_val, _ = target[end_idx]
        target[end_idx] = (start_val, max(target[end_idx][1], interval_end))


def interpolate_word_times(known_times: List[Optional[Tuple[float, float]]], segment_start: float, segment_end: float) -> List[Tuple[float, float]]:
    n = len(known_times)
    result: List[Optional[Tuple[float, float]]] = [None] * n
    known_indices = [idx for idx, value in enumerate(known_times) if value is not None]

    if not known_indices:
        fill_range(result, 0, n - 1, segment_start, segment_end)
        return [value if value is not None else (segment_start, segment_start) for value in result]

    for idx in known_indices:
        start, end = known_times[idx]  # type: ignore[misc]
        start = max(segment_start, float(start))
        end = min(segment_end, float(end))
        if end < start:
            end = start
        result[idx] = (start, end)

    first_known = known_indices[0]
    if first_known > 0:
        fill_range(result, 0, first_known - 1, segment_start, result[first_known][0])  # type: ignore[index]

    for left, right in zip(known_indices, known_indices[1:]):
        fill_range(result, left + 1, right - 1, result[left][1], result[right][0])  # type: ignore[index]

    last_known = known_indices[-1]
    if last_known < n - 1:
        fill_range(result, last_known + 1, n - 1, result[last_known][1], segment_end)  # type: ignore[index]

    return [value if value is not None else (segment_start, segment_start) for value in result]


def build_entries_from_alignment(
    cue: SubtitleCue,
    srt_words: Sequence[SRTWord],
    mapping: Dict[int, int],
    asr_words: Sequence[ASRWord],
) -> Tuple[List[dict], int]:
    total = len(srt_words)
    if total == 0:
        return [], 0

    known_times: List[Optional[Tuple[float, float]]] = [None] * total
    confs: List[float] = [0.0] * total
    matched = 0

    for srt_idx, asr_idx in mapping.items():
        if asr_idx < 0 or asr_idx >= len(asr_words):
            continue
        asr_word = asr_words[asr_idx]
        start = max(cue.start, asr_word.start)
        end = min(cue.end, asr_word.end)
        if end < start:
            end = start
        known_times[srt_idx] = (start, end)
        confs[srt_idx] = asr_word.conf
        matched += 1

    filled = interpolate_word_times(known_times, cue.start, cue.end)
    entries: List[dict] = []
    for idx, word in enumerate(srt_words):
        start, end = filled[idx]
        entries.append(
            {
                "word": word.text,
                "start": round(start, 6),
                "end": round(end, 6),
                "conf": float(confs[idx]) if confs[idx] else 0.0,
            }
        )

    return entries, matched


def align_cues_with_asr(
    cues: Sequence[SubtitleCue],
    asr_words: Sequence[ASRWord],
    time_margin: float = DEFAULT_TIME_MARGIN,
    match_window: int = DEFAULT_MATCH_WINDOW,
) -> Tuple[List[dict], Dict[str, float]]:
    segments: List[dict] = []
    pointer = 0
    asr_len = len(asr_words)

    total_words = 0
    matched_words = 0

    for cue in cues:
        srt_words = extract_srt_words(cue)
        if not srt_words:
            continue

        total_words += len(srt_words)

        while pointer < asr_len and asr_words[pointer].end < cue.start - time_margin:
            pointer += 1

        window_end = pointer
        while window_end < asr_len and asr_words[window_end].start <= cue.end + time_margin:
            window_end += 1

        if window_end <= pointer:
            window_end = min(asr_len, pointer + match_window)

        window = asr_words[pointer:window_end]
        local_mapping = match_words(srt_words, window, match_window)

        if len(local_mapping) < len(srt_words) * MIN_FALLBACK_MATCH_RATIO and window_end < asr_len:
            extended_end = min(asr_len, window_end + match_window)
            window = asr_words[pointer:extended_end]
            local_mapping = match_words(srt_words, window, match_window)
            window_end = extended_end

        global_mapping = {s_idx: pointer + local_idx for s_idx, local_idx in local_mapping.items()}

        if local_mapping:
            pointer = pointer + max(local_mapping.values()) + 1
        else:
            pointer = window_end

        entries, matched = build_entries_from_alignment(cue, srt_words, global_mapping, asr_words)
        matched_words += matched

        if not entries:
            continue

        content = " ".join(entry["word"] for entry in entries)
        segments.append(
            {
                "content": content,
                "start": round(entries[0]["start"], 6),
                "end": round(entries[-1]["end"], 6),
                "words": entries,
            }
        )

    stats = {
        "segments": len(segments),
        "words": total_words,
        "matched_words": matched_words,
        "mode": "aligned",
    }
    return segments, stats


# -----------------------------------------------------------------------------
# Conversion entry point & CLI
# -----------------------------------------------------------------------------

def convert_subtitles(
    input_path: Path,
    output_path: Optional[Path] = None,
    *,
    asr_path: Optional[Path] = None,
    time_margin: float = DEFAULT_TIME_MARGIN,
    match_window: int = DEFAULT_MATCH_WINDOW,
) -> Tuple[List[dict], Dict[str, float]]:
    cues = parse_subtitle_file(input_path)

    has_inline = any(
        HAS_TIMED_TOKEN_RE.search(line)
        for cue in cues
        for line in cue.lines
        if line
    )

    if asr_path is not None:
        asr_words = load_asr_words(asr_path)
        if not asr_words:
            raise ValueError(f"No ASR words found in {asr_path}")
        segments, stats = align_cues_with_asr(cues, asr_words, time_margin=time_margin, match_window=match_window)
    elif has_inline:
        segments = cues_to_segments_with_inline_timing(cues)
        stats = {
            "segments": len(segments),
            "words": sum(len(seg["words"]) for seg in segments),
            "matched_words": sum(len(seg["words"]) for seg in segments),
            "mode": "inline",
        }
    else:
        raise ValueError(
            "Subtitle file lacks per-word timing information. Provide an ASR JSON via --asr-json."
        )

    if not segments:
        raise ValueError(f"No segments could be extracted from {input_path}")

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(segments, handle, ensure_ascii=False, indent=2)

    return segments, stats


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build per-word transcripts from SRT/VTT subtitles.")
    parser.add_argument("subtitle", type=Path, help="Input subtitle file (.srt or .vtt).")
    parser.add_argument(
        "--asr-json",
        type=Path,
        help="Optional ASR transcript (Vosk/videogrep JSON with per-word timings). Required when no inline timings exist.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional path for the generated JSON transcript. Defaults to '<subtitle>.json'.",
    )
    parser.add_argument(
        "--time-margin",
        type=float,
        default=DEFAULT_TIME_MARGIN,
        help=f"Seconds of padding around subtitle cues when selecting ASR words (default: {DEFAULT_TIME_MARGIN}).",
    )
    parser.add_argument(
        "--match-window",
        type=int,
        default=DEFAULT_MATCH_WINDOW,
        help=f"Maximum ASR words to scan per subtitle token during alignment (default: {DEFAULT_MATCH_WINDOW}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse inputs and print alignment statistics without writing a file.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    input_path: Path = args.subtitle
    output_path: Optional[Path] = args.output
    if output_path is None and not args.dry_run:
        output_path = input_path.with_suffix(".json")

    segments, stats = convert_subtitles(
        input_path,
        output_path if not args.dry_run else None,
        asr_path=args.asr_json,
        time_margin=args.time_margin,
        match_window=args.match_window,
    )

    total_words = stats.get("words", 0) or 0
    matched_words = stats.get("matched_words", 0) or 0
    match_pct = (matched_words / total_words * 100.0) if total_words else 0.0
    mode = stats.get("mode", "inline")

    if args.dry_run:
        print(
            f"Parsed {stats.get('segments', 0)} segments / {total_words} words "
            f"(matched {match_pct:.1f}% via {mode}) from {input_path.name}"
        )
    elif output_path:
        print(
            f"Wrote {len(segments)} segments to {output_path} "
            f"(matched {match_pct:.1f}% of words via {mode})"
        )


if __name__ == "__main__":
    main()

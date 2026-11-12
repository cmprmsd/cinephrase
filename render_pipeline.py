"""Sample-accurate cutting shared by the browser preview and the timeline merge.

Why this module exists
----------------------
A cut is only "exact" if preview and export agree on what a millisecond means. Both therefore go through the
same two primitives here:

* ``audio_filter_chain`` / ``extract_pcm``: decode ``[start, start + duration)`` of a media file to 48 kHz
  stereo PCM with ffmpeg's accurate seek (sample-exact), optionally time-stretched with ``atempo``.
  The preview plays exactly these samples on the Web Audio clock; the merge concatenates exactly these samples.
* ``plan_timeline``: gives every timeline segment an exact number of audio samples and video frames.
  Audio is contiguous (no padding between clips), video cuts land on the nearest frame of the output grid,
  so the error per cut is at most half a frame and never accumulates.

The module has no Flask dependency so it can be tested on its own.
"""
from __future__ import annotations

import json
import math
import os
import shutil
import struct
import subprocess
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

AUDIO_RATE = 48000
AUDIO_CHANNELS = 2
BYTES_PER_FRAME = 2 * AUDIO_CHANNELS  # s16le
MIN_SPEED = 0.5
MAX_SPEED = 2.0
MIN_SEGMENT_SECONDS = 0.02
DEFAULT_FPS = Fraction(30)
CPU_VIDEO_ARGS = ['-c:v', 'libx264', '-preset', 'medium', '-crf', '18']
FFMPEG_TIMEOUT = 600


class RenderError(RuntimeError):
    """A user-presentable rendering failure."""


# --------------------------------------------------------------------------------------------------------------
# Probing
# --------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class MediaInfo:
    has_video: bool
    has_audio: bool
    width: int
    height: int
    fps: Optional[Fraction]
    duration: Optional[float]          # container duration in seconds
    audio_duration: Optional[float]    # audio stream duration in seconds (falls back to container duration)


def _parse_rate(value: Optional[str]) -> Optional[Fraction]:
    if not value or value in ('0/0', 'N/A'):
        return None
    try:
        rate = Fraction(value)
    except (ValueError, ZeroDivisionError):
        return None
    return rate if rate > 0 else None


def _to_float(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def probe_media(path: str) -> MediaInfo:
    """Read the stream properties the planner needs with a single ffprobe call."""
    command = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'stream=codec_type,width,height,r_frame_rate,avg_frame_rate,duration:format=duration',
        '-of', 'json', path,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except (subprocess.SubprocessError, OSError) as exc:
        raise RenderError(f'Could not probe {os.path.basename(path)}: {exc}') from exc
    if result.returncode != 0:
        raise RenderError(f'Could not probe {os.path.basename(path)}: {result.stderr.strip()[-300:]}')
    try:
        data = json.loads(result.stdout or '{}')
    except json.JSONDecodeError as exc:
        raise RenderError(f'Unreadable ffprobe output for {os.path.basename(path)}') from exc

    streams = data.get('streams') or []
    video = next((s for s in streams if s.get('codec_type') == 'video'), None)
    audio = next((s for s in streams if s.get('codec_type') == 'audio'), None)
    duration = _to_float((data.get('format') or {}).get('duration'))

    fps = None
    if video:
        fps = _parse_rate(video.get('r_frame_rate'))
        average = _parse_rate(video.get('avg_frame_rate'))
        # r_frame_rate is the timebase-derived rate and is absurd (e.g. 90000/1) for some variable-rate files.
        if fps is None or fps > 120 or (average and fps > average * 2):
            fps = average or fps
        if fps is not None and fps > 120:
            fps = None
    return MediaInfo(
        has_video=video is not None,
        has_audio=audio is not None,
        width=int(video.get('width') or 0) if video else 0,
        height=int(video.get('height') or 0) if video else 0,
        fps=fps,
        duration=duration,
        audio_duration=(_to_float(audio.get('duration')) if audio else None) or duration,
    )


# --------------------------------------------------------------------------------------------------------------
# Planning
# --------------------------------------------------------------------------------------------------------------

@dataclass
class Segment:
    """One cut: ``duration`` seconds of ``media`` starting at ``start``, played back at ``speed``."""
    media: str
    start: float
    duration: float
    speed: float = 1.0
    title: str = ''
    # filled in by plan_timeline()
    samples: int = 0
    frames: int = 0
    info: Optional[MediaInfo] = field(default=None, repr=False)


def clamp_speed(value) -> float:
    speed = _to_float(value)
    if speed is None or speed <= 0:
        return 1.0
    return max(MIN_SPEED, min(MAX_SPEED, speed))


def _exact(value: float) -> Fraction:
    """Seconds as an exact fraction at microsecond resolution (client values are whole milliseconds)."""
    return Fraction(round(value * 1_000_000), 1_000_000)


def output_seconds(duration: float, speed: float) -> Fraction:
    return _exact(duration) / Fraction(str(round(speed, 4)))


def plan_timeline(segments: Sequence[Segment], fps: Fraction, sample_rate: int = AUDIO_RATE) -> Fraction:
    """Assign every segment its exact audio sample and video frame budget. Returns the total duration.

    Cut ``k`` sits at ``t_k = sum(output durations before it)``. Segment ``k`` owns the audio samples
    ``[round(t_k * sr), round(t_(k+1) * sr))`` and the frames ``[round(t_k * fps), round(t_(k+1) * fps))``,
    so rounding never accumulates: after any number of clips the audio is within half a sample and the picture
    within half a frame of the ideal cut position.
    """
    position = Fraction(0)
    for segment in segments:
        end = position + output_seconds(segment.duration, segment.speed)
        segment.samples = round(end * sample_rate) - round(position * sample_rate)
        segment.frames = round(end * fps) - round(position * fps)
        position = end
    return position


def pick_fps(infos: Sequence[MediaInfo]) -> Fraction:
    """Most common source frame rate (ties: the higher one), so the usual single-rate library is untouched."""
    rates = Counter(info.fps for info in infos if info.has_video and info.fps)
    if not rates:
        return DEFAULT_FPS
    best = max(rates.items(), key=lambda item: (item[1], item[0]))[0]
    return best.limit_denominator(1001)


def pick_resolution(infos: Sequence[MediaInfo]) -> Tuple[int, int]:
    """Most common source resolution (ties: the larger one), rounded down to even dimensions for yuv420p."""
    sizes = Counter((info.width, info.height) for info in infos if info.has_video and info.width and info.height)
    if not sizes:
        return 1920, 1080
    width, height = max(sizes.items(), key=lambda item: (item[1], item[0][0] * item[0][1]))[0]
    return max(2, width - width % 2), max(2, height - height % 2)


# --------------------------------------------------------------------------------------------------------------
# ffmpeg command construction
# --------------------------------------------------------------------------------------------------------------

def _seconds(value: float) -> str:
    return f'{max(0.0, value):.6f}'


def _rate(fps: Fraction) -> str:
    return f'{fps.numerator}/{fps.denominator}'


def audio_filter_chain(speed: float = 1.0, samples: Optional[int] = None, sample_rate: int = AUDIO_RATE) -> str:
    """The one audio chain used by preview and merge: [stretch] -> 48 kHz stereo s16 -> [exact length]."""
    filters = []
    if abs(speed - 1.0) > 1e-6:
        filters.append(f'atempo={speed:.4f}')
    filters.append(f'aresample={sample_rate}')
    filters.append(f'aformat=sample_fmts=s16:channel_layouts=stereo')
    if samples is not None:
        # Pad with silence, then cut to the planned length, so every segment is exactly `samples` long.
        filters.append('apad')
        filters.append(f'atrim=end_sample={samples}')
    return ','.join(filters)


def audio_command(media: str, start: Optional[float], duration: Optional[float], speed: float = 1.0,
                  samples: Optional[int] = None, output: str = '-', container: str = 's16le') -> List[str]:
    command = ['ffmpeg', '-v', 'error', '-nostdin', '-y']
    if start is not None:
        command += ['-ss', _seconds(start)]
    if duration is not None:
        command += ['-t', _seconds(duration)]
    command += ['-i', media, '-vn', '-sn', '-dn',
                '-af', audio_filter_chain(speed, samples),
                '-c:a', 'pcm_s16le', '-f', container, output]
    return command


def silence_command(samples: int, output: str) -> List[str]:
    return ['ffmpeg', '-v', 'error', '-nostdin', '-y',
            '-f', 'lavfi', '-i', f'anullsrc=r={AUDIO_RATE}:cl=stereo',
            '-af', f'atrim=end_sample={samples}',
            '-c:a', 'pcm_s16le', '-f', 's16le', output]


def video_command(segment: Segment, fps: Fraction, width: int, height: int, encoder_args: Sequence[str],
                  output: str) -> List[str]:
    """Encode exactly ``segment.frames`` constant-rate frames starting at the segment's in-point."""
    filters = []
    if abs(segment.speed - 1.0) > 1e-6:
        filters.append(f'setpts=PTS/{segment.speed:.4f}')
    # start_time=0 pins the first frame to t=0 (the first decoded frame can sit up to one frame after the seek
    # point); tpad clones the last frame if the source runs out, -frames:v then cuts to the planned count.
    filters.append(f'fps={_rate(fps)}:start_time=0:round=near')
    filters.append(f'scale={width}:{height}:force_original_aspect_ratio=increase')
    filters.append(f'crop={width}:{height}')
    filters.append('setsar=1')
    filters.append('tpad=stop_mode=clone:stop=-1')
    filters.append('format=yuv420p')
    # Read a little past the out-point so the last planned frame is a real one rather than a clone.
    read_seconds = segment.duration + 2.0 * segment.speed / float(fps)
    return ['ffmpeg', '-v', 'error', '-nostdin', '-y',
            '-ss', _seconds(segment.start), '-t', _seconds(read_seconds),
            '-i', segment.media, '-an', '-sn', '-dn',
            '-vf', ','.join(filters),
            '-frames:v', str(segment.frames),
            *encoder_args,
            '-video_track_timescale', '90000',
            output]


def black_command(frames: int, fps: Fraction, width: int, height: int, encoder_args: Sequence[str],
                  output: str) -> List[str]:
    return ['ffmpeg', '-v', 'error', '-nostdin', '-y',
            '-f', 'lavfi', '-i', f'color=c=black:s={width}x{height}:r={_rate(fps)}',
            '-vf', 'format=yuv420p', '-frames:v', str(frames),
            *encoder_args, '-video_track_timescale', '90000', output]


def _run(command: Sequence[str], what: str, timeout: int = FFMPEG_TIMEOUT) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(command, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RenderError(f'{what}: ffmpeg timed out after {timeout}s') from exc
    except OSError as exc:
        raise RenderError(f'{what}: could not start ffmpeg ({exc})') from exc
    if result.returncode != 0:
        detail = (result.stderr or b'').decode(errors='ignore').strip().splitlines()
        raise RenderError(f'{what}: {" | ".join(detail[-3:]) or "ffmpeg failed"}')
    return result


# --------------------------------------------------------------------------------------------------------------
# Clip audio for the preview
# --------------------------------------------------------------------------------------------------------------

def wav_header(data_bytes: int, sample_rate: int = AUDIO_RATE, channels: int = AUDIO_CHANNELS) -> bytes:
    block_align = channels * 2
    return b''.join([
        b'RIFF', struct.pack('<I', 36 + data_bytes), b'WAVE',
        b'fmt ', struct.pack('<IHHIIHH', 16, 1, channels, sample_rate, sample_rate * block_align, block_align, 16),
        b'data', struct.pack('<I', data_bytes),
    ])


def extract_pcm(media: str, start: Optional[float] = None, duration: Optional[float] = None,
                speed: float = 1.0) -> bytes:
    """Decode a range of ``media`` to 48 kHz stereo s16le PCM (the reference signal for preview and merge).

    Without ``start``/``duration`` the whole file is decoded. With a duration the result is padded/cut to exactly
    ``duration / speed`` seconds so the browser buffer has the same length the merge will give the segment.
    """
    samples = None
    if duration is not None:
        samples = round(output_seconds(duration, speed) * AUDIO_RATE)
        if samples <= 0:
            return b''
    result = _run(audio_command(media, start, duration, speed, samples), 'Audio extraction', timeout=120)
    pcm = result.stdout
    return pcm[:len(pcm) - len(pcm) % BYTES_PER_FRAME]


def pcm_to_wav(pcm: bytes) -> bytes:
    return wav_header(len(pcm)) + pcm


def waveform_peaks(pcm: bytes, width: int) -> List[float]:
    """RMS envelope of s16le stereo PCM in ``width`` equal slices covering the *whole* signal, normalised 0..1."""
    width = max(1, int(width))
    total = len(pcm) // BYTES_PER_FRAME
    if total <= 0:
        return [0.0] * width
    try:
        import numpy as np
    except ImportError:  # pragma: no cover - numpy ships with the documented requirements
        np = None
    if np is not None:
        data = np.frombuffer(pcm, dtype='<i2', count=total * AUDIO_CHANNELS).astype(np.float64)
        mono = data.reshape(-1, AUDIO_CHANNELS).mean(axis=1)
        edges = (np.arange(width + 1) * total) // width
        power = np.concatenate(([0.0], np.cumsum(mono * mono)))
        counts = np.maximum(1, edges[1:] - edges[:-1])
        rms = np.sqrt((power[edges[1:]] - power[edges[:-1]]) / counts)
        values = rms.tolist()
    else:
        samples = struct.unpack(f'<{total * AUDIO_CHANNELS}h', pcm[:total * BYTES_PER_FRAME])
        values = []
        for index in range(width):
            begin = (index * total) // width
            end = max(begin + 1, ((index + 1) * total) // width)
            chunk = [(samples[2 * i] + samples[2 * i + 1]) / 2 for i in range(begin, min(end, total))]
            values.append(math.sqrt(sum(v * v for v in chunk) / len(chunk)) if chunk else 0.0)
    # Normalise to the clip's own peak but never amplify near-silence above 5 % of full scale.
    reference = max(max(values), 32768.0 * 0.05)
    return [min(1.0, value / reference) for value in values]


# --------------------------------------------------------------------------------------------------------------
# Timeline rendering
# --------------------------------------------------------------------------------------------------------------

EncoderChoice = Tuple[Optional[str], Sequence[str]]


def _encoder_args(encoder: EncoderChoice) -> List[str]:
    name, args = encoder
    if name:
        return ['-c:v', name, *list(args or [])]
    return list(CPU_VIDEO_ARGS)


def render_timeline(
    segments: Sequence[Segment],
    output_path: str,
    work_root: str,
    *,
    resolution: Optional[Tuple[int, int]] = None,
    fps: Optional[Fraction] = None,
    gpu_encoder: EncoderChoice = (None, ()),
    gpu_slots: Optional[object] = None,
    on_gpu_failure: Optional[Callable[[], None]] = None,
    max_workers: int = 4,
    is_cancelled: Callable[[], bool] = lambda: False,
) -> Iterator[Dict[str, object]]:
    """Render ``segments`` back to back into ``output_path``. Yields progress dictionaries.

    Every segment is encoded exactly once from its media (video straight to the final codec, audio to PCM);
    the pieces are then joined without re-encoding the picture and the audio is encoded once at the end.
    """
    if not segments:
        raise RenderError('Nothing to merge: the timeline has no enabled clips.')
    total = len(segments)
    yield {'stage': 'analysing', 'message': 'Analysing clips...', 'progress': 0, 'total': total}

    probe_cache: Dict[str, MediaInfo] = {}
    for segment in segments:
        if segment.media not in probe_cache:
            if not os.path.isfile(segment.media):
                raise RenderError(f'Clip "{segment.title}" is missing its media file.')
            probe_cache[segment.media] = probe_media(segment.media)
        segment.info = probe_cache[segment.media]
        segment.speed = clamp_speed(segment.speed)
        if segment.duration < MIN_SEGMENT_SECONDS:
            raise RenderError(f'Clip "{segment.title}" is trimmed to nothing ({segment.duration * 1000:.0f} ms).')

    infos = [segment.info for segment in segments]
    target_fps = fps or pick_fps(infos)
    width, height = resolution or pick_resolution(infos)
    total_seconds = plan_timeline(segments, target_fps)
    total_frames = sum(segment.frames for segment in segments)
    total_samples = sum(segment.samples for segment in segments)

    os.makedirs(work_root, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix='merge_', dir=work_root)
    try:
        def video_path(index: int) -> str:
            return os.path.join(work_dir, f'v{index:05d}.mp4')

        def audio_path(index: int) -> str:
            return os.path.join(work_dir, f'a{index:05d}.pcm')

        def render_audio(index: int) -> None:
            segment = segments[index]
            target = audio_path(index)
            if segment.samples <= 0:
                open(target, 'wb').close()
                return
            if segment.info.has_audio:
                _run(audio_command(segment.media, segment.start, segment.duration, segment.speed,
                                   segment.samples, target), f'Audio of "{segment.title}"')
            else:
                _run(silence_command(segment.samples, target), f'Silence for "{segment.title}"')
            actual = os.path.getsize(target)
            expected = segment.samples * BYTES_PER_FRAME
            if actual != expected:
                raise RenderError(f'Audio of "{segment.title}" has {actual // BYTES_PER_FRAME} samples, '
                                  f'expected {segment.samples}.')

        def render_video(index: int, encoder: EncoderChoice) -> None:
            segment = segments[index]
            if segment.frames <= 0:
                return
            args = _encoder_args(encoder)
            if segment.info.has_video:
                command = video_command(segment, target_fps, width, height, args, video_path(index))
            else:
                command = black_command(segment.frames, target_fps, width, height, args, video_path(index))
            use_slot = bool(encoder[0]) and gpu_slots is not None
            if use_slot:
                gpu_slots.acquire()
            try:
                _run(command, f'Video of "{segment.title}"')
            finally:
                if use_slot:
                    gpu_slots.release()

        def run_stage(job: Callable[[int], None], workers: int, label: str, done_offset: int, units: int):
            completed = 0
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                futures = {pool.submit(job, index): index for index in range(total)}
                try:
                    for future in as_completed(futures):
                        if is_cancelled():
                            raise RenderError('Merge cancelled.')
                        future.result()
                        completed += 1
                        yield {'stage': 'processing_videos',
                               'message': f'{label} {completed}/{total}: {segments[futures[future]].title}',
                               'progress': done_offset + completed, 'total': units}
                finally:
                    for future in futures:
                        future.cancel()

        units = total * 2 + 1
        yield from run_stage(render_audio, max_workers, 'Audio', 0, units)

        encoder: EncoderChoice = gpu_encoder if gpu_encoder and gpu_encoder[0] else (None, ())
        try:
            yield from run_stage(lambda index: render_video(index, encoder), max_workers, 'Video', total, units)
        except RenderError as exc:
            if not encoder[0] or is_cancelled():
                raise
            # All pieces must come from one encoder to be joined without re-encoding: redo them on the CPU.
            print(f'[Merge] GPU encoder {encoder[0]} failed ({exc}); re-encoding all segments with libx264')
            if on_gpu_failure:
                on_gpu_failure()
            for index in range(total):
                try:
                    os.remove(video_path(index))
                except OSError:
                    pass
            encoder = (None, ())
            yield from run_stage(lambda index: render_video(index, encoder), max_workers, 'Video (CPU)', total, units)

        yield {'stage': 'merging', 'message': 'Joining clips...', 'progress': total * 2, 'total': units}

        audio_all = os.path.join(work_dir, 'audio.pcm')
        with open(audio_all, 'wb') as joined:
            for index in range(total):
                with open(audio_path(index), 'rb') as piece:
                    shutil.copyfileobj(piece, joined)
        if os.path.getsize(audio_all) != total_samples * BYTES_PER_FRAME:
            raise RenderError('Joined audio has an unexpected length.')

        video_list = os.path.join(work_dir, 'video.txt')
        with open(video_list, 'w', encoding='utf-8') as handle:
            for index, segment in enumerate(segments):
                if segment.frames > 0:
                    escaped = video_path(index).replace("'", "'\\''")
                    handle.write(f"file '{escaped}'\n")

        temp_output = os.path.join(work_dir, 'merged.mp4')
        command = ['ffmpeg', '-v', 'error', '-nostdin', '-y']
        if total_frames > 0:
            command += ['-f', 'concat', '-safe', '0', '-i', video_list]
        command += ['-f', 's16le', '-ar', str(AUDIO_RATE), '-ac', str(AUDIO_CHANNELS), '-i', audio_all]
        if total_frames > 0:
            command += ['-map', '0:v:0', '-map', '1:a:0', '-c:v', 'copy']
        command += ['-c:a', 'aac', '-b:a', '192k', '-movflags', '+faststart', temp_output]
        _run(command, 'Joining clips', timeout=max(FFMPEG_TIMEOUT, total * 30))

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        os.replace(temp_output, output_path)
        yield {
            'stage': 'complete', 'message': 'Merge completed', 'progress': units, 'total': units,
            'duration_ms': round(float(total_seconds) * 1000), 'fps': float(target_fps),
            'width': width, 'height': height, 'frames': total_frames, 'samples': total_samples,
            'encoder': encoder[0] or 'libx264',
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

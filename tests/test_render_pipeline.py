"""Timing tests for render_pipeline: are cuts where the timeline says they are?

Run with:  python -m unittest discover -s tests -v
Needs ffmpeg/ffprobe on PATH and numpy (both are requirements of the app); the ffmpeg tests are skipped otherwise.
"""
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave
from fractions import Fraction

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import render_pipeline as rp  # noqa: E402

try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None

HAVE_FFMPEG = shutil.which('ffmpeg') is not None and shutil.which('ffprobe') is not None
SR = rp.AUDIO_RATE


class PlanTimelineTest(unittest.TestCase):
    """plan_timeline is pure arithmetic: rounding must never accumulate."""

    def test_budgets_add_up_exactly(self):
        rng = random.Random(1234)
        for fps in (Fraction(25), Fraction(30000, 1001), Fraction(24000, 1001), Fraction(60)):
            segments = [
                rp.Segment(media='x', start=0.0, duration=rng.randint(40, 4000) / 1000.0,
                           speed=rng.choice([1.0, 1.0, 0.5, 0.75, 0.85]))
                for _ in range(500)
            ]
            total = rp.plan_timeline(segments, fps)
            self.assertEqual(sum(s.samples for s in segments), round(total * SR))
            self.assertEqual(sum(s.frames for s in segments), round(total * fps))
            self.assertTrue(all(s.samples > 0 and s.frames >= 0 for s in segments))

    def test_every_cut_is_within_half_a_frame_and_half_a_sample(self):
        fps = Fraction(30000, 1001)
        segments = [rp.Segment(media='x', start=0.0, duration=0.333, speed=1.0) for _ in range(1000)]
        rp.plan_timeline(segments, fps)
        position = Fraction(0)
        samples = frames = 0
        for segment in segments:
            position += rp.output_seconds(segment.duration, segment.speed)
            samples += segment.samples
            frames += segment.frames
            self.assertLessEqual(abs(Fraction(samples, SR) - position), Fraction(1, 2 * SR))
            self.assertLessEqual(abs(frames / fps - position), 1 / (2 * fps))

    def test_speed_is_clamped(self):
        self.assertEqual(rp.clamp_speed(None), 1.0)
        self.assertEqual(rp.clamp_speed('abc'), 1.0)
        self.assertEqual(rp.clamp_speed(0), 1.0)
        self.assertEqual(rp.clamp_speed(0.1), rp.MIN_SPEED)
        self.assertEqual(rp.clamp_speed(9), rp.MAX_SPEED)
        self.assertEqual(rp.clamp_speed('0.75'), 0.75)

    def test_most_common_format_wins(self):
        def info(width, height, fps):
            return rp.MediaInfo(True, True, width, height, Fraction(fps), 10.0, 10.0)

        infos = [info(1280, 720, 25), info(1920, 1080, 30), info(1280, 720, 25)]
        self.assertEqual(rp.pick_resolution(infos), (1280, 720))
        self.assertEqual(rp.pick_fps(infos), Fraction(25))
        self.assertEqual(rp.pick_resolution([info(1281, 721, 25)]), (1280, 720))
        self.assertEqual(rp.pick_fps([]), rp.DEFAULT_FPS)


@unittest.skipUnless(np is not None, 'numpy missing')
class WaveformTest(unittest.TestCase):
    def test_peaks_cover_the_whole_signal(self):
        total = SR  # one second, burst in the third quarter
        mono = np.zeros(total)
        mono[total // 2: 3 * total // 4] = 0.5 * np.sin(2 * math.pi * 440 * np.arange(total // 4) / SR)
        pcm = (np.repeat(mono, 2) * 32767).astype('<i2').tobytes()
        peaks = rp.waveform_peaks(pcm, 400)
        self.assertEqual(len(peaks), 400)
        self.assertLess(max(peaks[:198]), 0.01)
        self.assertGreater(min(peaks[202:298]), 0.9)
        self.assertLess(max(peaks[302:]), 0.01)

    def test_empty_audio(self):
        self.assertEqual(rp.waveform_peaks(b'', 10), [0.0] * 10)


def _decode_mono(path):
    raw = subprocess.run(
        ['ffmpeg', '-v', 'error', '-i', path, '-map', '0:a:0', '-ac', '1', '-ar', str(SR), '-f', 'f32le', '-'],
        capture_output=True, check=True).stdout
    return np.frombuffer(raw, dtype='<f4').astype(np.float64)


def _frame_times(path):
    out = subprocess.run(
        ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'frame=pts_time', '-of', 'csv=p=0', path],
        capture_output=True, text=True, check=True).stdout
    return [float(value.strip(',')) for value in out.split() if value.strip(',') not in ('', 'N/A')]


def _best_lag(haystack, needle):
    size = 1 << (len(haystack) + len(needle) - 2).bit_length()
    corr = np.fft.irfft(np.fft.rfft(haystack, size) * np.conj(np.fft.rfft(needle, size)), size)
    lags = np.arange(0, len(haystack) - len(needle) + 1)
    energy = np.concatenate(([0.0], np.cumsum(haystack * haystack)))
    window = np.sqrt(np.maximum(energy[lags + len(needle)] - energy[lags], 1e-12))
    return int(np.argmax(corr[lags] / window))


@unittest.skipUnless(HAVE_FFMPEG and np is not None, 'ffmpeg/ffprobe or numpy missing')
class RenderAccuracyTest(unittest.TestCase):
    """Render a timeline of short cuts from a synthetic source and measure where every cut landed."""

    @classmethod
    def setUpClass(cls):
        cls.work = tempfile.mkdtemp(prefix='cinephrase_test_')
        seconds, fps = 16, 25
        rng = np.random.default_rng(5)
        # Noise bursts: no periodicity, so cross-correlation has a single sharp peak.
        signal = np.zeros(seconds * SR)
        cls.words = []
        cursor = 0.5
        while cursor < seconds - 1.5:
            length = float(rng.integers(150, 400)) / 1000.0
            a, b = int(round(cursor * SR)), int(round((cursor + length) * SR))
            signal[a:b] = rng.uniform(-0.5, 0.5, b - a)
            cls.words.append((a / SR, b / SR))
            cursor += length + float(rng.integers(60, 200)) / 1000.0
        wav_path = os.path.join(cls.work, 'source.wav')
        with wave.open(wav_path, 'wb') as handle:
            handle.setnchannels(1)
            handle.setsampwidth(2)
            handle.setframerate(SR)
            handle.writeframes((signal * 32767).astype('<i2').tobytes())
        cls.source = os.path.join(cls.work, 'source.mp4')
        subprocess.run([
            'ffmpeg', '-v', 'error', '-y', '-f', 'lavfi', '-i', f'testsrc2=size=320x180:rate={fps}:duration={seconds}',
            '-i', wav_path, '-c:v', 'libx264', '-preset', 'ultrafast', '-pix_fmt', 'yuv420p', '-g', '25',
            '-c:a', 'aac', '-b:a', '192k', '-shortest', cls.source,
        ], check=True)
        cls.reference = _decode_mono(cls.source)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.work, ignore_errors=True)

    def _segments(self, speed=1.0, lead=0.03, tail=0.03, count=12):
        return [rp.Segment(media=self.source, start=on - lead, duration=(off - on) + lead + tail, speed=speed,
                           title=f'word {index}')
                for index, (on, off) in enumerate(self.words[:count])]

    def _render(self, segments, **options):
        output = os.path.join(self.work, f'merged_{len(os.listdir(self.work))}.mp4')
        events = list(rp.render_timeline(segments, output, os.path.join(self.work, 'tmp'), max_workers=2, **options))
        return output, events[-1]

    def test_extract_pcm_has_exact_length(self):
        for duration, speed in ((0.5, 1.0), (0.333, 1.0), (0.777, 0.75), (1.0, 0.5)):
            pcm = rp.extract_pcm(self.source, 1.0, duration, speed)
            self.assertEqual(len(pcm), round(rp.output_seconds(duration, speed) * SR) * rp.BYTES_PER_FRAME)

    def test_extract_pcm_starts_on_the_requested_sample(self):
        on, off = self.words[3]
        pcm = rp.extract_pcm(self.source, on - 0.02, (off - on) + 0.04)
        mono = np.frombuffer(pcm, dtype='<i2').reshape(-1, 2).mean(axis=1) / 32768.0
        begin = int(round((on - 0.02) * SR))
        lag = _best_lag(self.reference[begin - 2400: begin + len(mono) + 2400], mono) - 2400
        self.assertLessEqual(abs(lag), 2, f'audio starts {lag} samples off')

    def test_cuts_land_within_a_millisecond_and_never_drift(self):
        segments = self._segments()
        output, summary = self._render(segments)
        merged = _decode_mono(output)
        cursor = 0.0
        for segment in segments:
            a = int(round(segment.start * SR))
            needle = self.reference[a: a + int(round(segment.duration * SR))]
            begin = int(round(cursor * SR))
            lo = max(0, begin - 4800)
            lag = lo + _best_lag(merged[lo: begin + len(needle) + 4800], needle) - begin
            self.assertLessEqual(abs(lag) / SR * 1000, 1.0, f'{segment.title} landed {lag / SR * 1000:+.2f} ms off')
            cursor += segment.duration
        self.assertEqual(summary['samples'], round(cursor * SR))
        self.assertEqual(summary['samples'], sum(s.samples for s in segments))

    def test_picture_is_constant_rate_with_the_planned_frame_count(self):
        segments = self._segments(count=8)
        output, summary = self._render(segments, resolution=(320, 180))
        times = _frame_times(output)
        fps = summary['fps']
        self.assertEqual(len(times), summary['frames'])
        self.assertEqual(len(times), sum(s.frames for s in segments))
        self.assertLess(max(abs(t - k / fps) for k, t in enumerate(times)), 0.0005)
        total = sum(s.duration for s in segments)
        self.assertLessEqual(abs(len(times) / fps - total), 0.5 / fps + 1e-9)

    def test_speed_change_keeps_following_cuts_exact(self):
        segments = self._segments(count=6)
        segments[0].speed = 0.5
        segments[2].speed = 0.75
        output, summary = self._render(segments)
        merged = _decode_mono(output)
        cursor = 0.0
        for segment in segments:
            if segment.speed == 1.0:
                a = int(round(segment.start * SR))
                needle = self.reference[a: a + int(round(segment.duration * SR))]
                begin = int(round(cursor * SR))
                lo = max(0, begin - 4800)
                lag = lo + _best_lag(merged[lo: begin + len(needle) + 4800], needle) - begin
                self.assertLessEqual(abs(lag) / SR * 1000, 1.0, f'{segment.title} landed {lag / SR * 1000:+.2f} ms off')
            cursor += segment.duration / segment.speed
        self.assertEqual(summary['duration_ms'], round(cursor * 1000))

    def test_rejects_empty_and_overtrimmed_timelines(self):
        with self.assertRaises(rp.RenderError):
            list(rp.render_timeline([], os.path.join(self.work, 'none.mp4'), self.work))
        too_short = [rp.Segment(media=self.source, start=1.0, duration=0.001, title='tiny')]
        with self.assertRaises(rp.RenderError):
            list(rp.render_timeline(too_short, os.path.join(self.work, 'tiny.mp4'), self.work))
        missing = [rp.Segment(media=os.path.join(self.work, 'nope.mp4'), start=0.0, duration=1.0, title='gone')]
        with self.assertRaises(rp.RenderError):
            list(rp.render_timeline(missing, os.path.join(self.work, 'gone.mp4'), self.work))

    def test_falls_back_to_cpu_when_the_gpu_encoder_fails(self):
        segments = self._segments(count=3)
        output, summary = self._render(segments, gpu_encoder=('no_such_gpu_encoder', ['-preset', 'p5']))
        self.assertEqual(summary['encoder'], 'libx264')
        self.assertEqual(len(_frame_times(output)), sum(s.frames for s in segments))

    def _variant(self, name, *arguments):
        path = os.path.join(self.work, name)
        if not os.path.exists(path):
            subprocess.run(['ffmpeg', '-v', 'error', '-y', *arguments, path], check=True)
        return path

    def test_rotated_video_keeps_its_orientation(self):
        rotated = self._variant('rotated.mp4', '-display_rotation', '90', '-i', self.source, '-c', 'copy')
        info = rp.probe_media(rotated)
        self.assertEqual((info.width, info.height), (180, 320))           # upright size, not the coded 320x180
        output, summary = self._render([rp.Segment(media=rotated, start=1.0, duration=1.0, title='portrait')])
        self.assertEqual((summary['width'], summary['height']), (180, 320))
        self.assertEqual(rp.probe_media(output).width, 180)

    def test_source_without_audio_gets_silence(self):
        silent = self._variant('noaudio.mp4', '-i', self.source, '-an', '-c', 'copy')
        segments = [rp.Segment(media=silent, start=1.0, duration=0.5, title='mute'),
                    rp.Segment(media=self.source, start=self.words[0][0], duration=0.4, title='sound')]
        output, summary = self._render(segments)
        merged = _decode_mono(output)
        self.assertLess(np.abs(merged[: int(0.45 * SR)]).max(), 0.01)
        self.assertGreater(np.abs(merged[int(0.5 * SR): int(0.9 * SR)]).max(), 0.1)
        self.assertEqual(summary['samples'], round(0.9 * SR))

    def test_preview_audio_of_a_silent_video_is_silence(self):
        silent = self._variant('noaudio.mp4', '-i', self.source, '-an', '-c', 'copy')
        pcm = rp.extract_pcm(silent, 1.0, 0.5)
        self.assertEqual(len(pcm), round(0.5 * SR) * rp.BYTES_PER_FRAME)
        self.assertEqual(set(pcm), {0})
        self.assertGreater(len(rp.extract_pcm(silent)), SR * rp.BYTES_PER_FRAME)       # whole file
        with self.assertRaises(rp.RenderError):
            rp.extract_pcm(os.path.join(self.work, 'missing.mp4'), 0.0, 1.0)

    def test_source_without_picture_gets_black_frames(self):
        audio_only = self._variant('audio.m4a', '-i', self.source, '-vn', '-c', 'copy')
        segments = [rp.Segment(media=audio_only, start=self.words[1][0], duration=0.6, title='voice only'),
                    rp.Segment(media=self.source, start=2.0, duration=0.4, title='picture')]
        output, summary = self._render(segments)
        self.assertEqual(len(_frame_times(output)), summary['frames'])
        self.assertEqual(summary['frames'], 25)
        self.assertEqual((summary['width'], summary['height']), (320, 180))

    def test_work_directory_is_removed(self):
        work_root = os.path.join(self.work, 'tmp_cleanup')
        list(rp.render_timeline(self._segments(count=2), os.path.join(self.work, 'clean.mp4'), work_root))
        self.assertEqual(os.listdir(work_root), [])


if __name__ == '__main__':
    unittest.main()

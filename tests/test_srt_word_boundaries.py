"""Tests for the subtitle -> word-timing converter.

Run with:  python -m unittest discover -s tests -v
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import srt_word_boundaries as swb  # noqa: E402


class SubtitleTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix='cinephrase_srt_'))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def write(self, name, text, encoding='utf-8'):
        path = self.dir / name
        path.write_bytes(text.encode(encoding))
        return path

    def asr(self, words, name='asr.json'):
        path = self.dir / name
        path.write_text(json.dumps([{'words': [{'word': w, 'start': a, 'end': b} for w, a, b in words]}]), encoding='utf-8')
        return path


class ParsingTest(SubtitleTestCase):
    def test_vtt_timestamps_without_hours(self):
        path = self.write('a.vtt', 'WEBVTT\n\n00:01.000 --> 00:04.000\nHello there\n')
        cues = swb.parse_subtitle_file(path)
        self.assertEqual([(cue.start, cue.end, cue.lines) for cue in cues], [(1.0, 4.0, ['Hello there'])])

    def test_empty_cue_does_not_swallow_the_next_index(self):
        path = self.write('a.srt', '1\n00:00:01,000 --> 00:00:02,000\n\n2\n00:00:03,000 --> 00:00:04,000\nHello there\n')
        cues = swb.parse_subtitle_file(path)
        self.assertEqual([cue.lines for cue in cues], [[], ['Hello there']])

    def test_windows_1252_and_utf16_files_are_read(self):
        text = '1\n00:00:01,000 --> 00:00:02,000\nGrüße aus München\n'
        for encoding in ('cp1252', 'utf-16', 'utf-8-sig'):
            cues = swb.parse_subtitle_file(self.write(f'{encoding}.srt', text, encoding))
            self.assertEqual(cues[0].lines, ['Grüße aus München'], encoding)

    def test_markup_is_not_turned_into_words(self):
        cue = swb.SubtitleCue(0.0, 2.0, ['<b>Hello</b> <font color="#ffff00">world</font>', '{\\an8}<u>Top</u>',
                                         '<v Roger Bingham>We are in New York'])
        self.assertEqual([word.text for word in swb.extract_srt_words(cue)],
                         ['Hello', 'world', 'Top', 'We', 'are', 'in', 'New', 'York'])

    def test_words_of_any_script_and_typographic_apostrophes(self):
        def words(line):
            return [word.text for word in swb.extract_srt_words(swb.SubtitleCue(0.0, 1.0, [line]))]

        self.assertEqual(words('I don’t know'), ['I', 'don’t', 'know'])
        self.assertEqual(words('Część świata'), ['Część', 'świata'])
        self.assertEqual(words('Привет мир'), ['Привет', 'мир'])
        self.assertEqual(words("rock'n'roll, it's"), ["rock'n'roll", "it's"])


class InlineTimingTest(SubtitleTestCase):
    def test_youtube_style_inline_timestamps(self):
        path = self.write('a.vtt', 'WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nHello<00:00:00.500><c> big</c><00:00:01.200><c> world</c>\n')
        segments, stats = swb.convert_subtitles(path)
        self.assertEqual([(w['word'], w['start']) for w in segments[0]['words']], [('Hello', 0.0), ('big', 0.5), ('world', 1.2)])
        self.assertEqual(stats['mode'], 'inline')

    def test_inline_timestamps_without_c_wrapper(self):
        path = self.write('a.vtt', 'WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nHello <00:00:02.000>big <00:00:03.500>world\n')
        segments, _ = swb.convert_subtitles(path)
        self.assertEqual([(w['word'], w['start'], w['end']) for w in segments[0]['words']],
                         [('Hello', 1.0, 2.0), ('big', 2.0, 3.5), ('world', 3.5, 4.0)])

    def test_plain_subtitles_need_asr_or_interpolation(self):
        path = self.write('a.srt', '1\n00:00:10,000 --> 00:00:12,000\na considerably longer word\n')
        with self.assertRaises(ValueError):
            swb.convert_subtitles(path)
        segments, stats = swb.convert_subtitles(path, interpolate=True)
        words = segments[0]['words']
        self.assertEqual(stats['mode'], 'interpolated')
        self.assertEqual([w['word'] for w in words], ['a', 'considerably', 'longer', 'word'])
        self.assertEqual((words[0]['start'], words[-1]['end']), (10.0, 12.0))
        durations = [w['end'] - w['start'] for w in words]
        self.assertGreater(durations[1], durations[0] * 5)            # long words get more time
        for before, after in zip(words, words[1:]):
            self.assertAlmostEqual(before['end'], after['start'], places=5)


class AlignmentTest(SubtitleTestCase):
    def test_one_unmatched_cue_does_not_derail_the_rest(self):
        lines = ['1\n00:00:00,000 --> 00:00:02,000\n[Musik]\n']
        asr = []
        for index in range(30):
            start = 5.0 + index * 3
            lines.append(f'{index + 2}\n00:00:{int(start):02d},000 --> 00:00:{int(start) + 2:02d},000\nword{index} again{index}\n'
                         if start < 60 else '')
            asr += [(f'word{index}', start + 0.1, start + 0.6), (f'again{index}', start + 0.8, start + 1.4)]
        path = self.write('a.srt', '\n'.join(line for line in lines if line))
        segments, stats = swb.convert_subtitles(path, asr_path=self.asr(asr))
        spoken = [segment for segment in segments if segment['content'] != 'Musik']
        self.assertEqual(stats['matched_words'], sum(len(segment['words']) for segment in spoken))
        self.assertAlmostEqual(spoken[5]['words'][0]['start'], 5.0 + 5 * 3 + 0.1, places=3)

    def test_a_misheard_common_word_does_not_shift_the_others(self):
        path = self.write('a.srt', '1\n00:00:10,000 --> 00:00:13,000\nthe cat sat on the mat\n')
        asr = self.asr([('a', 10.0, 10.2), ('cat', 10.3, 10.6), ('sat', 10.7, 11.0), ('on', 11.0, 11.2),
                        ('the', 11.2, 11.4), ('mat', 11.5, 12.0)])
        segments, _ = swb.convert_subtitles(path, asr_path=asr)
        times = {(w['word'], round(w['start'], 2)) for w in segments[0]['words']}
        self.assertTrue({('cat', 10.3), ('sat', 10.7), ('on', 11.0), ('mat', 11.5)} <= times, times)
        self.assertEqual(segments[0]['words'][4]['start'], 11.2)          # the second "the"

    def test_matched_words_keep_their_measured_times_outside_the_cue(self):
        path = self.write('a.srt', '1\n00:00:10,000 --> 00:00:12,000\nhello world again\n')
        asr = self.asr([('hello', 9.55, 10.05), ('world', 10.6, 11.0), ('again', 11.8, 12.4)])
        segments, _ = swb.convert_subtitles(path, asr_path=asr)
        self.assertEqual([(w['start'], w['end']) for w in segments[0]['words']], [(9.55, 10.05), (10.6, 11.0), (11.8, 12.4)])

    def test_output_is_written_completely_or_not_at_all(self):
        path = self.write('a.srt', '1\n00:00:10,000 --> 00:00:12,000\nhello world\n')
        output = self.dir / 'out.json'
        swb.convert_subtitles(path, output, interpolate=True)
        self.assertEqual(len(json.loads(output.read_text(encoding='utf-8'))), 1)
        self.assertFalse(list(self.dir.glob('*.tmp')))

    def test_cli_refuses_to_overwrite_the_asr_input(self):
        subtitle = self.write('clip.srt', '1\n00:00:10,000 --> 00:00:12,000\nhello world\n')
        asr = self.asr([('hello', 10.0, 10.5), ('world', 10.6, 11.0)], name='clip.json')
        before = asr.read_text(encoding='utf-8')
        with self.assertRaises(SystemExit):
            swb.main([str(subtitle), '--asr-json', str(asr)])
        self.assertEqual(asr.read_text(encoding='utf-8'), before)


if __name__ == '__main__':
    unittest.main()

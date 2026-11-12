"""Server tests: project store safety, clip reference resolution and the preview/merge endpoints.

Run with:  python -m unittest discover -s tests -v
Skipped when the server's dependencies (flask, videogrep) or ffmpeg are not installed.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import server
except Exception as exc:  # pragma: no cover - depends on the local environment
    server = None
    IMPORT_ERROR = exc

HAVE_FFMPEG = shutil.which('ffmpeg') is not None and shutil.which('ffprobe') is not None


@unittest.skipUnless(server is not None, 'server dependencies missing')
class ServerTestCase(unittest.TestCase):
    """Points the server at throw-away data/temp/library folders."""

    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix='cinephrase_srv_')
        cls.saved = {name: getattr(server, name) for name in
                     ('DATA_DIR', 'TEMP_DIR', 'PROJECTS_FILE', 'COLLECTIONS_FILE', 'LIBRARY_ROOTS')}
        server.DATA_DIR = os.path.join(cls.root, 'data')
        server.TEMP_DIR = os.path.join(cls.root, 'temp')
        server.PROJECTS_FILE = os.path.join(server.DATA_DIR, 'projects.json')
        server.COLLECTIONS_FILE = os.path.join(server.DATA_DIR, 'collections.json')
        cls.library = os.path.join(cls.root, 'Library')
        os.makedirs(cls.library)
        server.LIBRARY_ROOTS = [cls.library]
        server.ensure_project_store()
        cls.client = server.app.test_client()

    @classmethod
    def tearDownClass(cls):
        for name, value in cls.saved.items():
            setattr(server, name, value)
        shutil.rmtree(cls.root, ignore_errors=True)


class ProjectStoreTest(ServerTestCase):
    def setUp(self):
        for name in os.listdir(server.DATA_DIR):
            os.remove(os.path.join(server.DATA_DIR, name))

    def _create(self, name='Demo'):
        response = self.client.post('/api/projects', json={'name': name})
        self.assertEqual(response.status_code, 201)
        return response.get_json()

    def test_create_update_delete_roundtrip(self):
        project = self._create()
        timeline = [{'id': 't1', 'file': 'temp/a.mp4', 'startTrim': 120, 'endTrim': 80}]
        response = self.client.put(f"/api/projects/{project['id']}", json={'name': ' Renamed ', 'data': {'timeline': timeline}})
        self.assertEqual(response.status_code, 200)
        loaded = self.client.get(f"/api/projects/{project['id']}").get_json()
        self.assertEqual(loaded['name'], 'Renamed')
        self.assertEqual(loaded['data']['timeline'], timeline)
        self.assertEqual(self.client.delete(f"/api/projects/{project['id']}").status_code, 200)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}").status_code, 404)

    def test_stale_copies_cannot_overwrite_newer_saves(self):
        project = self._create()
        url = f"/api/projects/{project['id']}"
        self.assertEqual(project['revision'], 1)
        first = self.client.put(url, json={'baseRevision': 1, 'data': {'timeline': ['a', 'b']}})
        self.assertEqual((first.status_code, first.get_json()['revision']), (200, 2))
        stale = self.client.put(url, json={'baseRevision': 1, 'data': {'timeline': []}})      # the other tab
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.get_json()['project']['data']['timeline'], ['a', 'b'])
        self.assertEqual(self.client.get(url).get_json()['data']['timeline'], ['a', 'b'])
        forced = self.client.put(url, json={'data': {'timeline': ['c']}})                    # no base: last writer wins
        self.assertEqual((forced.status_code, forced.get_json()['revision']), (200, 3))

    def test_damaged_store_is_not_overwritten_with_an_empty_one(self):
        first = self._create('Keep me')
        self._create('Second')                      # the second write leaves the first state in .bak
        with open(server.PROJECTS_FILE, 'w', encoding='utf-8') as handle:
            handle.write('{"projects": [{"id": "trunc')          # interrupted write
        listed = self.client.get('/api/projects').get_json()
        self.assertEqual([p['name'] for p in listed], ['Keep me'])
        self.assertTrue(any('.damaged-' in name for name in os.listdir(server.DATA_DIR)))
        # saving now must keep the recovered project
        self.client.put(f"/api/projects/{first['id']}", json={'data': {'timeline': []}})
        self.assertEqual([p['name'] for p in self.client.get('/api/projects').get_json()], ['Keep me'])

    def test_no_temp_files_left_behind(self):
        self._create()
        self.assertFalse([name for name in os.listdir(server.DATA_DIR) if name.endswith('.tmp')])

    def test_malformed_bodies_do_not_crash(self):
        project = self._create()
        self.assertEqual(self.client.put(f"/api/projects/{project['id']}", json=[1, 2]).status_code, 200)
        self.assertEqual(self.client.post('/api/projects', data='not json', content_type='application/json').status_code, 201)
        self.assertEqual(self.client.post('/api/collections', json={'name': 123, 'files': ['a']}).status_code, 400)
        self.assertEqual(self.client.post('/api/collections', json={'name': 'x', 'files': [1]}).status_code, 400)

    def test_concurrent_updates_do_not_drop_projects(self):
        from concurrent.futures import ThreadPoolExecutor
        target = self._create('Target')

        def create(index):
            return server.app.test_client().post('/api/projects', json={'name': f'P{index}'}).status_code

        def update(index):
            return server.app.test_client().put(f"/api/projects/{target['id']}", json={'data': {'n': index}}).status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(create, range(20))) + list(pool.map(update, range(20)))
        self.assertTrue(all(code in (200, 201) for code in results))
        self.assertEqual(len(self.client.get('/api/projects').get_json()), 21)


class ClipReferenceTest(ServerTestCase):
    def test_media_paths_cannot_escape_their_folders(self):
        self.assertIsNone(server.resolve_media_path('temp/../../etc/passwd'))
        self.assertIsNone(server.resolve_media_path('../server.py'))
        self.assertIsNone(server.resolve_media_path(''))
        self.assertIsNone(server.resolve_media_path(None))
        inside = server.resolve_media_path('temp/group/clip.mp4')
        self.assertTrue(inside.startswith(server.TEMP_DIR + os.sep))

    def test_source_videos_must_be_inside_the_library(self):
        outside = os.path.join(self.root, 'private.mp4')
        open(outside, 'wb').close()
        inside = os.path.join(self.library, 'movie $1 `x`.mp4')
        open(inside, 'wb').close()
        self.assertIsNone(server.resolve_source_video(outside))
        self.assertIsNone(server.resolve_source_video(os.path.join(self.library, '..', 'private.mp4')))
        self.assertEqual(server.resolve_source_video(inside), inside)
        self.assertEqual(server.resolve_source_video('movie $1 `x`.mp4'), inside)

    def test_unknown_clip_is_a_404_not_a_crash(self):
        self.assertEqual(self.client.get('/clip_audio?clip=temp/nope.mp4').status_code, 404)
        self.assertEqual(self.client.post('/get_waveform', json={'clip': 'temp/nope.mp4'}).status_code, 404)
        self.assertEqual(self.client.post('/get_waveform', data='x', content_type='application/json').status_code, 404)
        self.assertEqual(self.client.post('/ensure_clip', json={'clip': '../evil.mp4'}).status_code, 400)
        self.assertEqual(self.client.post('/ensure_clip', json={'clip': 'temp/x.mp4'}).status_code, 404)
        self.assertEqual(self.client.post('/merge_videos_stream', json={'videos': []}).status_code, 400)


@unittest.skipUnless(HAVE_FFMPEG, 'ffmpeg/ffprobe missing')
class ClipEndpointsTest(ServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if server is None:
            return
        cls.source = os.path.join(cls.library, 'tone $pecial.mp4')     # shell metacharacter on purpose
        subprocess.run([
            'ffmpeg', '-v', 'error', '-y',
            '-f', 'lavfi', '-i', 'testsrc2=size=320x180:rate=25:duration=6',
            '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000:duration=6',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', cls.source,
        ], check=True)
        cls.reference = {'clip': 'temp/group/clip_a.mp4', 'source': cls.source, 'originalStart': 1.25, 'originalEnd': 3.0}

    def test_probe_helpers_handle_shell_metacharacters(self):
        self.assertAlmostEqual(server.get_video_duration(self.source), 6.0, delta=0.2)
        self.assertAlmostEqual(server.get_video_fps(self.source), 25.0, places=3)

    def test_clip_audio_is_exactly_as_long_as_the_cut(self):
        query = 'clip={clip}&source={source}&original_start={originalStart}&original_end={originalEnd}'.format(
            **{key: (value if not isinstance(value, str) else __import__('urllib.parse').parse.quote(value))
               for key, value in self.reference.items()})
        response = self.client.get('/clip_audio?' + query)
        self.assertEqual(response.status_code, 200)
        path = os.path.join(self.root, 'clip.wav')
        with open(path, 'wb') as handle:
            handle.write(response.data)
        with wave.open(path) as handle:
            self.assertEqual((handle.getframerate(), handle.getnchannels(), handle.getsampwidth()), (48000, 2, 2))
            self.assertEqual(handle.getnframes(), round(1.75 * 48000))
        trimmed = self.client.get('/clip_audio?' + query + '&start_ms=250&end_trim_ms=500&speed=0.5')
        with open(path, 'wb') as handle:
            handle.write(trimmed.data)
        with wave.open(path) as handle:
            self.assertEqual(handle.getnframes(), round((1.75 - 0.75) / 0.5 * 48000))
        self.assertEqual(self.client.get('/clip_audio?' + query + '&start_ms=1000&end_trim_ms=800').status_code, 400)

    def test_waveform_reports_the_cut_duration(self):
        data = self.client.post('/get_waveform', json={**self.reference, 'width': 200}).get_json()
        self.assertEqual(len(data['waveform']), 200)
        self.assertAlmostEqual(data['duration_ms'], 1750.0, places=3)
        self.assertGreater(min(data['waveform']), 0.5)        # a steady tone fills the whole clip

    def test_ensure_clip_restores_a_missing_preview_clip(self):
        clip_path = server.resolve_media_path(self.reference['clip'])
        if os.path.exists(clip_path):
            os.remove(clip_path)
        response = self.client.post('/ensure_clip', json=self.reference)
        self.assertEqual(response.get_json()['status'], 'restored')
        self.assertTrue(os.path.isfile(clip_path))
        self.assertEqual(self.client.post('/ensure_clip', json=self.reference).get_json()['status'], 'exists')
        self.assertAlmostEqual(server.cached_probe(clip_path).audio_duration, 1.75, delta=0.03)

    def test_legacy_clips_can_be_rehomed_to_a_content_addressed_file(self):
        legacy = {**self.reference, 'clip': 'temp/sentence-x_clips/sentence-x_00003.mp4'}
        first = self.client.post('/ensure_clip', json={**legacy, 'canonical': True}).get_json()
        self.assertEqual(first['status'], 'restored')
        self.assertRegex(first['clip'], r'^temp/sentence-x_clips/sentence-x_[0-9a-f]{16}\.mp4$')
        self.assertTrue(os.path.isfile(server.resolve_media_path(first['clip'])))
        self.assertFalse(os.path.exists(server.resolve_media_path(legacy['clip'])))
        again = self.client.post('/ensure_clip', json={**legacy, 'canonical': True}).get_json()
        self.assertEqual((again['status'], again['clip']), ('exists', first['clip']))
        # a fresh search for the same cut uses the very same file name
        self.assertEqual(os.path.basename(first['clip']),
                         server.clip_file_name('sentence-x', self.reference['source'], 1.25, 3.0))

    def test_merge_stream_renders_the_timeline(self):
        videos = [
            {'title': 'one', **self.reference, 'startTrim': 250, 'endTrim': 250, 'speed': 1},
            {'title': 'two', **self.reference, 'startTrim': 0, 'endTrim': 750, 'speed': 0.5},
        ]
        response = self.client.post('/merge_videos_stream', json={'videos': videos})
        events = [json.loads(line[6:]) for line in response.get_data(as_text=True).splitlines() if line.startswith('data: ')]
        self.assertTrue(events[-1].get('done'), events[-1])
        summary = events[-2]
        self.assertEqual(summary['duration_ms'], 1250 + 2000)
        merged = os.path.join(server.TEMP_DIR, events[-1]['merged_video'][len('temp/'):])
        self.assertAlmostEqual(server.get_video_duration(merged), 3.25, delta=0.05)
        self.assertEqual([name for name in os.listdir(server.TEMP_DIR) if name.startswith('merge_')], [])

    def test_merge_reports_overtrimmed_clips_as_readable_errors(self):
        videos = [{'title': 'too "short"', **self.reference, 'startTrim': 1000, 'endTrim': 800}]
        body = self.client.post('/merge_videos_stream', json={'videos': videos}).get_data(as_text=True)
        events = [json.loads(line[6:]) for line in body.splitlines() if line.startswith('data: ')]
        self.assertIn('trimmed to nothing', events[-1]['error'])


class StoryResponseTest(ServerTestCase):
    CORPUS = [{'text': 'first corpus sentence'}, {'text': 'second corpus sentence'}]

    def parse(self, text):
        return server.parse_story_response(text, self.CORPUS)

    def test_json_with_text_around_it(self):
        text = 'Sure! Here is the story:\n```json\n{"explanation": "why", "sentences": [{"sentence": "A b", "source_segments": ["a", "b"]}]}\n```\nHope {this} helps'
        explanation, queries, indices, _raw = self.parse(text)
        self.assertEqual((explanation, queries, indices), ('why', [('A b', ['a', 'b'])], None))

    def test_off_format_answers_do_not_crash_or_search_single_letters(self):
        _, queries, _, _ = self.parse(json.dumps({'sentences': [
            {'sentence': 'One', 'source_segments': 'in der'},          # string instead of list
            {'sentence': 'Two', 'source_segments': ['in der', 7, '']}, # junk items
            {'sentence': None, 'source_segments': ['only segments']},  # no sentence
            {'sentence': 'Four'},                                      # no segments
            'Five',                                                    # bare string
            42,
        ]}))
        self.assertEqual(queries, [('One', ['in der']), ('Two', ['in der']), ('only segments', ['only segments']),
                                   ('Four', ['Four']), ('Five', ['Five'])])

    def test_legacy_index_format(self):
        _, queries, indices, _ = self.parse('{"segments": [2, "x", 99, true, 1]}')
        self.assertEqual(queries, [('second corpus sentence', ['second corpus sentence']),
                                   ('first corpus sentence', ['first corpus sentence'])])
        self.assertEqual(indices, [2, 'x', 99, True, 1])

    def test_no_json_is_an_error(self):
        for text in ('I cannot do that.', '{broken', '[1, 2]'):
            with self.assertRaises(ValueError):
                self.parse(text)

    def test_prompt_honours_prefer_long_segments(self):
        original = server.load_transcript_sentences
        server.load_transcript_sentences = lambda path: [{'content': 'a sentence that is long enough', 'words': []}]
        try:
            long_prompt = server.build_story_llm_prompt('story', ['x.mp4'], 5, prefer_long_segments=True)[0]
            any_prompt = server.build_story_llm_prompt('story', ['x.mp4'], 5, prefer_long_segments=False)[0]
        finally:
            server.load_transcript_sentences = original
        self.assertIn('Prefer using longer segments', long_prompt)
        self.assertNotIn('longer segments', any_prompt)
        self.assertIn('x.mp4: a sentence that is long enough', any_prompt)


class MatchingTest(ServerTestCase):
    WORDS = 'the quick brown fox jumps over the lazy dog and the quick red fox sleeps'.split()

    def _sentences(self, gap=0.1):
        words, cursor = [], 1.0
        for word in self.WORDS:
            words.append({'word': word, 'start': round(cursor, 3), 'end': round(cursor + 0.3, 3)})
            cursor += 0.3 + gap
        return [{'content': ' '.join(self.WORDS[:9]), 'start': 1.0, 'end': words[8]['end'], 'words': words[:9]},
                {'content': ' '.join(self.WORDS[9:]), 'start': words[9]['start'], 'end': words[-1]['end'], 'words': words[9:]}]

    def test_phrases_are_found_across_sentence_boundaries_and_in_order(self):
        sentences = self._sentences()
        matches = server.find_matches_for_segment('the quick', sentences)
        self.assertEqual([round(m['start'], 3) for m in matches], [1.0, 5.0])
        self.assertEqual(len(server.find_matches_for_segment('dog and the', sentences)), 1)
        self.assertEqual(server.find_matches_for_segment('quick fox', sentences), [])
        self.assertEqual(server.find_matches_for_segment('', sentences), [])

    def test_index_gives_the_same_result_as_the_raw_transcript(self):
        sentences = self._sentences()
        index = server.WordIndex(sentences)
        for phrase in ('the', 'fox', 'the quick', 'quick brown fox jumps', 'sleeps', 'missing'):
            self.assertEqual(server.find_matches_for_segment(phrase, index, 0.0, 10.0, 2),
                             server.find_matches_for_segment(phrase, sentences, 0.0, 10.0, 2))

    def test_single_words_respect_the_silence_window_and_longer_phrases_do_not(self):
        sentences = self._sentences(gap=0.1)
        self.assertEqual(server.find_matches_for_segment('fox', sentences, min_duration=0.5, max_duration=10.0), [])
        self.assertEqual(len(server.find_matches_for_segment('fox', sentences, min_duration=0.05, max_duration=10.0)), 2)
        self.assertEqual(len(server.find_matches_for_segment('quick brown', sentences, min_duration=0.5, max_duration=10.0)), 1)

    def test_typographic_apostrophes_and_case_do_not_matter(self):
        self.assertEqual(server.normalize_token('Don\u2019t,'), "don't")
        self.assertEqual(server.normalize_token('\u201eStra\u00dfe\u201c'), 'strasse')
        self.assertEqual(server.normalize_token('well\u2013known'), 'well-known')
        sentences = [{'content': "I don't know", 'words': [
            {'word': 'I', 'start': 0.0, 'end': 0.2}, {'word': "don't", 'start': 0.3, 'end': 0.6},
            {'word': 'know.', 'start': 0.7, 'end': 1.0}]}]
        self.assertEqual(len(server.find_matches_for_segment('i don\u2019t know', sentences)), 1)

    def test_partial_matches_skip_short_sub_phrases_unless_asked(self):
        sentences = self._sentences()
        filtered = {m['segment'] for m in server.find_longest_matches('the quick brown fox', sentences, min_words=3)}
        everything = {m['segment'] for m in server.find_longest_matches('the quick brown fox', sentences)}
        self.assertEqual(filtered, {'the quick brown fox', 'the quick brown', 'quick brown fox'})
        self.assertIn('fox', everything)
        self.assertIn('the quick', everything)

    def test_results_are_spread_over_the_source_files(self):
        items = [{'file': name, 'match': index} for name in ('a', 'b', 'c') for index in range(3)][:-2]
        mixed = server.interleave_by_file(items)
        self.assertEqual([item['file'] for item in mixed], ['a', 'b', 'c', 'a', 'b', 'a', 'b'])
        self.assertEqual(len(mixed), len(items))

    def test_clip_bounds_are_clamped_to_the_file(self):
        self.assertEqual(server.compute_clip_bounds(10.0, 11.0, 60.0), (9.55, 11.45))
        self.assertEqual(server.compute_clip_bounds(0.2, 0.8, 60.0), (0.0, 1.25))
        start, end = server.compute_clip_bounds(59.5, 59.9, 60.0)
        self.assertEqual((round(start, 2), end), (59.05, 60.0))
        with self.assertRaises(ValueError):
            server.compute_clip_bounds(5.0, 6.0, 0.0)

    def test_clip_names_depend_only_on_source_and_bounds(self):
        first = server.clip_file_name('group', 'Library/a.mp4', 1.2345, 3.0)
        self.assertEqual(first, server.clip_file_name('group', 'Library/a.mp4', 1.2345, 3.0))
        self.assertNotEqual(first, server.clip_file_name('group', 'Library/a.mp4', 1.3, 3.0))
        self.assertNotEqual(first, server.clip_file_name('group', 'Library/b.mp4', 1.2345, 3.0))


@unittest.skipUnless(HAVE_FFMPEG, 'ffmpeg/ffprobe missing')
class SearchExportTest(ServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if server is None:
            return
        cls.source = os.path.join(cls.library, 'talk.mp4')
        subprocess.run([
            'ffmpeg', '-v', 'error', '-y',
            '-f', 'lavfi', '-i', 'testsrc2=size=320x180:rate=25:duration=12',
            '-f', 'lavfi', '-i', 'sine=frequency=330:sample_rate=48000:duration=12',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', cls.source,
        ], check=True)
        words = [('hello', 0.2, 0.6), ('there', 0.7, 1.0), ('general', 1.1, 1.6),
                 ('hello', 5.0, 5.4), ('there', 5.5, 5.8), ('friend', 5.9, 6.3),
                 ('hello', 9.0, 9.4), ('again', 9.5, 9.9)]
        transcript = [{'content': ' '.join(w for w, _, _ in words), 'start': 0.2, 'end': 9.9,
                       'words': [{'word': w, 'start': a, 'end': b} for w, a, b in words]}]
        with open(os.path.join(cls.library, 'talk.json'), 'w', encoding='utf-8') as handle:
            json.dump(transcript, handle)

    def _search(self, **body):
        payload = {'files': [self.source], 'sentenceSearchCounter': 1, **body}
        text = self.client.post('/search_longest_segments_stream', json=payload).get_data(as_text=True)
        return [json.loads(line[6:]) for line in text.splitlines() if line.startswith('data: ')]

    def test_search_exports_clips_with_bounds_and_padding_info(self):
        events = self._search(sentence_groups=['hello there'])
        self.assertTrue(events[-1]['done'])
        result = next(event for event in events if event.get('files'))
        self.assertEqual(result['phrase'], 'hello there')
        self.assertEqual(result['total_matches'], 2)
        first, second = result['files']
        self.assertEqual((first['start'], first['end']), (0.2, 1.0))                 # matched order, not encode order
        self.assertEqual((first['original_start'], first['lead_ms'], first['tail_ms']), (0.0, 200, 450))  # clamped at file start
        self.assertEqual((second['lead_ms'], second['tail_ms'], second['duration_ms']), (450, 450, 1700))
        for item in (first, second):
            self.assertTrue(os.path.isfile(server.resolve_media_path(item['file'])))
        folder = os.path.dirname(server.resolve_media_path(first['file']))
        self.assertEqual([name for name in os.listdir(folder) if not name.endswith('.mp4')], [])

    def test_a_clip_path_always_denotes_the_same_cut(self):
        wide = next(e for e in self._search(sentence_groups=['hello'], maxResultsPerSegment=25) if e.get('files'))
        narrow = next(e for e in self._search(sentence_groups=['hello'], maxResultsPerSegment=1) if e.get('files'))
        by_path = {item['file']: (item['original_start'], item['original_end']) for item in wide['files']}
        self.assertEqual(len(by_path), 3)
        self.assertEqual(len(narrow['files']), 1)
        self.assertEqual(by_path[narrow['files'][0]['file']],
                         (narrow['files'][0]['original_start'], narrow['files'][0]['original_end']))
        stamp = {path: os.path.getmtime(server.resolve_media_path(path)) for path in by_path}
        again = next(e for e in self._search(sentence_groups=['hello']) if e.get('files'))
        self.assertEqual({item['file'] for item in again['files']}, set(by_path))
        self.assertEqual(stamp, {path: os.path.getmtime(server.resolve_media_path(path)) for path in by_path})  # reused

    def test_commas_do_not_split_a_bare_sentence(self):
        events = self._search(sentence='hello, there')
        phrases = [event['phrase'] for event in events if event.get('files')]
        self.assertEqual(phrases, ['hello, there'])

    def test_files_outside_the_library_are_ignored(self):
        outside = os.path.join(self.root, 'outside.mp4')
        shutil.copy(self.source, outside)
        shutil.copy(os.path.join(self.library, 'talk.json'), os.path.join(self.root, 'outside.json'))
        events = self._search(sentence_groups=['hello'], files=[outside])
        self.assertEqual([event for event in events if event.get('files')], [])
        self.assertEqual(self.client.post('/get_sentences', json={'files': [outside]}).get_json(), {'sentences': []})

    def test_skip_before_the_first_clip_yields_a_skipped_event(self):
        search_id = 'test-skip'
        with server.SKIP_LOCK:
            server.SKIP_SEGMENTS[search_id] = {'hello there'}
        try:
            events = list(server.find_and_export_longest_matches_incremental(
                'hello there', [self.source], 0, export_clips=True, search_id=search_id))
        finally:
            server.SKIP_SEGMENTS.pop(search_id, None)
        self.assertEqual([event for event in events if 'phrase' in event],
                         [{'phrase': 'hello there', 'files': [], 'word_count': 2, 'skipped': True}])

    def test_cancel_stops_the_search(self):
        search_id = 'test-cancel'
        with server.CANCEL_LOCK:
            server.CANCELLED_SEARCHES.add(search_id)
        try:
            with self.assertRaises(server.SearchCancelled):
                list(server.find_and_export_longest_matches_incremental(
                    'hello there', [self.source], 0, export_clips=True, search_id=search_id))
        finally:
            server.CANCELLED_SEARCHES.discard(search_id)

    def test_skip_and_cancel_are_ignored_for_searches_that_are_not_running(self):
        self.assertEqual(self.client.post('/cancel_search', json={'search_id': 'ended'}).status_code, 404)
        self.assertEqual(self.client.post('/skip_segment', json={'search_id': 'ended', 'segment_phrase': 'x'}).status_code, 404)
        self.assertEqual(self.client.post('/skip_segment', json={'search_id': 5, 'segment_phrase': 5}).status_code, 400)
        self.assertNotIn('ended', server.CANCELLED_SEARCHES)
        self.assertNotIn('ended', server.SKIP_SEGMENTS)

    def test_failed_renders_are_reported_with_the_result(self):
        original = server.render_preview_clip

        def failing(source_path, clip_start, clip_end, output_path, should_abort=None):
            raise RuntimeError('simulated encoder failure')

        server.render_preview_clip = failing
        try:
            events = self._search(sentence_groups=['there friend'])
        finally:
            server.render_preview_clip = original
        result = next(event for event in events if event.get('phrase') == 'there friend')
        self.assertEqual((result['files'], result['failed'], result['first_error']), ([], 1, 'simulated encoder failure'))

    def test_silence_search_streams_each_clip_once(self):
        text = self.client.post('/search_silences_stream', json={'files': [self.source], 'minSilence': 2.0, 'maxSilence': 5.0}).get_data(as_text=True)
        events = [json.loads(line[6:]) for line in text.splitlines() if line.startswith('data: ')]
        clips = [item for event in events for item in event.get('files', [])]
        self.assertEqual(len(clips), 2)
        self.assertEqual(len({item['file'] for item in clips}), 2)
        self.assertEqual(events[-1]['total_silences'], 2)
        self.assertEqual(sorted(round(item['silence_end'] - item['silence_start'], 2) for item in clips), [2.7, 3.4])

    def test_plain_subtitles_make_a_video_searchable(self):
        video = os.path.join(self.library, 'subtitled.mp4')
        shutil.copy(self.source, video)
        with open(os.path.join(self.library, 'subtitled.srt'), 'w', encoding='utf-8') as handle:
            handle.write('1\n00:00:02,000 --> 00:00:04,000\nGood morning everybody\n\n2\n00:00:06,000 --> 00:00:07,000\nSee you\n')
        try:
            index = server.load_word_index(video)
            self.assertEqual([word['normalized_word'] for word in index.words], ['good', 'morning', 'everybody', 'see', 'you'])
            match = server.find_matches_for_segment('morning everybody', index)[0]
            self.assertGreater(match['start'], 2.0)
            self.assertAlmostEqual(match['end'], 4.0, places=3)
            self.assertFalse(os.path.exists(os.path.join(self.library, 'subtitled.json')))   # nothing written
            self.assertIs(server.load_word_index(video), index)                                # cached
        finally:
            os.remove(video)
            os.remove(os.path.join(self.library, 'subtitled.srt'))

    def test_a_sibling_videos_transcript_is_not_borrowed(self):
        video = os.path.join(self.library, 'show.mp4')
        shutil.copy(self.source, video)
        shutil.copy(os.path.join(self.library, 'talk.json'), os.path.join(self.library, 'show.part2.json'))
        try:
            self.assertFalse(server._has_own_transcript(video))
            self.assertIsNone(server.load_transcript_sentences(video))
            os.rename(os.path.join(self.library, 'show.part2.json'), os.path.join(self.library, 'show.en.json'))
            self.assertTrue(server._has_own_transcript(video))          # <name>.<language>.json is its own
            self.assertTrue(server.load_transcript_sentences(video))
        finally:
            for name in ('show.mp4', 'show.part2.json', 'show.en.json'):
                if os.path.exists(os.path.join(self.library, name)):
                    os.remove(os.path.join(self.library, name))

    def test_transcripts_are_cached_until_the_file_changes(self):
        first = server.load_transcript_sentences(self.source)
        self.assertIs(first, server.load_transcript_sentences(self.source))
        path = os.path.join(self.library, 'talk.json')
        data = json.load(open(path, encoding='utf-8'))
        data[0]['words'].append({'word': 'extra', 'start': 10.5, 'end': 10.9})
        with open(path, 'w', encoding='utf-8') as handle:
            json.dump(data, handle)
        try:
            self.assertEqual(len(server.find_matches_for_segment('extra', server.load_word_index(self.source))), 1)
        finally:
            data[0]['words'].pop()
            with open(path, 'w', encoding='utf-8') as handle:
                json.dump(data, handle)


if __name__ == '__main__':
    unittest.main()

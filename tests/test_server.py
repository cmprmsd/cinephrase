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


if __name__ == '__main__':
    unittest.main()

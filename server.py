import argparse
import glob
import hashlib
import ipaddress
import json
import os
import posixpath
import re
import shutil
import socket
import subprocess
import sys
import traceback
import unicodedata
import uuid
from collections import OrderedDict
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from threading import Event, Lock, RLock, Semaphore
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from flask import Flask, jsonify, render_template, request, send_from_directory, Response, stream_with_context
from werkzeug.exceptions import HTTPException
from videogrep import find_transcript, parse_transcript
from srt_word_boundaries import convert_subtitles
import render_pipeline

app = Flask(__name__)

DATA_DIR = os.path.join(app.root_path, 'data')
TEMP_DIR = os.path.join(app.root_path, 'temp')
PROJECTS_FILE = os.path.join(DATA_DIR, 'projects.json')
COLLECTIONS_FILE = os.path.join(DATA_DIR, 'collections.json')
# Reentrant: routes hold the lock across read-modify-write, the read/write helpers take it again.
PROJECT_STORE_LOCK = RLock()
COLLECTIONS_LOCK = RLock()

# Track skipped segments per search session
SKIP_SEGMENTS = {}  # Format: {search_id: set(segment_phrases)}
SKIP_LOCK = Lock()

# Track cancelled searches
CANCELLED_SEARCHES = set()  # Format: set of search_id strings
CANCEL_LOCK = Lock()


class SearchCancelled(Exception):
    """The user cancelled the running search."""

# GPU encoder detection cache (initialized at startup)
_GPU_ENCODER_CACHE = None
_GPU_DISABLED = False  # Track if GPU was disabled due to runtime failures

# Maximum GPU encoding streams (set via --gpustreams argument, default 2)
MAX_GPU_STREAMS = 2

# Semaphore to limit concurrent GPU encoding sessions (initialized at startup)
GPU_ENCODING_SEMAPHORE = None

def test_gpu_encoder(encoder_name, encoder_args):
    """
    Test if a GPU encoder actually works by encoding a test frame.
    Uses a more realistic test with actual video decoding to catch codec compatibility issues.
    Returns True if encoder works, False otherwise.
    """
    try:
        # Test with a more realistic scenario that requires video decoding
        # This helps catch driver/SDK compatibility issues that simple tests might miss
        # Use testsrc2 which requires actual video processing
        test_cmd = [
            'ffmpeg', '-y',
            '-f', 'lavfi',
            '-i', 'testsrc2=duration=0.2:size=640x480:rate=25',
            '-t', '0.2',
            '-c:v', encoder_name
        ]
        test_cmd.extend(encoder_args)
        test_cmd.extend(['-f', 'null', '-'])
        
        result = subprocess.run(
            test_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15
        )
        
        if result.returncode == 0:
            return True
        
        # Check for GPU-specific errors
        stderr = result.stderr.decode() if result.stderr else ''
        gpu_errors = [
            'No capable devices', 
            'incompatible client key', 
            'OpenEncodeSessionEx failed', 
            'No device', 
            'Cannot load', 
            'failed to initialize', 
            'encoder initialization',
            'Could not open encoder'
        ]
        
        # Return code 187 often indicates encoder initialization failure (driver issues)
        if result.returncode == 187 or any(err.lower() in stderr.lower() for err in gpu_errors):
            # Log the specific error for debugging
            if 'incompatible client key' in stderr.lower():
                print(f"[GPU] Test failed: Driver/SDK incompatibility detected for {encoder_name}")
                print(f"[GPU] This usually means NVIDIA driver version doesn't match FFmpeg's NVENC SDK")
                print(f"[GPU] Error: {stderr[:500]}")
            return False
        
        # If it's not a clear GPU error, still return False to be safe
        return False
    except Exception as e:
        print(f"[GPU] Exception during encoder test: {e}")
        return False

def disable_gpu_encoder():
    """Disable GPU encoding due to runtime failures"""
    global _GPU_ENCODER_CACHE, _GPU_DISABLED
    _GPU_DISABLED = True
    _GPU_ENCODER_CACHE = (None, None)
    print("[GPU] GPU encoder disabled due to runtime failures, switching to CPU")

# Hardware H.264 encoders in order of preference, each with the argument sets to try (simplest first: some
# driver/ffmpeg combinations reject the richer ones). HEVC and VAAPI are left out on purpose: browsers cannot
# play HEVC preview clips reliably, and VAAPI needs a device/hwupload setup the export commands do not have.
GPU_ENCODER_CANDIDATES = [
    ('h264_nvenc', [
        ['-preset', 'p4'],
        ['-preset', 'p4', '-rc', 'vbr'],
        ['-preset', 'p1', '-rc', 'cbr', '-b:v', '5M'],
        ['-preset', 'p4', '-rc', 'constqp', '-qp', '23'],
        ['-preset', 'p4', '-rc', 'vbr', '-cq', '23'],
    ]),
    ('h264_qsv', [['-preset', 'medium', '-global_quality', '23']]),
    ('h264_amf', [['-quality', 'balanced', '-rc', 'vbr_peak', '-qmin', '18', '-qmax', '28']]),
    ('h264_videotoolbox', [['-allow_sw', '1', '-b:v', '5M']]),
]


def detect_gpu_encoder():
    """
    Detect available GPU encoder for hardware acceleration.
    Returns tuple: (encoder_name, encoder_args) or (None, None) if no GPU available.
    Every encoder ffmpeg lists is test-encoded: stock ffmpeg builds list encoders for hardware that is not there.
    """
    global _GPU_ENCODER_CACHE, _GPU_DISABLED
    if _GPU_DISABLED:
        return (None, None)
    if _GPU_ENCODER_CACHE is not None:
        return _GPU_ENCODER_CACHE

    try:
        result = subprocess.run(
            ['ffmpeg', '-hide_banner', '-encoders'],
            capture_output=True,
            text=True,
            timeout=5
        )
        encoders = (result.stdout + result.stderr) if result.returncode == 0 else ''
        for encoder_name, argument_sets in GPU_ENCODER_CANDIDATES:
            if encoder_name not in encoders:
                continue
            for encoder_args in argument_sets:
                if test_gpu_encoder(encoder_name, encoder_args):
                    _GPU_ENCODER_CACHE = (encoder_name, encoder_args)
                    print(f"[GPU] Detected and verified {encoder_name} with args: {encoder_args}")
                    return _GPU_ENCODER_CACHE
            print(f"[GPU] {encoder_name} is listed by ffmpeg but not functional on this machine")
    except Exception as e:
        print(f"[GPU] Could not detect GPU encoder: {e}")

    _GPU_ENCODER_CACHE = (None, None)
    print("[GPU] No working GPU encoder found, using CPU (libx264)")
    return _GPU_ENCODER_CACHE

DEFAULT_PROJECT_STATE = {
    'name': 'Untitled Project',
    'data': {
        'sentences': [],
        'currentSentence': '',
        'phraseInput': '',
        'selectedFiles': [],
        'timeline': [],
        'silencePreferences': {
            'minSilence': 0.0,
            'maxSilence': 10.0,
            'silenceWordThreshold': 2
        }
    }
}

# Quality-targeted arguments for the merge, per GPU encoder (the detection arguments only prove the encoder
# works; "-preset p4" alone encodes at the encoder's small default bitrate).
MERGE_GPU_QUALITY_ARGS = {
    'h264_nvenc': ['-preset', 'p5', '-rc', 'vbr', '-cq', '19', '-b:v', '0'],
    'h264_qsv': ['-preset', 'medium', '-global_quality', '19'],
    'h264_amf': ['-quality', 'quality', '-rc', 'cqp', '-qp_i', '19', '-qp_p', '19'],
    'h264_videotoolbox': ['-allow_sw', '1', '-q:v', '60'],
}
_MERGE_GPU_CACHE = {}


def get_merge_gpu_encoder():
    """GPU encoder and arguments for merges: the quality set if this machine accepts it, else the detected set."""
    encoder, detected_args = get_gpu_encoder()
    if not encoder:
        return (None, ())
    if encoder not in _MERGE_GPU_CACHE:
        quality_args = MERGE_GPU_QUALITY_ARGS.get(encoder)
        if quality_args and test_gpu_encoder(encoder, quality_args):
            _MERGE_GPU_CACHE[encoder] = quality_args
            print(f"[GPU] Merge uses {encoder} with {quality_args}")
        else:
            _MERGE_GPU_CACHE[encoder] = list(detected_args or [])
            print(f"[GPU] Merge uses {encoder} with the detection arguments {detected_args}")
    return (encoder, _MERGE_GPU_CACHE[encoder])


def get_max_workers():
    """
    Get optimal number of worker threads (real CPU cores only, excluding hyperthreading).
    Uses /proc/cpuinfo on Linux to count physical cores, falls back to os.cpu_count() // 2 on other systems.
    """
    try:
        # Try to get real core count (excluding hyperthreading) on Linux
        if os.path.exists('/proc/cpuinfo'):
            with open('/proc/cpuinfo', 'r') as f:
                cpuinfo = f.read()
                # Count unique physical cores (look for "physical id" and "core id" pairs)
                physical_ids = set()
                for line in cpuinfo.split('\n'):
                    if line.startswith('physical id'):
                        physical_id = line.split(':')[1].strip()
                        physical_ids.add(physical_id)
                
                # If we found physical IDs, count cores per physical CPU
                if physical_ids:
                    cores_per_cpu = {}
                    current_physical = None
                    for line in cpuinfo.split('\n'):
                        if line.startswith('physical id'):
                            current_physical = line.split(':')[1].strip()
                        elif line.startswith('core id'):
                            current_core = line.split(':')[1].strip()
                            if current_physical and current_core:
                                key = f"{current_physical}_{current_core}"
                                cores_per_cpu[current_physical] = cores_per_cpu.get(current_physical, set())
                                cores_per_cpu[current_physical].add(current_core)
                    
                    if cores_per_cpu:
                        # Count total unique cores across all physical CPUs
                        total_cores = sum(len(cores) for cores in cores_per_cpu.values())
                        if total_cores > 0:
                            return total_cores
                
                # Fallback: count processor entries (might include hyperthreading)
                processor_count = cpuinfo.count('processor\t:')
                if processor_count > 0:
                    # Assume hyperthreading if processor count is even and > 2
                    # Divide by 2 as a heuristic (not perfect but better than using all)
                    if processor_count > 2 and processor_count % 2 == 0:
                        return processor_count // 2
                    return processor_count
        
        # Fallback for non-Linux systems: use os.cpu_count() and assume hyperthreading
        cpu_count = os.cpu_count() or 4
        # Heuristic: if CPU count is even and > 2, assume hyperthreading and use half
        if cpu_count > 2 and cpu_count % 2 == 0:
            return cpu_count // 2
        return cpu_count
    except:
        return 4

def get_max_gpu_workers():
    """
    Get maximum number of concurrent GPU encoding workers.
    Returns the value set via --gpustreams command-line argument (default: 2).
    """
    return MAX_GPU_STREAMS

def get_gpu_encoder():
    """
    Get the cached GPU encoder (detected at startup).
    Returns tuple: (encoder_name, encoder_args) or (None, None) if no GPU available.
    """
    global _GPU_ENCODER_CACHE
    return _GPU_ENCODER_CACHE if _GPU_ENCODER_CACHE is not None else (None, None)

def initialize_gpu_at_startup():
    """
    Initialize GPU encoder detection at startup (only once).
    This avoids repeated checks during video rendering.
    """
    global _GPU_ENCODER_CACHE, GPU_ENCODING_SEMAPHORE
    
    print("[Startup] Detecting GPU encoder...")
    gpu_encoder, gpu_args = detect_gpu_encoder()
    
    if gpu_encoder:
        print(f"[Startup] ✓ GPU encoder detected: {gpu_encoder}")
        print(f"[Startup] ✓ GPU encoding enabled with max {MAX_GPU_STREAMS} concurrent streams")
    else:
        print("[Startup] ✓ No GPU encoder found, using CPU encoding only")
    
    # Initialize GPU semaphore with the configured max streams
    GPU_ENCODING_SEMAPHORE = Semaphore(MAX_GPU_STREAMS)
    print(f"[Startup] ✓ GPU semaphore initialized with limit: {MAX_GPU_STREAMS}")

def ensure_project_store():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)


def _utc_now():
    """Current UTC time without tzinfo (the stored timestamps carry a literal 'Z')."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def timestamp():
    return _utc_now().replace(microsecond=0).isoformat() + 'Z'


def _load_json_list(path, key):
    """Parse ``{key: [...]}`` from ``path``. Returns None if the file is unreadable or has another shape."""
    try:
        with open(path, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except (OSError, ValueError):
        return None
    items = data.get(key) if isinstance(data, dict) else None
    return items if isinstance(items, list) else None


def _dump_json_atomically(path, payload):
    temp_path = f'{path}.{uuid.uuid4().hex}.tmp'
    try:
        with open(temp_path, 'w', encoding='utf-8') as file:
            json.dump(payload, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def read_json_store(path, key):
    """Read a ``{key: [...]}`` store.

    A damaged file is never treated as "empty" (the next save would overwrite everything): the previous good
    version (``.bak``) is restored instead and the damaged file is kept next to it for manual recovery.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        return []
    items = _load_json_list(path, key)
    if items is not None:
        return items
    damaged_copy = f"{path}.damaged-{_utc_now().strftime('%Y%m%d-%H%M%S')}"
    try:
        shutil.copy2(path, damaged_copy)
    except OSError:
        damaged_copy = None
    backup = _load_json_list(f'{path}.bak', key)
    print(f"[Store] {os.path.basename(path)} is damaged"
          f"{f' (copy kept as {os.path.basename(damaged_copy)})' if damaged_copy else ''}; "
          f"{'restored the previous version from .bak' if backup is not None else 'no usable .bak, starting empty'}")
    recovered = backup if backup is not None else []
    if damaged_copy:
        _dump_json_atomically(path, {key: recovered})
    return recovered


def write_json_store(path, key, items):
    """Write a store atomically (temp file + fsync + rename), keeping the previous version as ``.bak``."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if _load_json_list(path, key) is not None:
        shutil.copy2(path, f'{path}.bak')
    _dump_json_atomically(path, {key: items})


def read_projects():
    with PROJECT_STORE_LOCK:
        return read_json_store(PROJECTS_FILE, 'projects')


def write_projects(projects):
    with PROJECT_STORE_LOCK:
        write_json_store(PROJECTS_FILE, 'projects', projects)


def library_files_only(files):
    """The client-supplied video paths that exist inside a library root, as the paths that were checked.

    Paths as /get_files lists them come back unchanged; other spellings of a library file are replaced by the
    file that was actually validated, so nothing downstream opens a path that was not checked.
    """
    if not isinstance(files, list):
        return []
    resolved = (resolve_source_video(path) for path in files if isinstance(path, str))
    return list(OrderedDict.fromkeys(path for path in resolved if path))


def missing_files_note(requested, usable):
    """A sentence for the user when some of the requested videos are not in the library (else '')."""
    wanted = len([path for path in requested if isinstance(path, str)]) if isinstance(requested, list) else 0
    missing = wanted - len(usable)
    if missing <= 0:
        return ''
    return (f'{missing} of {wanted} project videos are not in the library and were not searched '
            f'(moved, deleted, or the server was started without their --library-path).')


def clamp_max_results(value):
    return max(1, min(sanitize_int(value, 25), MAX_RESULTS_PER_SEGMENT_LIMIT))


ALLOWED_HOSTS = set()      # extra host names from --allowed-host (lower case)


def _local_host_names():
    names = {'localhost'}
    for lookup in (socket.gethostname, socket.getfqdn):
        try:
            name = lookup().strip().lower()
        except OSError:
            continue
        if name:
            names.update({name, name.split('.')[0], name.split('.')[0] + '.local'})
    return names


LOCAL_HOST_NAMES = _local_host_names()


def host_is_allowed(host_header):
    """Whether a Host header names this machine: an IP address, localhost, the machine's own name or a name
    given with --allowed-host. Anything else is how a DNS-rebinding page reaches a local server."""
    try:
        host = (urlsplit('//' + (host_header or '')).hostname or '').lower().rstrip('.')
    except ValueError:
        return False
    if not host:
        return False
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    return host in LOCAL_HOST_NAMES or host.endswith('.localhost') or host in ALLOWED_HOSTS


@app.before_request
def reject_foreign_requests():
    """This server has no login: keep other web sites from using the browser to talk to it.

    * The Host header must name this machine (stops DNS rebinding).
    * A request that changes something must come from a page of this server (stops cross-site form posts).
    """
    if not host_is_allowed(request.host):
        return jsonify({'error': f'Host "{request.host}" is not allowed. Open the app by IP address or start '
                                 f'the server with --allowed-host <name>.'}), 403
    if request.method in ('POST', 'PUT', 'DELETE', 'PATCH'):
        origin = request.headers.get('Origin')
        if origin and origin != 'null':
            try:
                same_origin = urlsplit(origin).netloc.lower() == request.host.lower()
            except ValueError:
                same_origin = False
        else:
            # No (usable) Origin: browsers that send none still say where the request comes from
            same_origin = request.headers.get('Sec-Fetch-Site', 'same-origin') in ('same-origin', 'none') \
                and origin != 'null'
        if not same_origin:
            return jsonify({'error': 'Requests from other sites are not accepted.'}), 403
    return None


def request_json_object():
    """The request body as a dict ({} for missing, malformed or non-object JSON)."""
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def find_project(projects, project_id):
    for project in projects:
        if project.get('id') == project_id:
            return project
    return None


def default_project_payload(name=None):
    payload = json.loads(json.dumps(DEFAULT_PROJECT_STATE))
    if name:
        payload['name'] = name
    return payload


def normalize_project_name(name, fallback='Untitled Project'):
    if not isinstance(name, str):
        return fallback
    cleaned = name.strip()
    return cleaned or fallback


@app.route('/api/projects', methods=['GET'])
def list_projects():
    projects = read_projects()
    response = [
        {
            'id': project.get('id'),
            'name': project.get('name', 'Untitled Project'),
            'createdAt': project.get('createdAt'),
            'updatedAt': project.get('updatedAt')
        }
        for project in projects
    ]
    return jsonify(response)


@app.route('/api/projects', methods=['POST'])
def create_project():
    payload = request_json_object()
    name = normalize_project_name(payload.get('name'), DEFAULT_PROJECT_STATE['name'])
    base_payload = default_project_payload(name)

    custom_data = payload.get('data')
    if isinstance(custom_data, dict):
        base_payload['data'].update(custom_data)

    now = timestamp()
    new_project = {
        'id': str(uuid.uuid4()),
        'name': base_payload['name'],
        'data': base_payload['data'],
        'revision': 1,
        'createdAt': now,
        'updatedAt': now
    }

    with PROJECT_STORE_LOCK:
        projects = read_projects()
        projects.append(new_project)
        write_projects(projects)
    return jsonify(new_project), 201


@app.route('/api/projects/<project_id>', methods=['GET'])
def get_project(project_id):
    projects = read_projects()
    project = find_project(projects, project_id)
    if not project:
        return jsonify({'error': 'Project not found'}), 404
    project.setdefault('revision', 0)
    return jsonify(project)


@app.route('/api/projects/<project_id>', methods=['PUT'])
def update_project(project_id):
    payload = request_json_object()
    with PROJECT_STORE_LOCK:
        projects = read_projects()
        project = find_project(projects, project_id)
        if not project:
            return jsonify({'error': 'Project not found'}), 404

        # Optimistic locking: a client that says which revision its copy is based on is refused when the
        # project has been saved from elsewhere since (another tab, a tab left open yesterday), instead of
        # silently replacing the newer timeline with its old one.
        revision = sanitize_int(project.get('revision'), 0)
        base_revision = payload.get('baseRevision')
        if base_revision is not None and sanitize_int(base_revision, -1) != revision:
            return jsonify({'error': 'The project was changed elsewhere', 'project': {**project, 'revision': revision}}), 409
        project['revision'] = revision + 1

        if 'name' in payload:
            project['name'] = normalize_project_name(payload['name'], project.get('name', 'Untitled Project'))

        if isinstance(payload.get('data'), dict):
            project['data'] = payload['data']

        project['updatedAt'] = timestamp()
        write_projects(projects)
    return jsonify(project)


@app.route('/api/projects/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    with PROJECT_STORE_LOCK:
        projects = read_projects()
        updated_projects = [project for project in projects if project.get('id') != project_id]
        if len(updated_projects) == len(projects):
            return jsonify({'error': 'Project not found'}), 404
        write_projects(updated_projects)
    return jsonify({'status': 'deleted', 'id': project_id})


def read_collections():
    """Read collections from file."""
    with COLLECTIONS_LOCK:
        return read_json_store(COLLECTIONS_FILE, 'collections')


def write_collections(collections):
    """Write collections to file."""
    with COLLECTIONS_LOCK:
        write_json_store(COLLECTIONS_FILE, 'collections', collections)


@app.route('/api/collections', methods=['GET'])
def get_collections():
    """Get all collections."""
    with COLLECTIONS_LOCK:
        collections = read_collections()
    return jsonify({'collections': collections})


@app.route('/api/collections', methods=['POST'])
def save_collection():
    """Save a new collection."""
    data = request_json_object()
    name = data.get('name')
    name = name.strip() if isinstance(name, str) else ''
    files = data.get('files', [])
    
    if not name:
        return jsonify({'error': 'Collection name is required'}), 400
    
    if not files or not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        return jsonify({'error': 'Files list is required'}), 400
    
    with COLLECTIONS_LOCK:
        collections = read_collections()
        
        # Same name: update that collection when the client asked for it, refuse otherwise
        existing = next((c for c in collections if c.get('name') == name), None)
        if existing is not None:
            if not data.get('overwrite'):
                return jsonify({'error': 'Collection with this name already exists'}), 400
            existing['files'] = files
            existing['updatedAt'] = timestamp()
            write_collections(collections)
            return jsonify({'status': 'updated', 'collection': existing})
        
        collection = {
            'id': str(uuid.uuid4()),
            'name': name,
            'files': files,
            'createdAt': timestamp(),
            'updatedAt': timestamp()
        }
        
        collections.append(collection)
        write_collections(collections)
    
    return jsonify({'status': 'saved', 'collection': collection})


@app.route('/api/collections/<collection_id>', methods=['DELETE'])
def delete_collection(collection_id):
    """Delete a collection."""
    with COLLECTIONS_LOCK:
        collections = read_collections()
        updated_collections = [c for c in collections if c.get('id') != collection_id]
        if len(updated_collections) == len(collections):
            return jsonify({'error': 'Collection not found'}), 404
        write_collections(updated_collections)
    return jsonify({'status': 'deleted', 'id': collection_id})


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    """API callers always get JSON: the frontend cannot show an HTML error page."""
    if isinstance(error, HTTPException):
        return jsonify({'error': error.description or error.name}), error.code
    print(f"[Server] Unhandled error in {request.method} {request.path}: {error}")
    traceback.print_exc()
    return jsonify({'error': str(error) or error.__class__.__name__}), 500


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/temp/<path:filename>')
def serve_temp(filename):
    """Serve files from the temp directory (generated clips, etc.)."""
    return send_from_directory(TEMP_DIR, filename)

# Define library root folders
LIBRARY_ROOTS = [
                 "Library",
                 #"/mnt/media/Youtube"
                 #"/home/user/Desktop/Source"
                 ]

VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.webm', '.mov', '.m4v', '.avi')

# LLM used by "AI Story" (an Ollama server). Overridden by --ollama-url / --ollama-model or the environment
# variables OLLAMA_URL / OLLAMA_MODEL.
OLLAMA_URL = os.environ.get('OLLAMA_URL', 'http://dockerlxc.cmprmsd.local:11434/api/generate')
OLLAMA_MODEL = os.environ.get('OLLAMA_MODEL', 'llama3.2:latest')
OLLAMA_TIMEOUT = 120

DEFAULT_CLIP_START_PADDING = 0.45
DEFAULT_CLIP_END_PADDING = 0.45
MAX_RESULTS_PER_SEGMENT_LIMIT = 1000

# Merge output format. None = follow the timeline (most common source resolution / frame rate), so portrait or
# 720p libraries are not cropped or upscaled. Overridden by --merge-resolution / --merge-fps.
MERGE_RESOLUTION = None
MERGE_FPS = None


@app.route("/get_sentences", methods=['POST'])
def get_sentences():
    payload = request_json_object()
    selected_files = library_files_only(payload.get('files'))

    corpus_entries = []

    for file_path in selected_files:
        if not isinstance(file_path, str) or not file_path.strip():
            continue
        sentences = load_transcript_sentences(file_path)
        if not sentences:
            continue
        sentence_texts = extract_sentences_from_segments(sentences)
        if not sentence_texts:
            continue
        
        # Log success message
        filename = os.path.basename(file_path)
        print(f"{filename}: {len(sentence_texts)} sentences extracted")

        for index, sentence in enumerate(sentence_texts):
            prev_sentence = sentence_texts[index - 1] if index > 0 else ""
            next_sentence = sentence_texts[index + 1] if index + 1 < len(sentence_texts) else ""
            corpus_entries.append({
                'prev': prev_sentence,
                'current': sentence,
                'next': next_sentence,
                'file': file_path
            })

    return jsonify({'sentences': corpus_entries})


@app.route('/get_files', methods=['GET'])
def get_files():
    file_list = []
    seen = set()        # overlapping library roots must not list (and search) a video twice
    for root in LIBRARY_ROOTS:
        for path, dirs, files in os.walk(root, followlinks=True):
            for file in files:
                if file.lower().endswith(VIDEO_EXTENSIONS):
                    full_path = os.path.join(path, file)
                    real_path = os.path.realpath(full_path)
                    if real_path not in seen:
                        seen.add(real_path)
                        file_list.append(full_path)
    file_list.sort(key=str.lower)
    return jsonify(file_list)


@app.route('/skip_segment', methods=['POST'])
def skip_segment():
    """
    Endpoint to signal that a segment should be skipped during processing.
    """
    data = request_json_object()
    search_id = data.get('search_id')
    segment_phrase = data.get('segment_phrase')
    
    if not isinstance(search_id, str) or not isinstance(segment_phrase, str) or not search_id or not segment_phrase:
        return jsonify({'error': 'Missing search_id or segment_phrase'}), 400
    
    # Only running searches have an entry: flags for finished searches would never be cleaned up.
    with SKIP_LOCK:
        if search_id not in SKIP_SEGMENTS:
            return jsonify({'error': 'This search is not running any more'}), 404
        SKIP_SEGMENTS[search_id].add(segment_phrase)
    
    print(f"[Skip] ⏭️  Marked segment for skip: '{segment_phrase[:50]}...' (search_id: {search_id})")
    return jsonify({'status': 'ok', 'skipped': segment_phrase})


def forget_search(search_id):
    """Drop the skip/cancel state of a search that has ended."""
    with SKIP_LOCK:
        SKIP_SEGMENTS.pop(search_id, None)
    with CANCEL_LOCK:
        CANCELLED_SEARCHES.discard(search_id)


@app.route('/cancel_search', methods=['POST'])
def cancel_search():
    """
    Endpoint to signal that an entire search should be cancelled immediately.
    """
    data = request_json_object()
    search_id = data.get('search_id')
    
    if not isinstance(search_id, str) or not search_id:
        return jsonify({'error': 'Missing search_id'}), 400
    
    # A cancel that arrives after its search has ended must not linger and abort a later search with the same id.
    with SKIP_LOCK:
        running = search_id in SKIP_SEGMENTS
    if not running:
        return jsonify({'error': 'This search is not running any more'}), 404
    with CANCEL_LOCK:
        CANCELLED_SEARCHES.add(search_id)
    
    print(f"[Cancel] ❌ Marked search for cancellation: {search_id}")
    return jsonify({'status': 'ok', 'cancelled': search_id})


def build_story_llm_prompt(user_prompt, selected_files, max_segments, prefer_long_segments=True):
    """
    Build the LLM prompt for story generation.
    Returns tuple: (llm_prompt, corpus_entries, corpus_text)
    """
    if prefer_long_segments:
        length_rule = ("4. Prefer using longer segments when they fit naturally, but you can break them apart and "
                       "recombine parts if needed for grammatical correctness.")
        length_hints = ("- Use entire segments as-is (prefer longer segments when possible)\n"
                        "- Combine parts from different segments\n"
                        "- Rearrange words/phrases from the corpus to create grammatically correct sentences\n"
                        "- Focus on using longer segments where they fit naturally\n"
                        "- Find a good balance between long segments and short segments to create a coherent story.")
    else:
        length_rule = ("4. Segments of any length are fine: break them apart and recombine parts freely if that "
                       "serves the story.")
        length_hints = ("- Use entire segments as-is\n"
                        "- Combine parts from different segments\n"
                        "- Rearrange words/phrases from the corpus to create grammatically correct sentences")
    # Load corpus from selected files
    corpus_entries = []
    
    for file_path in selected_files:
        if not isinstance(file_path, str) or not file_path.strip():
            continue
        
        sentences = load_transcript_sentences(file_path)
        if not sentences:
            continue
        
        sentence_texts = extract_sentences_from_segments(sentences)
        if not sentence_texts:
            continue
        
        # Store segments with metadata
        for sentence in sentence_texts:
            if not sentence or len(sentence.strip()) < 10:  # Skip very short segments
                continue
            
            corpus_entries.append({
                'text': sentence,
                'file': file_path,
                'word_count': len(sentence.split())
            })
    
    if not corpus_entries:
        return None, None, None
    
    # Group corpus entries by video file
    corpus_by_file = {}
    for entry in corpus_entries:
        file_path = entry['file']
        if file_path not in corpus_by_file:
            corpus_by_file[file_path] = []
        corpus_by_file[file_path].append(entry['text'])
    
    # Format corpus for LLM - each video's corpus on one line
    corpus_text = ""
    for file_path, segments in corpus_by_file.items():
        # Get just the filename for display
        filename = os.path.basename(file_path)
        # Join all segments from this video into one line
        video_corpus_line = " ".join(segments)
        corpus_text += f"{filename}: {video_corpus_line}\n"
    
    # Create prompt for LLM
    llm_prompt = f"""You are a video editor assistant. You have access to a corpus of video segments and need to create a story by creatively combining parts of these segments.

User's creative prompt: {user_prompt}

Available segments (for reference):
{corpus_text}

CRITICAL RULES - YOU MUST FOLLOW THESE:
1. Use ONLY words that appear in the corpus segments above. Do NOT invent, translate, or add any words that are not in the corpus.
2. Maintain the EXACT language of the corpus. If the corpus is in German, your sentences must be in German. If it's in English, use English. Do NOT translate.
3. You can rearrange and combine words/phrases from the corpus, but every word must come from the segments provided.
{length_rule}

Task: Create up to {max_segments} sentences that form a coherent story/narrative matching the user's prompt. You can:
{length_hints}

Important: Every single word in your sentences must exist in the corpus segments above. Do not translate, do not invent new words, do not change the language.

Respond with ONLY a JSON object in this exact format:
{{
  "explanation": "Brief explanation of the story you created (in the same language as the corpus)",
  "sentences": [
    {{
      "sentence": "Your created sentence here",
      "source_segments": ["original corpus segment 1", "original corpus segment 2", ...]
    }},
    ...
  ]
}}

IMPORTANT FORMATTING RULES:
- "sentence": The grammatically correct sentence you created
- "source_segments": An array of searchable parts that can be found in the corpus. When you rearrange words, split your sentence into parts that match the corpus exactly.
- Each part in source_segments must be an EXACT substring from the corpus (case-insensitive matching is OK, but word order must match)
- If you rearranged words, split the sentence into multiple parts that each exist in the corpus
- If you used a segment as-is, source_segments can be just that one segment
- The source_segments will be searched separately, so each part must be findable in the corpus

CRITICAL: When you rearrange words from the corpus, you MUST split your created sentence into parts that each exist as continuous substrings in the corpus.

Example: 
- Corpus contains: "in der vergangenen nacht waren in vielen teilen deutschlands wieder polarlichter zu sehen"
- You create the sentence: "In vielen Teilen Deutschlands waren wieder polarlichter zu sehen"
- You MUST provide source_segments as: ["in vielen teilen deutschlands", "waren", "wieder polarlichter zu sehen"]
- Notice: Each part is a continuous substring from the corpus, split where words were rearranged
- DO NOT provide unrelated corpus segments - only provide parts that are actually in your created sentence
- The parts will be searched separately, and the first match found will be used"""
    
    return llm_prompt, corpus_entries, corpus_text

def parse_story_response(llm_response, corpus_entries):
    """Read the LLM's story answer. Returns (explanation, sentence_queries, selected_indices, raw_sentences).

    ``sentence_queries`` is a list of (sentence, [corpus segments to search]). Models rarely follow the format
    to the letter, so this tolerates text around the JSON object, a single string instead of a list of
    segments, bare strings instead of objects and the old index-based format. Raises ValueError if there is no
    JSON object at all.
    """
    # The answer is the first object that has the list of sentences. Other objects are kept as a fallback:
    # an answer cut off by the token limit never closes its outer object, but the sentences before the cut
    # are complete objects of their own (as is an example object some models put in their preamble).
    story_data = None
    loose_sentences = []
    decoder = json.JSONDecoder()
    position = llm_response.find('{')
    found_object = False
    while position != -1:
        try:
            candidate, end = decoder.raw_decode(llm_response, position)
        except json.JSONDecodeError:
            candidate, end = None, position + 1
        if isinstance(candidate, dict):
            found_object = True
            if any(isinstance(candidate.get(key), list) for key in ('sentences', 'storyline', 'segments')):
                story_data = candidate
                break
            if 'sentence' in candidate or 'source_segments' in candidate:
                loose_sentences.append(candidate)
        else:
            end = position + 1
        position = llm_response.find('{', end)
    if story_data is None:
        if not found_object:
            raise ValueError('The LLM did not return a JSON object')
        story_data = {'sentences': loose_sentences}

    def as_text(value):
        return value.strip() if isinstance(value, str) else ''

    explanation = as_text(story_data.get('explanation'))
    raw_sentences = next((story_data[key] for key in ('sentences', 'storyline')
                          if isinstance(story_data.get(key), list)), [])
    selected_indices = None
    sentence_queries = []

    if not raw_sentences:
        # Legacy format: 1-based indices into the corpus
        indices = story_data.get('segments')
        if isinstance(indices, list) and indices:
            selected_indices = indices
            for index in indices:
                if isinstance(index, int) and not isinstance(index, bool) and 1 <= index <= len(corpus_entries):
                    sentence_text = corpus_entries[index - 1]['text']
                    sentence_queries.append((sentence_text, [sentence_text]))
    for item in raw_sentences:
        if isinstance(item, str):
            item = {'sentence': item}
        if not isinstance(item, dict):
            continue
        sentence_text = as_text(item.get('sentence'))
        segments = item.get('source_segments')
        if isinstance(segments, str):
            segments = [segments]
        if not isinstance(segments, list):
            segments = []
        search_queries = [as_text(segment) for segment in segments if as_text(segment)]
        if not sentence_text and search_queries:
            sentence_text = ' '.join(search_queries)
        if sentence_text:
            sentence_queries.append((sentence_text, search_queries or [sentence_text]))
    return explanation, sentence_queries, selected_indices, raw_sentences


@app.route('/parse_story_response', methods=['POST'])
def parse_story_response_route():
    """Read an LLM answer the user pasted, with the same tolerance as /generate_story."""
    data = request_json_object()
    text = data.get('response') if isinstance(data.get('response'), str) else ''
    try:
        explanation, sentence_queries, _indices, _raw = parse_story_response(text, [])
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    return jsonify({
        'explanation': explanation,
        'sentences': [{'sentence': sentence, 'queries': queries} for sentence, queries in sentence_queries],
    })


@app.route('/generate_story_prompt', methods=['POST'])
def generate_story_prompt():
    """
    Generate the LLM prompt without calling Ollama.
    Returns the prompt that would be sent to the LLM.
    """
    data = request_json_object()
    prompt = data.get('prompt') if isinstance(data.get('prompt'), str) else ''
    prompt = prompt.strip()
    selected_files = library_files_only(data.get('files'))
    max_segments = max(1, min(50, sanitize_int(data.get('maxSegments'), 10)))
    
    if not prompt:
        return jsonify({'error': 'No prompt provided'}), 400
    
    if not selected_files:
        return jsonify({'error': missing_files_note(data.get('files'), selected_files) or 'No files selected'}), 400
    
    llm_prompt, corpus_entries, corpus_text = build_story_llm_prompt(
        prompt, selected_files, max_segments, prefer_long_segments=data.get('preferLongSegments', True) is not False)
    
    if llm_prompt is None:
        return jsonify({'error': 'No corpus data found in selected files'}), 400
    
    return jsonify({'prompt': llm_prompt})

@app.route('/generate_story', methods=['POST'])
def generate_story():
    """
    Use Ollama LLM to generate a story by selecting segments from the corpus.

    The client may name a ``search_id``; /cancel_search with that id stops the work after the LLM has answered
    (the LLM request itself cannot be interrupted) instead of exporting clips nobody waits for.
    """
    data = request_json_object()
    search_id = data.get('search_id') if isinstance(data.get('search_id'), str) and data.get('search_id') else None
    if search_id:
        with SKIP_LOCK:
            SKIP_SEGMENTS[search_id] = set()
        with CANCEL_LOCK:
            CANCELLED_SEARCHES.discard(search_id)
    try:
        return _generate_story(data, search_id)
    except SearchCancelled:
        print("[Story] ❌ Cancelled by the user")
        return jsonify({'cancelled': True, 'segments': [], 'unmatched': []})
    finally:
        if search_id:
            forget_search(search_id)


def _generate_story(data, search_id):
    import requests

    def stop_if_cancelled():
        if search_id:
            with CANCEL_LOCK:
                if search_id in CANCELLED_SEARCHES:
                    raise SearchCancelled()

    prompt = data.get('prompt') if isinstance(data.get('prompt'), str) else ''
    prompt = prompt.strip()
    selected_files = library_files_only(data.get('files'))
    max_segments = max(1, min(50, sanitize_int(data.get('maxSegments'), 10)))
    prefer_long_segments = data.get('preferLongSegments', True)
    debug_mode = data.get('debugMode', False)  # Enable debug logging
    
    if not prompt:
        return jsonify({'error': 'No prompt provided'}), 400
    
    if not selected_files:
        return jsonify({'error': missing_files_note(data.get('files'), selected_files) or 'No files selected'}), 400
    
    print(f"[Story] Generating story with prompt: '{prompt[:100]}...'")
    print(f"[Story] Max segments: {max_segments}, Prefer long: {prefer_long_segments}")
    
    # Build LLM prompt using shared function
    llm_prompt, corpus_entries, corpus_text = build_story_llm_prompt(
        prompt, selected_files, max_segments, prefer_long_segments=prefer_long_segments is not False)
    
    if llm_prompt is None:
        return jsonify({'error': 'No corpus data found in selected files'}), 400
    
    print(f"[Story] Loaded {len(corpus_entries)} segments from {len(selected_files)} files")
    
    # Group corpus entries by video file for logging
    corpus_by_file = {}
    for entry in corpus_entries:
        file_path = entry['file']
        if file_path not in corpus_by_file:
            corpus_by_file[file_path] = []
        corpus_by_file[file_path].append(entry['text'])
    
    print(f"[Story] Sending full corpus from {len(corpus_by_file)} files to LLM")

    # Debug: Log full prompt sent to LLM
    if debug_mode:
        print("\n" + "="*80)
        print("[DEBUG] FULL PROMPT SENT TO LLM:")
        print("="*80)
        print(llm_prompt)
        print("="*80 + "\n")

    print(f"[Story] Sending request to Ollama (corpus: {len(corpus_entries)} segments)")
    
    # Call Ollama API
    try:
        ollama_url = OLLAMA_URL
        ollama_payload = {
            "model": OLLAMA_MODEL,
            "prompt": llm_prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                # Room for the requested number of sentences (a fixed limit cut long stories off mid-JSON)
                "num_predict": 300 + 150 * max_segments
            }
        }
        
        response = requests.post(ollama_url, json=ollama_payload, timeout=OLLAMA_TIMEOUT)
        response.raise_for_status()
        
        result = response.json()
        llm_response = result.get('response', '')
        
        stop_if_cancelled()
        print(f"[Story] Received LLM response ({len(llm_response)} chars)")
        print(f"[Story] LLM response: {llm_response}")
        
        # Debug: Log full LLM response
        if debug_mode:
            print("\n" + "="*80)
            print("[DEBUG] FULL LLM RESPONSE:")
            print("="*80)
            print(llm_response)
            print("="*80 + "\n")
        
        try:
            explanation, sentence_queries, selected_indices, llm_sentences_data = parse_story_response(
                llm_response, corpus_entries)
        except ValueError as exc:
            print(f"[Story] ❌ {exc}: '{llm_response[:300]}'")
            return jsonify({'error': f'{exc}. Response: {llm_response[:200]}'}), 502

        if not sentence_queries:
            return jsonify({'error': 'The LLM did not provide any usable sentences'}), 502
        
        print(f"[Story] LLM created {len(sentence_queries)} sentences:")
        for i, (sentence_text, search_queries) in enumerate(sentence_queries, 1):
            print(f"  {i}. '{sentence_text[:80]}...'")
            if len(search_queries) > 1 or (len(search_queries) == 1 and search_queries[0] != sentence_text):
                print(f"      (searching: {', '.join([q[:40] + '...' if len(q) > 40 else q for q in search_queries])})")
        
        # Now search for each sentence using its source_segments
        # This uses the sentence search without partial matches to find exact matches
        story_results = []
        unmatched_segments = []
        story_counter = 0  # Counter for story generation
        
        for sentence_text, search_queries in sentence_queries:
            if not sentence_text:
                continue
            
            print(f"[Story] Searching for sentence {len(story_results) + 1}/{len(sentence_queries)}: '{sentence_text[:50]}...'")
            print(f"[Story]   Using {len(search_queries)} search query/queries from source segments")
            
            # Search for ALL source segments and create one card per search segment
            try:
                for search_query in search_queries:
                    if not search_query:
                        continue
                    
                    print(f"[Story]   Searching for: '{search_query[:60]}...'")
                    
                    # Use find_and_export_longest_matches_incremental to search and export
                    # This searches across all selected files and handles all export logic
                    for result in find_and_export_longest_matches_incremental(
                        search_query,
                        selected_files,  # Search across all selected files
                        story_counter,
                        export_clips=True,
                        # One group per searched segment: with a shared group every segment of a
                        # sentence was exported to the same clip file and all cards played the last one.
                        clip_group=generate_clip_group_name('story', search_query, selected_files),
                        min_silence=0.0,
                        max_silence=10.0,
                        silence_word_threshold=2,
                        search_id=search_id,  # lets Cancel stop the exports
                        include_partial_matches=False,  # No partial matches - exact match only
                        all_partial_matches=False,  # No partial matches for story generation
                        max_results_per_segment=1  # Only need first match per segment
                    ):
                        # Filter out progress messages and skip notifications
                        # Only process actual segment results (those with 'files' key)
                        if not result or not isinstance(result, dict):
                            continue
                        
                        # Skip progress messages
                        if 'progress' in result:
                            continue
                        
                        # Skip skipped segments
                        if result.get('skipped'):
                            continue
                        
                        # Only process actual segment results with files
                        if not result.get('files'):
                            continue
                        
                        # Add source_video field for display (use first file's source)
                        for file_entry in result['files']:
                            if isinstance(file_entry, dict) and 'source_video' not in file_entry:
                                # Try to extract source from file path
                                file_path = file_entry.get('file', '')
                                if file_path:
                                    # Find matching source file
                                    for src_file in selected_files:
                                        if src_file in file_path or os.path.basename(src_file) in file_path:
                                            file_entry['source_video'] = src_file
                                            break
                        
                        # Create one card per search segment (not combined)
                        # The phrase shows the search query (the segment that was searched)
                        result['phrase'] = search_query
                        result['created_sentence'] = sentence_text  # Store the created sentence for reference
                        
                        story_results.append(result)
                        story_counter += 1
                        print(f"[Story] ✓ Found and exported {len(result['files'])} clip(s) for search query: '{search_query[:50]}...'")
                        break  # Only take first result from this search_query
                    else:
                        # The LLM used words that are not spoken (in this order) in the selected videos
                        unmatched_segments.append(search_query)

            except SearchCancelled:
                raise
            except Exception as e:
                print(f"[Story] Error searching for sentence '{sentence_text[:50]}...': {e}")
                traceback.print_exc()
                continue
        
        print(f"[Story] ✓ Generated story with {len(story_results)} segments")
        
        response_data = {
            'segments': story_results,
            'story_explanation': explanation,
            'unmatched': unmatched_segments
        }
        
        # Include debug information if debug mode is enabled
        if debug_mode:
            # Format sentence_queries for debug display
            debug_sentences = []
            for sentence_text, search_queries in sentence_queries:
                debug_sentences.append({
                    'sentence': sentence_text,
                    'search_queries': search_queries
                })
            
            debug_info = {
                'corpus': corpus_text,
                'prompt': llm_prompt,
                'llm_response': llm_response,
                'corpus_entry_count': len(corpus_entries),
                'sentence_queries': debug_sentences  # Shows both created sentence and search queries used
            }
            # Include selected_indices if it was used (for backward compatibility)
            if selected_indices is not None:
                debug_info['selected_indices'] = selected_indices
            # Include old format for backward compatibility
            if llm_sentences_data and isinstance(llm_sentences_data[0], str):
                debug_info['llm_sentences'] = llm_sentences_data
            response_data['debug'] = debug_info
        
        return jsonify(response_data)
        
    except requests.exceptions.Timeout:
        print(f"[Story] ❌ Ollama did not answer within {OLLAMA_TIMEOUT}s")
        return jsonify({'error': f'The LLM at {OLLAMA_URL} did not answer within {OLLAMA_TIMEOUT} s. '
                                 f'Try fewer source files or a faster model.'}), 504
    except requests.exceptions.ConnectionError as e:
        print(f"[Story] ❌ Cannot reach Ollama: {e}")
        return jsonify({'error': f'Cannot reach the LLM server at {OLLAMA_URL}. Start Ollama or set '
                                 f'--ollama-url / OLLAMA_URL.'}), 502
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 'unknown'
        print(f"[Story] ❌ Ollama API error: {e}")
        return jsonify({'error': f'The LLM server answered with HTTP {status}. Is the model "{OLLAMA_MODEL}" '
                                 f'installed (ollama pull)?'}), 502
    except requests.exceptions.RequestException as e:
        print(f"[Story] ❌ Ollama API error: {e}")
        return jsonify({'error': f'LLM request failed: {str(e)}'}), 502
    except SearchCancelled:
        raise
    except Exception as e:
        print(f"[Story] ❌ Error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/search_longest_segments_stream', methods=['POST'])
def search_longest_segments_stream():
    """
    SSE endpoint that streams search results incrementally, one segment at a time.
    Handles multiple comma-separated search queries by searching each one separately.
    """
    data = request_json_object()
    counter = sanitize_int(data.get('sentenceSearchCounter'), 0)
    selected_files = library_files_only(data.get('files'))

    # One search per group. The client splits its input on ';'; a bare `sentence` is searched as a whole
    # (splitting it on commas cut story segments such as "Well, I think so" into fragments).
    sentence_groups = data.get('sentence_groups')
    if not isinstance(sentence_groups, list):
        sentence_groups = [data.get('sentence')]
    sentence_groups = [group.strip() for group in sentence_groups if isinstance(group, str) and group.strip()]

    files_note = missing_files_note(data.get('files'), selected_files)
    if not sentence_groups or not selected_files:
        events = [{'warning': files_note}] if files_note else []
        events.append({'done': True, 'total_segments': 0, 'total_skipped': 0})
        return Response(''.join(f'data: {json.dumps(event)}\n\n' for event in events), mimetype='text/event-stream')

    min_silence, max_silence = extract_silence_preferences(data)
    silence_word_threshold = sanitize_int(data.get('silenceWordThreshold'), 2)
    include_partial_matches = bool(data.get('includePartialMatches', False))
    all_partial_matches = bool(data.get('allPartialMatches', False))
    max_results_per_segment = clamp_max_results(data.get('maxResultsPerSegment'))
    
    def generate():
        total_segment_count = 0
        total_skipped_count = 0
        if files_note:
            yield f'data: {json.dumps({"warning": files_note})}\n\n'
        
        # Process each sentence group separately
        for group_index, sentence in enumerate(sentence_groups, 1):
            if not sentence:
                continue
                
            clip_group = generate_clip_group_name('sentence', sentence, selected_files)
            # Unique per run: two identical searches (two tabs, a quick repeat) must not share skip/cancel state
            search_id = f"{counter}_{clip_group}_{uuid.uuid4().hex[:8]}"
            
            # Registered while the search runs: /skip_segment and /cancel_search only accept running searches
            with SKIP_LOCK:
                SKIP_SEGMENTS[search_id] = set()
            
            if not include_partial_matches:
                mode_str = "full match only"
            elif all_partial_matches:
                mode_str = "all partial matches"
            else:
                mode_str = "partial matches (filtered short ones)"
                
            print(f"[SSE] [{group_index}/{len(sentence_groups)}] Starting search for: '{sentence}' across {len(selected_files)} file(s) ({mode_str}, search_id: {search_id})")
            
            segment_count = 0
            skipped_count = 0

            # Tell the client the id right away: without it, Cancel cannot reach the server during the
            # matching phase (which reads every transcript on the first search after a start).
            try:
                yield 'data: ' + json.dumps({
                    'progress': f"Looking for '{sentence}'...", 'search_id': search_id,
                    'group_index': group_index, 'total_groups': len(sentence_groups),
                }) + '\n\n'
            except (GeneratorExit, BrokenPipeError, ConnectionError):
                forget_search(search_id)
                return

            try:
                # Use the generator version that yields results per segment
                for result in find_and_export_longest_matches_incremental(
                    sentence,
                    selected_files,
                    counter,
                    export_clips=True,
                    clip_group=clip_group,
                    min_silence=min_silence,
                    max_silence=max_silence,
                    silence_word_threshold=max(silence_word_threshold, 1),
                    search_id=search_id,
                    include_partial_matches=include_partial_matches,
                    all_partial_matches=all_partial_matches,
                    max_results_per_segment=max_results_per_segment
                ):
                    try:
                        # Update progress with group information
                        if 'progress' in result:
                            # Add group information to progress updates
                            result['group_index'] = group_index
                            result['total_groups'] = len(sentence_groups)
                            result['search_id'] = search_id
                            print(f"[SSE] [{group_index}/{len(sentence_groups)}] Progress: {result.get('segment_index', '?')}/{result.get('total_segments', '?')}")
                        elif 'skipped' in result and result['skipped']:
                            skipped_count += 1
                            total_skipped_count += 1
                            print(f"[SSE] [{group_index}/{len(sentence_groups)}] ⏭️  Segment skipped: '{result.get('phrase', '')[:50]}...'")
                        elif 'phrase' in result:
                            segment_count += 1
                            total_segment_count += 1
                            files_count = len(result.get('files', []))
                            print(f"[SSE] [{group_index}/{len(sentence_groups)}] Sending result: '{result['phrase'][:50]}...' ({files_count} clips)")
                        
                        # Add group information to the result
                        result['group_index'] = group_index
                        result['total_groups'] = len(sentence_groups)
                        
                        # Send each segment result as an SSE event
                        yield f"data: {json.dumps(result)}\n\n"
                    except (GeneratorExit, BrokenPipeError, ConnectionError) as e:
                        print(f"[SSE] ❌ CANCELLED BY CLIENT after {total_segment_count} segments sent ({type(e).__name__})")
                        return
                
                print(f"[SSE] [{group_index}/{len(sentence_groups)}] ✓ Search completed - {segment_count} segments, {skipped_count} skipped")
                if segment_count == 0 and skipped_count == 0:
                    try:
                        yield 'data: ' + json.dumps({
                            'no_match': True, 'phrase': sentence,
                            'group_index': group_index, 'total_groups': len(sentence_groups),
                        }) + '\n\n'
                    except (GeneratorExit, BrokenPipeError, ConnectionError):
                        return
                
            except SearchCancelled:
                print(f"[SSE] ❌ Search cancelled by the user after {total_segment_count} segments")
                try:
                    yield f'data: {json.dumps({"done": True, "cancelled": True, "total_segments": total_segment_count, "total_skipped": total_skipped_count})}\n\n'
                except (GeneratorExit, BrokenPipeError, ConnectionError):
                    pass
                return
            except Exception as e:
                print(f"[SSE] ❌ Error in search for '{sentence}': {e}")
                traceback.print_exc()
                try:
                    error_msg = f"Error searching for '{sentence}': {str(e)}"
                    yield f'data: {json.dumps({"error": error_msg, "group_index": group_index, "total_groups": len(sentence_groups)})}\n\n'
                except (GeneratorExit, BrokenPipeError, ConnectionError):
                    print(f"[SSE] ❌ Client disconnected while sending error")
                    return
            finally:
                forget_search(search_id)
        
        # Signal completion of all searches
        print(f"[SSE] ✓ All searches completed - Total: {total_segment_count} segments, {total_skipped_count} skipped")
        yield 'data: {"done": true, "total_segments": ' + str(total_segment_count) + ', "total_skipped": ' + str(total_skipped_count) + '}\n\n'
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/search_silences_stream', methods=['POST'])
def search_silences_stream():
    """
    SSE endpoint that streams silence search results incrementally, one file at a time.
    """
    data = request_json_object()
    selected_files = library_files_only(data.get('files'))
    files_note = missing_files_note(data.get('files'), selected_files)
    if not selected_files:
        events = [{'warning': files_note}] if files_note else []
        events.append({'done': True, 'total_silences': 0})
        return Response(''.join(f'data: {json.dumps(event)}\n\n' for event in events), mimetype='text/event-stream')

    min_silence = sanitize_float(data.get('minSilence'), 0.0)
    max_silence = sanitize_float(data.get('maxSilence'), 10.0)
    if max_silence < min_silence:
        max_silence = min_silence

    max_results_per_segment = clamp_max_results(data.get('maxResultsPerSegment'))

    print(f"[Silence SSE] Searching for silences between {min_silence:.3f}s and {max_silence:.3f}s across {len(selected_files)} file(s)")

    def generate():
        if files_note:
            yield f'data: {json.dumps({"warning": files_note})}\n\n'
        # Collect all silence matches first (across all files)
        all_silence_entries = []
        
        for file_index, filename in enumerate(selected_files, 1):
            try:
                sentences = load_transcript_sentences(filename)
                if not sentences:
                    print(f"[Silence SSE] No transcript data for '{filename}', skipping")
                    continue

                all_words = []
                for sentence in sentences:
                    for word in sentence.get('words', []):
                        start_time = word.get('start')
                        end_time = word.get('end')
                        if start_time is None or end_time is None:
                            continue
                        all_words.append({
                            'word': word.get('word', ''),
                            'start': float(start_time),
                            'end': float(end_time)
                        })

                if len(all_words) < 2:
                    continue

                all_words.sort(key=lambda w: w['start'])
                video_name = os.path.basename(filename)

                # Collect all candidate silences for this file
                per_file_silences = []
                for i in range(len(all_words) - 1):
                    prev_word = all_words[i]
                    next_word = all_words[i + 1]
                    gap = max(0.0, float(next_word['start']) - float(prev_word['end']))

                    if min_silence <= gap <= max_silence:
                        silence_start = float(prev_word['end'])
                        silence_end = float(next_word['start'])
                        per_file_silences.append({
                            'file': filename,
                            'video_name': video_name,
                            'silence_start': silence_start,
                            'silence_end': silence_end,
                            'gap': gap,
                            'word_before': prev_word.get('word', ''),
                            'word_after': next_word.get('word', ''),
                        })

                all_silence_entries.extend(per_file_silences)
                print(f"[Silence SSE] [{file_index}/{len(selected_files)}] Found {len(per_file_silences)} matches in {video_name}")

            except Exception as e:
                print(f"[Silence SSE] Error processing file '{filename}': {e}")
                traceback.print_exc()
                continue

        if not all_silence_entries:
            yield 'data: {"done": true, "total_silences": 0}\n\n'
            return

        # Sort all silences by gap duration (longest first) and limit to max_results_per_segment GLOBALLY
        all_silence_entries.sort(key=lambda e: -e['gap'])
        all_silence_entries = all_silence_entries[:max_results_per_segment]
        
        print(f"[Silence SSE] Processing top {len(all_silence_entries)} silences across all files (max={max_results_per_segment})")

        # Export clips and stream results incrementally
        clip_group_name = generate_clip_group_name('silence', f"{min_silence:.2f}-{max_silence:.2f}", selected_files)
        clip_output_dir_name = f"{clip_group_name}_clips"
        clip_output_dir = os.path.join(TEMP_DIR, clip_output_dir_name)
        os.makedirs(clip_output_dir, exist_ok=True)

        # Group silences by duration buckets for display
        buckets = {}
        source_durations = {}
        for idx, entry in enumerate(all_silence_entries):
            filename = entry['file']
            silence_start = entry['silence_start']
            silence_end = entry['silence_end']
            gap = entry['gap']

            try:
                if not os.path.exists(filename):
                    print(f"[Silence SSE] Source video not found: {filename}")
                    continue

                if filename not in source_durations:
                    source_durations[filename] = get_video_duration(filename)
                video_duration = source_durations[filename]
                if video_duration is None:
                    print(f"[Silence SSE] Could not determine video duration for: {filename}")
                    continue

                clip_start = max(silence_start, 0.0)
                clip_end = min(silence_end, video_duration)
                if clip_end <= clip_start:
                    print(f"[Silence SSE] Invalid clip range for silence: start={clip_start}, end={clip_end}, video_duration={video_duration}")
                    continue

                clip_filename = clip_file_name(clip_group_name, filename, clip_start, clip_end)
                output_path = os.path.join(clip_output_dir, clip_filename)
                try:
                    render_preview_clip(filename, clip_start, clip_end, output_path)
                except RuntimeError as exc:
                    print(f"[Silence SSE] Could not export silence {idx} of '{filename}': {exc}")
                    continue

                relative_path = posixpath.join('temp', clip_output_dir_name, clip_filename)
                clip_duration_ms = round((clip_end - clip_start) * 1000)

                # Determine bucket label
                duration = gap
                if duration < 1:
                    label = '<1s silences'
                else:
                    s = int(duration)
                    upper = s + 1
                    label = f'{s}-{upper}s silences'

                # Initialize bucket if needed
                if label not in buckets:
                    buckets[label] = {
                        'phrase': label,
                        'files': []
                    }

                # Check if bucket is full
                if len(buckets[label]['files']) >= max_results_per_segment:
                    continue

                file_data = {
                    'file': relative_path,
                    'source_video': filename,
                    'silence_start': silence_start,
                    'silence_end': silence_end,
                    'original_start': clip_start,  # Original segment start in seconds
                    'original_end': clip_end,  # Original segment end in seconds
                    'word_before': entry['word_before'],
                    'word_after': entry['word_after'],
                }
                if clip_duration_ms is not None:
                    file_data['duration_ms'] = clip_duration_ms

                buckets[label]['files'].append(file_data)

                # Stream only the new clip: the client appends it to the bucket's card. (Re-sending the
                # whole bucket after every clip was quadratic in the number of results.)
                result = {
                    'phrase': label,
                    'files': [file_data],
                    'word_count': 0  # Silences don't have word count
                }
                
                try:
                    yield f'data: {json.dumps(result)}\n\n'
                except (GeneratorExit, BrokenPipeError, ConnectionError):
                    print(f"[Silence SSE] ❌ Client disconnected")
                    return

            except Exception as exc:
                print(f"[Silence SSE] Error exporting silence clip for '{filename}': {exc}")
                traceback.print_exc()
                continue

        # Signal completion
        total_silences = sum(len(bucket['files']) for bucket in buckets.values())
        print(f"[Silence SSE] ✓ Search completed - {total_silences} silences in {len(buckets)} buckets")
        yield f'data: {{"done": true, "total_silences": {total_silences}, "total_buckets": {len(buckets)}}}\n\n'

    return Response(stream_with_context(generate()), mimetype='text/event-stream')




# Phones type ’ and –, transcripts usually contain ' and -: treat them as the same character when matching.
_TOKEN_PUNCTUATION = str.maketrans({
    '\u2019': "'", '\u2018': "'", '\u02bc': "'", '\u00b4': "'", '`': "'",
    '\u2010': '-', '\u2011': '-', '\u2012': '-', '\u2013': '-', '\u2014': '-',
    '\u00ad': None,
})


def normalize_token(token):
    if not isinstance(token, str):
        return ''
    normalized = unicodedata.normalize('NFKC', token).casefold().translate(_TOKEN_PUNCTUATION).strip()
    if not normalized:
        return ''
    normalized = re.sub(r'^[\W_]+', '', normalized)
    normalized = re.sub(r'[\W_]+$', '', normalized)
    return normalized


def slugify_text(text, max_length=40):
    if not isinstance(text, str):
        return 'entry'
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    normalized = re.sub(r'[^a-z0-9]+', '-', normalized.lower()).strip('-')
    if not normalized:
        normalized = 'entry'
    if len(normalized) > max_length:
        normalized = normalized[:max_length].rstrip('-')
    return normalized or 'entry'


def generate_clip_group_name(prefix, phrase, filenames):
    slug = slugify_text(f"{prefix}-{phrase}")
    phrase_hash_source = phrase.lower() if isinstance(phrase, str) else ''
    phrase_hash = hashlib.md5(phrase_hash_source.encode('utf-8')).hexdigest()[:8] if phrase_hash_source else 'nophrase'
    file_hash_source = '\n'.join(sorted(map(str, filenames))) if filenames else ''
    file_hash = hashlib.md5(file_hash_source.encode('utf-8')).hexdigest()[:8] if file_hash_source else 'nofiles'
    return f"{slug}-{phrase_hash}-{file_hash}"


def sanitize_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def sanitize_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def get_video_duration(video_path):
    """Get the duration of a video file in seconds using ffprobe."""
    if not os.path.exists(video_path):
        return None
    try:
        # Argument list, no shell: file names may contain $, quotes or backticks.
        duration_command = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                            '-of', 'default=noprint_wrappers=1:nokey=1', video_path]
        duration_result = subprocess.run(duration_command, capture_output=True, text=True, timeout=10)
        if duration_result.returncode == 0 and duration_result.stdout.strip():
            return float(duration_result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError, subprocess.SubprocessError, OSError) as e:
        print(f"Error getting video duration for {video_path}: {e}")
    return None


def get_video_duration_ms(video_path):
    """Get the duration of a video file in milliseconds using ffprobe."""
    duration_sec = get_video_duration(video_path)
    if duration_sec is not None:
        return int(round(duration_sec * 1000))
    return None


def get_video_fps(video_path):
    """Frame rate of a video as a Fraction, or None if unknown.

    Variable-rate files (phones, screen recordings) report a timebase such as 90000/1 as their "rate"; the probe
    falls back to the average rate for those, so clips are not encoded with thousands of duplicated frames.
    """
    try:
        return cached_probe(video_path).fps
    except (render_pipeline.RenderError, OSError):
        return None


def extract_silence_preferences(payload, default_min=0.0, default_max=10.0):
    min_silence = sanitize_float(payload.get('minSilence', default_min), default_min)
    max_silence = sanitize_float(payload.get('maxSilence', default_max), default_max)
    if max_silence < min_silence:
        max_silence = min_silence
    return min_silence, max_silence


_TRANSCRIPT_EXTENSIONS = ('.json', '.vtt', '.srt')
_LANGUAGE_TAG = re.compile(r'[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})?')
_DIRECTORY_TRANSCRIPTS = {}         # folder -> (mtime_ns, {video stem: [transcript file names]})
_DIRECTORY_TRANSCRIPTS_LOCK = Lock()


def _directory_transcripts(directory: str) -> Dict[str, List[str]]:
    """Transcript files of a folder by the video name they belong to; re-read when the folder changes.

    "<name>.<ext>" comes first, then "<name>.<language>.<ext>" (yt-dlp and videogrep write those). Nothing else
    counts: "talk.part2.json" is not a transcript of "talk.mp4".
    """
    try:
        stamp = os.stat(directory or '.').st_mtime_ns
    except OSError:
        return {}
    with _DIRECTORY_TRANSCRIPTS_LOCK:
        cached = _DIRECTORY_TRANSCRIPTS.get(directory)
    if cached and cached[0] == stamp:
        return cached[1]
    index: Dict[str, List[str]] = {}
    try:
        names = sorted(os.listdir(directory or '.'))
    except OSError:
        names = []
    for name in names:
        base, extension = os.path.splitext(name)
        if extension.lower() not in _TRANSCRIPT_EXTENSIONS:
            continue
        index.setdefault(base, []).insert(0, name)
        stem, dot, tag = base.rpartition('.')
        if dot and _LANGUAGE_TAG.fullmatch(tag):
            index.setdefault(stem, []).append(name)
    with _DIRECTORY_TRANSCRIPTS_LOCK:
        if len(_DIRECTORY_TRANSCRIPTS) > 512:
            _DIRECTORY_TRANSCRIPTS.clear()
        _DIRECTORY_TRANSCRIPTS[directory] = (stamp, index)
    return index


def _find_related_files(video_path: str, extensions: Sequence[str]) -> List[Path]:
    """The video's transcript files with one of ``extensions``, in order of preference."""
    source_path = Path(video_path)
    names = _directory_transcripts(str(source_path.parent)).get(source_path.stem, [])
    wanted = [ext if ext.startswith('.') else f'.{ext}' for ext in extensions]
    return [source_path.parent / name for ext in wanted for name in names if name.lower().endswith(ext)]


def _load_json_transcript(path: Path) -> Optional[List[dict]]:
    try:
        with path.open('r', encoding='utf-8') as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Failed to load JSON transcript '{path}': {exc}")
        return None
    if not isinstance(data, list):
        return None
    return data


class WordIndex:
    """All timed words of a transcript in spoken order, plus a token -> positions lookup for matching."""

    __slots__ = ('words', 'positions')

    def __init__(self, sentences):
        self.words = []
        self.positions = {}
        for sentence in sentences or []:
            if not isinstance(sentence, dict):
                continue
            for word in sentence.get('words') or []:
                if not isinstance(word, dict):
                    continue
                start_time = word.get('start')
                end_time = word.get('end')
                token = normalize_token(word.get('word', ''))
                if token and start_time is not None and end_time is not None:
                    self.positions.setdefault(token, []).append(len(self.words))
                    self.words.append({
                        'word': word.get('word', ''),
                        'normalized_word': token,
                        'start': start_time,
                        'end': end_time
                    })


_TRANSCRIPT_CACHE = OrderedDict()   # video path -> (signature, sentences, WordIndex)
_TRANSCRIPT_CACHE_LOCK = Lock()
TRANSCRIPT_CACHE_FILES = 512


def _transcript_signature(video_path):
    """Identity of the transcript/subtitle files next to a video (None while there are none)."""
    signature = []
    for candidate in _find_related_files(video_path, _TRANSCRIPT_EXTENSIONS):
        try:
            stat = candidate.stat()
        except OSError:
            continue
        signature.append((candidate.name, stat.st_mtime_ns, stat.st_size))
    return tuple(signature) or None


def _cached_transcript(video_path):
    """(sentences, WordIndex) for a video, re-read only when its transcript file changes."""
    signature = _transcript_signature(video_path)
    with _TRANSCRIPT_CACHE_LOCK:
        cached = _TRANSCRIPT_CACHE.get(video_path)
        if cached and signature is not None and cached[0] == signature:
            _TRANSCRIPT_CACHE.move_to_end(video_path)
            return cached[1], cached[2]
    sentences = _read_transcript_sentences(video_path)
    if not sentences:
        return None, None
    index = WordIndex(sentences)
    signature = _transcript_signature(video_path)   # a subtitle conversion may just have written the JSON
    if signature is not None:
        with _TRANSCRIPT_CACHE_LOCK:
            _TRANSCRIPT_CACHE[video_path] = (signature, sentences, index)
            _TRANSCRIPT_CACHE.move_to_end(video_path)
            while len(_TRANSCRIPT_CACHE) > TRANSCRIPT_CACHE_FILES:
                _TRANSCRIPT_CACHE.popitem(last=False)
    return sentences, index


def _has_own_transcript(video_path):
    """True if videogrep's lookup finds a transcript that belongs to this video.

    videogrep matches by prefix, so "talk.mp4" would pick up "talk.part2.json" of another video and be cut at
    that video's timestamps. Only "<name>.<ext>" and "<name>.<language>.<ext>" count.
    """
    try:
        found = find_transcript(video_path)
    except Exception:
        return False
    if not found:
        return False
    stem = Path(video_path).stem
    name = Path(found).name
    if not name.startswith(stem + '.'):
        return False
    return re.fullmatch(r'(?:[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})?\.)?(?:json|vtt|srt|transcript)', name[len(stem) + 1:]) is not None


def load_transcript_sentences(video_path: str) -> Optional[List[dict]]:
    """Transcript segments of a video (cached; callers must not modify the returned list)."""
    return _cached_transcript(video_path)[0]


def load_word_index(video_path: str) -> Optional[WordIndex]:
    return _cached_transcript(video_path)[1]


def _read_transcript_sentences(video_path: str) -> Optional[List[dict]]:
    # First, try to find and load JSON files (preferred format)
    json_candidates = _find_related_files(video_path, ['.json'])
    for json_path in json_candidates:
        sentences = _load_json_transcript(json_path)
        if sentences:
            return sentences
    
    # No JSON: videogrep's own transcript formats. Sentence-level text without word timings can feed the
    # corpus but cannot be searched, so keep looking for something better before settling for it.
    text_only = None
    try:
        sentences = parse_transcript(video_path) if _has_own_transcript(video_path) else None
        if sentences and extract_sentences_from_segments(sentences):
            if any(isinstance(segment, dict) and segment.get('words') for segment in sentences):
                return sentences
            text_only = sentences
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"Failed to parse transcript via videogrep for '{video_path}': {exc}")

    for subtitle_path in _find_related_files(video_path, ['.vtt', '.srt']):
        output_path = Path(video_path).with_suffix('.json')
        try:
            # Subtitles with inline word timestamps (YouTube VTT) are converted once and stored as JSON
            segments, _stats = convert_subtitles(subtitle_path, output_path)
        except ValueError:
            try:
                # Plain subtitles: estimate word times inside each cue. Good enough to find a phrase (the
                # clip is padded and trimmed by hand anyway); kept in memory so a real transcript can replace it.
                segments, _stats = convert_subtitles(subtitle_path, None, interpolate=True)
                print(f"Using estimated word timings from '{subtitle_path.name}' (no word-level transcript)")
            except Exception as exc:
                print(f"Failed to convert subtitles '{subtitle_path}' for '{video_path}': {exc}")
                continue
        except Exception as exc:
            print(f"Failed to convert subtitles '{subtitle_path}' for '{video_path}': {exc}")
            continue
        if segments:
            return segments

    if text_only:
        return text_only
    print(f"No subtitle data available for '{video_path}'")
    return None


def normalize_sentence_text(text: str) -> str:
    if not isinstance(text, str):
        return ''
    normalized = unicodedata.normalize('NFKC', text)
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()


def extract_sentences_from_segments(segments: Sequence[dict]) -> List[str]:
    sentences_list: List[str] = []
    previous_lower = None

    for segment in segments:
        if not isinstance(segment, dict):
            continue

        sentence_text = ''
        content = segment.get('content')
        if isinstance(content, str) and content.strip():
            sentence_text = normalize_sentence_text(content)
        elif isinstance(segment.get('words'), list):
            words = []
            for word_entry in segment['words']:
                if not isinstance(word_entry, dict):
                    continue
                word = word_entry.get('word')
                if isinstance(word, str) and word.strip():
                    words.append(word.strip())
            sentence_text = normalize_sentence_text(' '.join(words))

        if not sentence_text:
            continue

        sentence_lower = sentence_text.lower()
        if previous_lower == sentence_lower:
            continue

        previous_lower = sentence_lower
        sentences_list.append(sentence_text)

    return sentences_list


def _merge_output_name(titles):
    """Timestamped, filesystem-safe file name built from the first words of the timeline."""
    words = ' '.join(str(title or '') for title in titles).replace('_', ' ').split()[:5]
    readable = unicodedata.normalize('NFKD', '_'.join(words).replace('ß', 'ss')).encode('ascii', 'ignore').decode('ascii')
    name_part = re.sub(r'[^a-zA-Z0-9_-]', '_', readable)
    name_part = re.sub(r'_+', '_', name_part).strip('_')[:100] or 'merge'
    return f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{name_part}.mp4"


def build_merge_segments(video_info):
    """Turn the client's timeline payload into render segments (cut straight from the library sources)."""
    segments = []
    for index, video in enumerate(video_info):
        if not isinstance(video, dict):
            raise render_pipeline.RenderError(f'Timeline entry {index + 1} is malformed.')
        title = str(video.get('title') or f'Clip {index + 1}')
        try:
            media, base, seconds = resolve_cut_media(**_clip_reference_from(video))
        except ClipReferenceError:
            raise render_pipeline.RenderError(
                f'Clip {index + 1} ("{title}") has no media any more. Use "Restore missing clips" or remove it.')
        start_trim = max(0.0, sanitize_float(video.get('startTrim'), 0.0)) / 1000.0
        end_trim = max(0.0, sanitize_float(video.get('endTrim'), 0.0)) / 1000.0
        duration = seconds - start_trim - end_trim
        if render_pipeline.too_short(duration):
            raise render_pipeline.RenderError(
                f'Clip {index + 1} ("{title}") is trimmed to nothing: {seconds * 1000:.0f} ms long, '
                f'trims {start_trim * 1000:.0f} + {end_trim * 1000:.0f} ms.')
        segments.append(render_pipeline.Segment(
            media=media,
            start=base + start_trim,
            duration=duration,
            speed=render_pipeline.clamp_speed(video.get('speed')),
            title=title,
        ))
    return segments


@app.route('/merge_videos_stream', methods=['POST'])
def merge_videos_stream():
    """SSE endpoint: render the timeline sample-accurately and stream progress."""
    data = request_json_object()
    video_info = data.get('videos')
    if not isinstance(video_info, list) or not video_info:
        return jsonify({'error': 'No videos to merge'}), 400

    print(f"[Merge SSE] Received {len(video_info)} videos to merge")
    filename = _merge_output_name(video.get('title') if isinstance(video, dict) else '' for video in video_info)
    output_path = os.path.join(TEMP_DIR, filename)

    def generate():
        try:
            segments = build_merge_segments(video_info)
            events = render_pipeline.render_timeline(
                segments,
                output_path,
                TEMP_DIR,
                resolution=MERGE_RESOLUTION,
                fps=MERGE_FPS,
                gpu_encoder=get_merge_gpu_encoder(),
                gpu_slots=GPU_ENCODING_SEMAPHORE,
                max_workers=get_max_workers(),
            )
            summary = {}
            for progress_update in events:
                summary = progress_update
                yield f'data: {json.dumps(progress_update)}\n\n'
            print(f"[Merge SSE] ✓ {filename}: {summary.get('duration_ms')} ms, {summary.get('frames')} frames, "
                  f"{summary.get('width')}x{summary.get('height')} @ {summary.get('fps')} fps ({summary.get('encoder')})")
            done = {'done': True, 'merged_video': f'temp/{filename}'}
            # Clips whose library video is gone are cut from their preview clip, which is a lossy re-encode
            from_previews = sum(1 for segment in segments if _is_inside(segment.media, TEMP_DIR))
            if from_previews:
                done['warning'] = (f'{from_previews} of {len(segments)} clips were cut from their preview clips '
                                   f'because the source video was not found: their quality is lower.')
            yield f'data: {json.dumps(done)}\n\n'
        except (GeneratorExit, BrokenPipeError, ConnectionError):
            print("[Merge SSE] ❌ Client disconnected")
            raise
        except render_pipeline.RenderError as exc:
            print(f"[Merge SSE] ❌ {exc}")
            yield f'data: {json.dumps({"error": str(exc)})}\n\n'
        except Exception as exc:
            print(f"[Merge SSE] ❌ Error during merge: {exc}")
            traceback.print_exc()
            yield f'data: {json.dumps({"error": str(exc)})}\n\n'

    return Response(stream_with_context(generate()), mimetype='text/event-stream')



# ---------------------------------------------------------------------------------------------------------
# Preview clip export
# ---------------------------------------------------------------------------------------------------------

def compute_clip_bounds(match_start, match_end, video_duration):
    """Padded clip bounds for a match, clamped to the file. Returns (clip_start, clip_end) in source seconds."""
    clip_start = max(match_start - DEFAULT_CLIP_START_PADDING, 0.0)
    clip_end = match_end + DEFAULT_CLIP_END_PADDING
    clip_start = min(clip_start, max(video_duration - 0.1, 0.0))   # leave at least 0.1 s
    clip_end = min(clip_end, video_duration)
    if clip_end <= clip_start:
        wanted = max(match_end - match_start, 0.0) + DEFAULT_CLIP_START_PADDING + DEFAULT_CLIP_END_PADDING
        clip_end = min(clip_start + max(wanted, 0.1), video_duration)
    if clip_start >= clip_end:
        raise ValueError(f"Invalid clip range: start={clip_start}, end={clip_end}, video_duration={video_duration}")
    return clip_start, clip_end


def clip_file_name(clip_group_name, source, clip_start, clip_end):
    """Name a clip after what it contains (source + bounds, millisecond precision).

    A path therefore always denotes the same cut: repeating a search with other options cannot change what an
    existing timeline entry shows, and an identical cut is rendered only once.
    """
    key = hashlib.md5(f'{source}|{clip_start:.3f}|{clip_end:.3f}'.encode('utf-8')).hexdigest()[:16]
    return f'{clip_group_name}_{key}.mp4'


# Messages that mean the GPU encoder itself is unusable (as opposed to one input it could not take).
GPU_FAILURE_MARKERS = (
    'cannot load', 'nvidia driver', 'no capable devices', 'incompatible client key', 'openencodesessionex failed', 'no device',
    'failed to initialize', 'error while opening encoder', 'could not open encoder', 'encoder initialization',
    'invalid parameter',
)


def _clip_export_command(source_path, clip_start, clip_end, fps, video_args, output_path):
    return [
        'ffmpeg', '-v', 'error', '-nostdin', '-y',
        '-ss', f'{clip_start:.6f}', '-i', source_path, '-t', f'{clip_end - clip_start:.6f}',
        *video_args,
        '-pix_fmt', 'yuv420p',       # 10-bit and 4:4:4 sources otherwise give H.264 profiles browsers cannot play
        '-c:a', 'aac', '-ac', '2',   # stereo: mono and 5.1 sources otherwise play wrong in browsers
        '-r', str(fps),
        '-f', 'mp4', output_path,
    ]


def render_preview_clip(source_path, clip_start, clip_end, output_path, should_abort=None):
    """Cut ``[clip_start, clip_end)`` of a library video into a browser-playable preview clip.

    Uses the GPU encoder while a slot is free (the slot is held for the whole encode, so --gpustreams is
    honoured) and libx264 otherwise or when the GPU encode fails. The clip appears at ``output_path`` only when
    it is complete. Raises RuntimeError with a readable message on failure.
    """
    if os.path.isfile(output_path) and os.path.getsize(output_path) > 0:
        return
    fps = get_video_fps(source_path) or 25
    partial_path = f'{output_path}.{uuid.uuid4().hex}.partial'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    def run(video_args):
        command = _clip_export_command(source_path, clip_start, clip_end, fps, video_args, partial_path)
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = 300.0
            while True:
                try:
                    _, stderr = process.communicate(timeout=0.25)
                    return process.returncode, (stderr or b'').decode(errors='ignore')
                except subprocess.TimeoutExpired:
                    deadline -= 0.25
                    if deadline <= 0 or (should_abort and should_abort()):
                        process.kill()
                        process.communicate()
                        return -1, 'aborted' if deadline > 0 else 'ffmpeg timed out'
        finally:
            if process.poll() is None:
                process.kill()

    try:
        gpu_encoder, gpu_args = get_gpu_encoder()
        returncode, stderr = None, ''
        if gpu_encoder and GPU_ENCODING_SEMAPHORE and GPU_ENCODING_SEMAPHORE.acquire(blocking=False):
            try:
                returncode, stderr = run(['-c:v', gpu_encoder, *(gpu_args or [])])
                if returncode == 244 and 'nvenc' in gpu_encoder:
                    # "Invalid parameter": some driver/ffmpeg combinations only accept the plain preset.
                    returncode, stderr = run(['-c:v', gpu_encoder, '-preset', 'p4'])
            finally:
                GPU_ENCODING_SEMAPHORE.release()
            if returncode not in (0, -1):
                lowered = stderr.lower()
                if returncode in (187, 244) or any(marker in lowered for marker in GPU_FAILURE_MARKERS):
                    print(f"[GPU] {gpu_encoder} failed (code {returncode}): {stderr.strip()[-300:]}")
                    disable_gpu_encoder()
                returncode = None     # retry on the CPU below
        if returncode is None:
            returncode, stderr = run(['-c:v', 'libx264', '-preset', 'ultrafast'])
        if returncode != 0 or not os.path.isfile(partial_path) or os.path.getsize(partial_path) == 0:
            detail = stderr.strip().splitlines()[-1:] or ['ffmpeg produced no output']
            raise RuntimeError(detail[0][:300])
        os.replace(partial_path, output_path)
    finally:
        if os.path.exists(partial_path):
            try:
                os.remove(partial_path)
            except OSError:
                pass


def interleave_by_file(items):
    """Round-robin over the source files so a limited result list is spread across the selected videos."""
    by_file = OrderedDict()
    for item in items:
        by_file.setdefault(item['file'], []).append(item)
    mixed = []
    queues = [list(group) for group in by_file.values()]
    while queues:
        for queue in queues:
            mixed.append(queue.pop(0))
        queues = [queue for queue in queues if queue]
    return mixed


def find_and_export_longest_matches_incremental(
    input_sentence,
    filenames,
    counter,
    export_clips=False,
    clip_group=None,
    min_silence=0.0,
    max_silence=10.0,
    silence_word_threshold=2,
    search_id=None,
    include_partial_matches=False,
    all_partial_matches=False,
    max_results_per_segment=25
):
    """
    Generator version that yields results per segment as they're exported.
    Each yield is a dict with 'phrase', 'files', and 'word_count' for one segment.
    If include_partial_matches is False (default), only returns the full input_sentence match.
    If include_partial_matches is True, returns all sub-segments found.
    """
    clip_output_dir = None
    clip_output_dir_name = None
    clip_group_name = clip_group or generate_clip_group_name('segment', input_sentence, filenames)
    silence_word_threshold = max(silence_word_threshold, 1)
    max_results_per_segment = max(1, max_results_per_segment)

    if export_clips:
        # Clips are named after their content (see clip_file_name), so the folder is only ever added to.
        clip_output_dir_name = f'{clip_group_name}_clips'
        clip_output_dir = os.path.join(TEMP_DIR, clip_output_dir_name)
        os.makedirs(clip_output_dir, exist_ok=True)

    def is_cancelled():
        if not search_id:
            return False
        with CANCEL_LOCK:
            return search_id in CANCELLED_SEARCHES

    def is_skipped(segment):
        if not search_id:
            return False
        with SKIP_LOCK:
            return segment in SKIP_SEGMENTS.get(search_id, set())

    # First, search ALL files and collect ALL matches
    all_file_matches = []
    for filename in filenames:
        if is_cancelled():
            raise SearchCancelled()
        # Word index of the transcript (cached); files without word timings cannot be matched
        sentences = load_word_index(filename)
        if not sentences or not sentences.words:
            continue

        if not include_partial_matches:
            # Only the exact full sentence, not all sub-phrases
            matches = find_matches_for_segment(
                input_sentence,
                sentences,
                min_duration=min_silence if len(input_sentence.split()) < silence_word_threshold else 0.0,
                max_duration=max_silence,
                silence_word_threshold=silence_word_threshold
            )
        else:
            # Sub-phrases too; very short ones (< 3 words) only when "all partial matches" is on
            matches = find_longest_matches(
                input_sentence,
                sentences,
                min_silence=min_silence,
                max_silence=max_silence,
                silence_word_threshold=silence_word_threshold,
                min_words=1 if all_partial_matches else 3
            )

        if matches:
            all_file_matches.append({'file': filename, 'matches': matches})
            video_name = os.path.basename(filename)
            # Count unique segments found
            unique_segments = set(match.get('segment', '') for match in matches)
            print(f"Found {len(matches)} matches ({len(unique_segments)} unique segments) for '{input_sentence}' in {video_name}")
    
    if not all_file_matches:
        return
    
    # Group all matches by segment across all files
    segment_groups = {}
    for entry in all_file_matches:
        for match in entry['matches']:
            segment = match.get('segment', '')
            if segment not in segment_groups:
                segment_groups[segment] = []
            segment_groups[segment].append({'file': entry['file'], 'match': match})
    
    # Sort segments by length (longest first)
    sorted_segments = sorted(segment_groups.keys(), key=lambda s: len(s.split()), reverse=True)
    
    # If not including partial matches, filter to only the full input sentence
    if not include_partial_matches:
        # Keep only segments that match the full input sentence (ignoring case and extra whitespace)
        input_normalized = ' '.join(input_sentence.lower().split())
        print(f"[Filter] Full match only mode: filtering {len(sorted_segments)} segments to match '{input_normalized}'")
        print(f"[Filter] Sample segments before filtering: {sorted_segments[:5] if len(sorted_segments) > 0 else 'none'}")
        
        filtered_segments = []
        for seg in sorted_segments:
            seg_normalized = ' '.join(seg.lower().split())
            if seg_normalized == input_normalized:
                filtered_segments.append(seg)
        
        sorted_segments = filtered_segments
        print(f"[Filter] After filtering: {len(sorted_segments)} segments match exactly")
        
        if sorted_segments:
            total_full_matches = sum(len(segment_groups[seg]) for seg in sorted_segments)
            print(f"Full match only mode: found {total_full_matches} matches for '{sorted_segments[0]}' across {len(all_file_matches)} video(s)")
        else:
            print(f"Full match only mode: no exact matches found for '{input_sentence}'")
            print(f"[Filter] Available segments were: {list(sorted(segment_groups.keys(), key=lambda s: len(s.split()), reverse=True))[:10]}")
            return
    else:
        # If including partial matches, prioritize longest matches first
        # Check if the full input sentence exists in any file
        input_normalized = ' '.join(input_sentence.lower().split())
        full_match_exists = False
        for seg in sorted_segments:
            seg_normalized = ' '.join(seg.lower().split())
            if seg_normalized == input_normalized:
                full_match_exists = True
                break
        
        if full_match_exists:
            # If full match exists, only process segments that are at least as long as the input
            # or are not substrings of longer segments that match the input
            input_word_count = len(input_normalized.split())
            prioritized_segments = []
            other_segments = []
            
            for seg in sorted_segments:
                seg_normalized = ' '.join(seg.lower().split())
                seg_word_count = len(seg_normalized.split())
                
                # Prioritize segments that are >= input length or are the full match
                if seg_word_count >= input_word_count or seg_normalized == input_normalized:
                    prioritized_segments.append(seg)
                else:
                    # For shorter segments, check if they're substrings of longer prioritized segments
                    is_substring_of_longer = False
                    for longer_seg in prioritized_segments:
                        longer_normalized = ' '.join(longer_seg.lower().split())
                        if len(longer_normalized.split()) > seg_word_count and seg_normalized in longer_normalized:
                            is_substring_of_longer = True
                            break
                    
                    if not is_substring_of_longer:
                        other_segments.append(seg)
            
            # Process longest matches first, then shorter ones
            sorted_segments = prioritized_segments + other_segments
            
            if not all_partial_matches:
                # Filter out very short partials from the "other_segments" part
                filtered_other = []
                for seg in other_segments:
                    seg_normalized = ' '.join(seg.lower().split())
                    seg_words = seg_normalized.split()
                    
                    # Keep if it's 3+ words, or if it's not a substring of a longer segment
                    if len(seg_words) >= 3:
                        filtered_other.append(seg)
                    else:
                        # Check if this short segment is a substring of any longer segment
                        is_substring = False
                        for other_seg in sorted_segments:
                            if seg == other_seg:
                                continue
                            other_normalized = ' '.join(other_seg.lower().split())
                            other_words = other_normalized.split()
                            
                            # If other is longer and contains this segment as a substring
                            if len(other_words) > len(seg_words) and seg_normalized in other_normalized:
                                is_substring = True
                                break
                        
                        # Only keep if it's not a substring of a longer segment
                        if not is_substring:
                            filtered_other.append(seg)
                
                sorted_segments = prioritized_segments + filtered_other
                total_found = sum(len(segment_groups[seg]) for seg in sorted_segments)
                print(f"Partial matches mode (filtered): found {total_found} matches across {len(all_file_matches)} video(s)")
                print(f"Processing {len(sorted_segments)} unique segments (prioritizing longest matches, filtered short partials), max {max_results_per_segment} per segment")
            else:
                total_found = sum(len(segment_groups[seg]) for seg in sorted_segments)
                print(f"Partial matches mode: found {total_found} matches across {len(all_file_matches)} video(s)")
                print(f"Processing {len(sorted_segments)} unique segments (prioritizing longest matches), max {max_results_per_segment} per segment")
        else:
            # No full match exists, use original filtering logic
            if not all_partial_matches:
                input_words = input_normalized.split()
                
                # Filter out segments that are very short (< 3 words) and are substrings of longer segments
                filtered_segments = []
                for seg in sorted_segments:
                    seg_normalized = ' '.join(seg.lower().split())
                    seg_words = seg_normalized.split()
                    
                    # Keep if it's 3+ words, or if it's not a substring of a longer segment
                    if len(seg_words) >= 3:
                        filtered_segments.append(seg)
                    else:
                        # Check if this short segment is a substring of any longer segment
                        is_substring = False
                        for other_seg in sorted_segments:
                            if seg == other_seg:
                                continue
                            other_normalized = ' '.join(other_seg.lower().split())
                            other_words = other_normalized.split()
                            
                            # If other is longer and contains this segment as a substring
                            if len(other_words) > len(seg_words) and seg_normalized in other_normalized:
                                is_substring = True
                                break
                        
                        # Only keep if it's not a substring of a longer segment
                        if not is_substring:
                            filtered_segments.append(seg)
                
                sorted_segments = filtered_segments
                total_found = sum(len(segment_groups[seg]) for seg in sorted_segments)
                print(f"Partial matches mode (filtered): found {total_found} matches across {len(all_file_matches)} video(s)")
                print(f"Processing {len(sorted_segments)} unique segments (filtered short partials), max {max_results_per_segment} per segment")
            else:
                total_found = sum(len(segment_groups[seg]) for seg in segment_groups)
                print(f"Total matches found: {total_found} across {len(all_file_matches)} video(s)")
                print(f"Processing {len(segment_groups)} unique segments (including all partial matches), max {max_results_per_segment} per segment")
    
    duration_cache = {}

    def plan_clip(index, item):
        """Where to cut and where to put it. Raises ValueError/FileNotFoundError with a readable message."""
        source = item['file']
        match = item['match']
        if not os.path.exists(source):
            raise FileNotFoundError(f"Video file not found: {source}")
        match_start = sanitize_float(match.get('start'), None)
        match_end = sanitize_float(match.get('end'), match_start)
        if match_start is None or match_end is None:
            raise ValueError(f"Invalid time values: start={match.get('start')}, end={match.get('end')}")
        if source not in duration_cache:
            duration_cache[source] = get_video_duration(source)
        video_duration = duration_cache[source]
        if video_duration is None:
            raise ValueError(f"Could not determine video duration for: {source}")
        clip_start, clip_end = compute_clip_bounds(match_start, match_end, video_duration)
        clip_filename = clip_file_name(clip_group_name, source, clip_start, clip_end)
        return {
            'index': index,
            'source': source,
            'clip_start': clip_start,
            'clip_end': clip_end,
            'output_path': os.path.join(clip_output_dir, clip_filename),
            'result': {
                'file': posixpath.join('temp', clip_output_dir_name, clip_filename),
                'start': match_start,
                'end': match_end,
                'segment': match.get('segment'),
                'source_video': source,
                # Bounds of the exported clip in source time: the canonical definition of the clip.
                'original_start': clip_start,
                'original_end': clip_end,
                'duration_ms': round((clip_end - clip_start) * 1000),
                # How much padding surrounds the match (less than the default at the file start/end).
                'lead_ms': max(0, round((match_start - clip_start) * 1000)),
                'tail_ms': max(0, round((clip_end - match_end) * 1000)),
            },
        }

    # Process each segment and yield results incrementally
    total_segments = len(sorted_segments)
    for segment_idx, segment in enumerate(sorted_segments, 1):
        all_segment_matches = interleave_by_file(segment_groups[segment])
        segment_matches = all_segment_matches[:max_results_per_segment]
        word_count = len(segment.split())

        progress_msg = f"Processing segment {segment_idx}/{total_segments}: '{segment}' ({len(segment_matches)} matches)"
        print(progress_msg)
        # Progress first, so the user can skip the segment before any clip is rendered
        yield {
            'progress': progress_msg,
            'segment_index': segment_idx,
            'total_segments': total_segments,
            'segment_phrase': segment
        }

        if is_cancelled():
            print("[Generator] ❌ CANCELLED before clip processing - exiting immediately")
            raise SearchCancelled()
        if is_skipped(segment):
            print(f"[Generator] ⏭️  Segment {segment_idx}/{total_segments}: '{segment}' was skipped")
            yield {'phrase': segment, 'files': [], 'word_count': word_count, 'skipped': True}
            continue
        if not export_clips or not clip_output_dir:
            yield {'phrase': segment, 'files': [], 'word_count': word_count, 'total_matches': len(all_segment_matches)}
            continue

        plans, errors = [], []
        for index, item in enumerate(segment_matches):
            try:
                plans.append(plan_clip(index, item))
            except (ValueError, OSError) as exc:
                errors.append(str(exc))

        rendered = {}
        segment_skipped = False
        stop_rendering = Event()
        executor = ThreadPoolExecutor(max_workers=get_max_workers())
        try:
            futures = {
                executor.submit(render_preview_clip, plan['source'], plan['clip_start'], plan['clip_end'],
                                plan['output_path'], stop_rendering.is_set): plan
                for plan in plans
            }
            completed = 0
            for future in as_completed(futures):
                plan = futures[future]
                completed += 1
                try:
                    future.result()
                    rendered[plan['index']] = plan['result']
                except CancelledError:
                    pass
                except Exception as exc:
                    if not stop_rendering.is_set():
                        errors.append(str(exc))
                        print(f"Failed to render clip {plan['index'] + 1}/{len(segment_matches)}: {exc}")
                if stop_rendering.is_set():
                    continue
                if is_cancelled():
                    print("[Generator] ❌ CANCELLED during clip rendering")
                    raise SearchCancelled()
                if is_skipped(segment):
                    print(f"[Generator] ⏭️  SKIP detected during clip rendering: '{segment}'")
                    segment_skipped = True
                    stop_rendering.set()
                    for pending in futures:
                        pending.cancel()
                    continue
                yield {
                    'progress': f"Processing segment {segment_idx}/{total_segments}: '{segment}' ({completed}/{len(segment_matches)} matches)",
                    'segment_index': segment_idx,
                    'total_segments': total_segments,
                    'segment_phrase': segment,
                    'clip_index': completed,
                    'total_clips': len(segment_matches)
                }
        finally:
            # Also reached when the client disconnects (GeneratorExit) or cancels: stop encoding right away.
            stop_rendering.set()
            executor.shutdown(wait=True, cancel_futures=True)

        if segment_skipped:
            yield {'phrase': segment, 'files': [], 'word_count': word_count, 'skipped': True}
            continue

        files = [rendered[index] for index in sorted(rendered)]     # stable order: as matched, not as encoded
        print(f"[Generator] ✓ Yielding segment {segment_idx}/{total_segments}: '{segment}' with {len(files)} clips")
        result = {
            'phrase': segment,
            'files': files,
            'word_count': word_count,
            'total_matches': len(all_segment_matches)
        }
        if errors:
            result['failed'] = len(errors)
            result['first_error'] = errors[0]
        yield result


def find_matches_for_segment(segment, sentences, min_duration=0.0, max_duration=10.0, silence_word_threshold=2):
    """All occurrences of ``segment`` (a word sequence) in a transcript.

    ``sentences`` is a WordIndex or the raw transcript segments. Phrases shorter than
    ``silence_word_threshold`` words must be surrounded by silences within [min_duration, max_duration];
    longer ones are returned regardless of the pauses around them.
    """
    index = sentences if isinstance(sentences, WordIndex) else WordIndex(sentences)
    all_words = index.words
    segment_words = [token for token in (normalize_token(raw_word) for raw_word in segment.split()) if token]
    word_count = len(segment_words)
    if not word_count or not all_words:
        return []

    all_matches = []
    total_available = len(all_words)
    # Only positions where the first word occurs can start a match (same order as a linear scan).
    for i in index.positions.get(segment_words[0], ()):
        if i + word_count > total_available:
            break
        if any(all_words[i + offset]['normalized_word'] != segment_words[offset] for offset in range(1, word_count)):
            continue
        start_time = all_words[i]['start']
        end_time = all_words[i + word_count - 1]['end']

        # Silence before and after the segment
        first_silence = start_time - all_words[i - 1]['end'] if i > 0 else start_time
        if i + word_count < total_available:
            second_silence = all_words[i + word_count]['start'] - end_time
        else:
            second_silence = max_duration
        first_silence = max(first_silence, 0.0)
        second_silence = max(second_silence, 0.0)

        if word_count < silence_word_threshold and not (
                min_duration <= first_silence <= max_duration and min_duration <= second_silence <= max_duration):
            continue
        all_matches.append({
            'segment': segment,
            'start': start_time,
            'end': end_time,
            'first_silence': first_silence,
            'second_silence': second_silence
        })
    return all_matches


def find_longest_matches(
    input_sentence,
    sentences,
    min_silence=0.0,
    max_silence=10.0,
    silence_word_threshold=2,
    min_words=1
):
    """Matches of every contiguous sub-phrase of ``input_sentence`` (so "finanziellen belastungen" is found
    even when "an den finanziellen belastungen durch" also matched). Sub-phrases shorter than ``min_words``
    are skipped unless they are the whole input."""
    input_words = input_sentence.split()
    total_words = len(input_words)
    if total_words == 0:
        return []

    index = sentences if isinstance(sentences, WordIndex) else WordIndex(sentences)
    silence_word_threshold = max(silence_word_threshold, 1)
    matches = []
    seen_ranges = set()
    for i in range(total_words):
        for j in range(i + 1, total_words + 1):
            segment_words = input_words[i:j]
            segment = " ".join(segment_words)
            word_count = len(segment_words)
            if word_count < min_words and segment != input_sentence:
                continue
            for match in find_matches_for_segment(
                segment,
                index,
                min_duration=min_silence if word_count < silence_word_threshold else 0.0,
                max_duration=max_silence,
                silence_word_threshold=silence_word_threshold
            ):
                key = (match['start'], match['end'], segment)
                if key not in seen_ranges:
                    seen_ranges.add(key)
                    matches.append(match)
    return matches


# ---------------------------------------------------------------------------------------------------------
# Clip references, preview audio, waveform and clip restore
# ---------------------------------------------------------------------------------------------------------

class ClipReferenceError(ValueError):
    """The client referenced media that does not exist (any more) or lies outside the allowed folders."""


def _is_inside(path, root):
    """True if ``path`` lies below ``root`` after normalising ``..`` (symlinks inside the root stay allowed)."""
    root = os.path.abspath(root)
    path = os.path.abspath(path)
    return path == root or path.startswith(root + os.sep)


def resolve_media_path(reference):
    """Map a client media reference ('temp/...' or a path below static/) to an absolute path, or None."""
    if not isinstance(reference, str) or not reference.strip():
        return None
    cleaned = reference.replace('\\', '/').lstrip('/')
    if cleaned.startswith('temp/'):
        root, relative = TEMP_DIR, cleaned[len('temp/'):]
    else:
        root, relative = os.path.join(app.root_path, 'static'), cleaned
    candidate = os.path.normpath(os.path.join(root, relative))
    return candidate if relative and _is_inside(candidate, root) else None


def resolve_source_video(reference):
    """Map a source video reference (as returned by /get_files) to an existing file inside a library root."""
    if not isinstance(reference, str) or not reference.strip():
        return None
    candidates = [reference, os.path.join(app.root_path, reference)]
    for lib_root in LIBRARY_ROOTS:
        candidates.append(os.path.join(lib_root, reference.lstrip('/\\')))
    for candidate in candidates:
        candidate = os.path.normpath(candidate)
        if not os.path.isfile(candidate):
            continue
        if any(_is_inside(candidate, lib_root) or _is_inside(candidate, os.path.join(app.root_path, lib_root))
               for lib_root in LIBRARY_ROOTS):
            return candidate
    return None


def _finite_number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float('inf'), float('-inf')) else None


_PROBE_CACHE = {}
_PROBE_CACHE_LOCK = Lock()


def cached_probe(path):
    """probe_media() memoised on (path, mtime, size)."""
    stat = os.stat(path)
    key = (path, stat.st_mtime_ns, stat.st_size)
    with _PROBE_CACHE_LOCK:
        cached = _PROBE_CACHE.get(key)
    if cached is None:
        cached = render_pipeline.probe_media(path)
        with _PROBE_CACHE_LOCK:
            if len(_PROBE_CACHE) > 2048:
                _PROBE_CACHE.clear()
            _PROBE_CACHE[key] = cached
    return cached


def resolve_cut_media(clip=None, source=None, original_start=None, original_end=None):
    """Resolve a client clip reference to ``(media_path, base_seconds, clip_seconds)``.

    The library source plus the recorded bounds is the canonical definition of a clip: preview audio, waveform
    and merge all cut from it, so they agree to the sample. The exported clip file in temp/ is only the fallback
    (entries without bounds, or a source that has been moved).
    """
    source_path = resolve_source_video(source)
    start = _finite_number(original_start)
    end = _finite_number(original_end)
    if source_path and start is not None and end is not None and start >= 0 and end > start:
        if end - start > MAX_CLIP_SECONDS:
            raise ClipReferenceError(f'Clip bounds span more than {MAX_CLIP_SECONDS} seconds.')
        return source_path, start, end - start
    clip_path = resolve_media_path(clip)
    if clip_path and os.path.isfile(clip_path):
        info = cached_probe(clip_path)
        seconds = info.audio_duration or info.duration
        if seconds and 0 < seconds <= MAX_CLIP_SECONDS:
            return clip_path, 0.0, seconds
    raise ClipReferenceError('Clip media not found. The source video may have moved and the cached clip is gone.')


# Clips are a few seconds long. The bound keeps a bogus request from decoding hours of audio into memory.
MAX_CLIP_SECONDS = 600

_PCM_CACHE = OrderedDict()
_PCM_CACHE_LOCK = Lock()
PCM_CACHE_MAX_BYTES = 192 * 1024 * 1024


def cached_pcm(media, start, duration, speed=1.0):
    """extract_pcm() with a small LRU so the waveform and the preview of one clip decode only once."""
    stat = os.stat(media)
    key = (media, stat.st_mtime_ns, stat.st_size, round(start * 1e6), round(duration * 1e6), round(speed * 1e4))
    with _PCM_CACHE_LOCK:
        pcm = _PCM_CACHE.get(key)
        if pcm is not None:
            _PCM_CACHE.move_to_end(key)
            return pcm
    pcm = render_pipeline.extract_pcm(media, start, duration, speed)
    with _PCM_CACHE_LOCK:
        _PCM_CACHE[key] = pcm
        total = sum(len(value) for value in _PCM_CACHE.values())
        while total > PCM_CACHE_MAX_BYTES and len(_PCM_CACHE) > 1:
            _, evicted = _PCM_CACHE.popitem(last=False)
            total -= len(evicted)
    return pcm


def _clip_reference_from(mapping):
    """Read a clip reference from request args / JSON (accepts the legacy 'file_path' and 'video' keys)."""
    return {
        'clip': mapping.get('clip') or mapping.get('file_path') or mapping.get('video'),
        'source': mapping.get('source') or mapping.get('sourceVideo') or mapping.get('source_video'),
        'original_start': mapping.get('originalStart', mapping.get('original_start')),
        'original_end': mapping.get('originalEnd', mapping.get('original_end')),
    }


@app.route('/clip_audio', methods=['GET'])
def clip_audio():
    """48 kHz stereo WAV of a clip for the sample-accurate (Web Audio) preview.

    Without trims the whole clip is returned and the browser cuts it on the audio clock. With ``speed`` != 1 the
    browser asks for the trimmed range so the server applies the same trim -> atempo chain the merge uses.
    """
    try:
        media, base, seconds = resolve_cut_media(**_clip_reference_from(request.args))
    except ClipReferenceError as exc:
        return jsonify({'error': str(exc)}), 404
    start_ms = max(0.0, sanitize_float(request.args.get('start_ms'), 0.0))
    end_trim_ms = max(0.0, sanitize_float(request.args.get('end_trim_ms'), 0.0))
    speed = render_pipeline.clamp_speed(request.args.get('speed'))
    duration = seconds - (start_ms + end_trim_ms) / 1000.0
    if render_pipeline.too_short(duration):
        return jsonify({'error': 'Trim values leave nothing to play'}), 400
    try:
        pcm = cached_pcm(media, base + start_ms / 1000.0, duration, speed)
    except render_pipeline.RenderError as exc:
        return jsonify({'error': str(exc)}), 500
    response = Response(render_pipeline.pcm_to_wav(pcm), mimetype='audio/wav')
    response.headers['Cache-Control'] = 'private, max-age=600'
    return response


@app.route('/get_waveform', methods=['POST'])
def get_waveform():
    """Waveform envelope of exactly the audio the preview plays, plus its duration (the trim reference)."""
    data = request_json_object()
    try:
        media, base, seconds = resolve_cut_media(**_clip_reference_from(data))
    except ClipReferenceError as exc:
        return jsonify({'error': str(exc)}), 404
    width = max(16, min(4000, sanitize_int(data.get('width'), 800)))
    try:
        pcm = cached_pcm(media, base, seconds, 1.0)
    except render_pipeline.RenderError as exc:
        return jsonify({'error': str(exc)}), 500
    waveform = render_pipeline.waveform_peaks(pcm, width)
    return jsonify({'waveform': waveform, 'width': len(waveform), 'duration_ms': round(seconds * 1000.0, 3)})


@app.route('/ensure_clip', methods=['POST'])
def ensure_clip():
    """Make sure a preview clip exists, re-cutting it from its source video and bounds if necessary.

    With ``canonical`` the clip is (re)homed to its content-addressed name (see clip_file_name) and that path is
    returned. Timeline entries made before clips were content-addressed point at index-named files that a
    later search may have overwritten with other footage; this gives them a file that is theirs.
    """
    data = request_json_object()
    reference = _clip_reference_from(data)
    clip_path = resolve_media_path(reference['clip'])
    if not clip_path or not _is_inside(clip_path, TEMP_DIR) or not clip_path.lower().endswith('.mp4'):
        return jsonify({'error': 'Invalid clip path'}), 400
    clip_reference = reference['clip']
    source_path = resolve_source_video(reference['source'])
    start = _finite_number(reference['original_start'])
    end = _finite_number(reference['original_end'])
    has_bounds = bool(source_path) and start is not None and end is not None and start >= 0 and end > start
    if has_bounds and end - start > MAX_CLIP_SECONDS:
        return jsonify({'error': f'Clip bounds span more than {MAX_CLIP_SECONDS} seconds.'}), 400
    # Clips live in one folder per search below temp/ (older versions wrote them into temp/ itself)
    relative = os.path.relpath(clip_path, TEMP_DIR).replace(os.sep, '/').split('/')
    if len(relative) > 2 or not re.fullmatch(r'[\w.,+ -]+\.mp4', relative[-1]):
        return jsonify({'error': 'Invalid clip path'}), 400

    if data.get('canonical') and has_bounds:
        folder = os.path.dirname(clip_path)
        folder_name = os.path.basename(folder)
        group = folder_name[:-len('_clips')] if folder_name.endswith('_clips') else 'clip'
        clip_path = os.path.join(folder, clip_file_name(group, reference['source'], start, end))
        clip_reference = posixpath.join('temp', os.path.relpath(clip_path, TEMP_DIR).replace(os.sep, '/'))

    if os.path.isfile(clip_path):
        return jsonify({'status': 'exists', 'clip': clip_reference})
    if not has_bounds:
        return jsonify({'error': 'The clip is gone and its source video or bounds are unknown'}), 404
    try:
        render_preview_clip(source_path, start, end, clip_path)
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print(f"[ensure_clip] {exc}")
        return jsonify({'error': str(exc)}), 500
    return jsonify({'status': 'restored', 'clip': clip_reference})


if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Video Phrase Extractor')
    parser.add_argument(
        '--gpustreams',
        type=int,
        default=2,
        help='Maximum number of parallel GPU encoding sessions (default: 2)'
    )
    parser.add_argument(
        '--library-path',
        type=str,
        action='append',
        default=[],
        help='Additional library search path (can be specified multiple times)'
    )
    parser.add_argument(
        '--listen',
        type=str,
        default='127.0.0.1:5000',
        help='Server listener address in format HOST:PORT (default: 127.0.0.1:5000, example: 0.0.0.0:8080)'
    )
    parser.add_argument(
        '--allowed-host',
        action='append',
        default=[],
        metavar='NAME',
        help='Host name the app may be opened under, besides IP addresses, localhost and this machine\'s name '
             '(repeatable; needed behind a reverse proxy or with a custom DNS name)'
    )
    parser.add_argument(
        '--merge-resolution',
        type=str,
        default='auto',
        help="Merged video size as WIDTHxHEIGHT (e.g. 1920x1080), or 'auto' for the most common source resolution (default: auto)"
    )
    parser.add_argument(
        '--merge-fps',
        type=str,
        default='auto',
        help="Merged video frame rate, e.g. 30 or 30000/1001, or 'auto' for the most common source rate (default: auto)"
    )
    parser.add_argument(
        '--ollama-url',
        type=str,
        default=None,
        help=f"Ollama generate endpoint for AI Story (default: env OLLAMA_URL or {OLLAMA_URL})"
    )
    parser.add_argument(
        '--ollama-model',
        type=str,
        default=None,
        help=f"Ollama model for AI Story (default: env OLLAMA_MODEL or {OLLAMA_MODEL})"
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Run Flask in debug mode (auto-reload and the interactive debugger). Never use it on a public address.'
    )
    args = parser.parse_args()

    if args.ollama_url:
        OLLAMA_URL = args.ollama_url
    if args.ollama_model:
        OLLAMA_MODEL = args.ollama_model

    # Merge output format
    try:
        if args.merge_resolution.strip().lower() == 'auto':
            MERGE_RESOLUTION = None
        else:
            merge_width, merge_height = (int(part) for part in args.merge_resolution.lower().split('x'))
            if merge_width < 16 or merge_height < 16 or merge_width % 2 or merge_height % 2:
                raise ValueError('width and height must be even numbers >= 16')
            MERGE_RESOLUTION = (merge_width, merge_height)
        if args.merge_fps.strip().lower() == 'auto':
            MERGE_FPS = None
        else:
            MERGE_FPS = Fraction(args.merge_fps.strip())
            if not 1 <= MERGE_FPS <= 240:
                raise ValueError('frame rate must be between 1 and 240')
    except (ValueError, ZeroDivisionError) as exc:
        print(f"[Startup] Error: invalid --merge-resolution/--merge-fps ({exc})")
        sys.exit(1)
    print(f"[Startup] Merge output: {'auto' if MERGE_RESOLUTION is None else 'x'.join(map(str, MERGE_RESOLUTION))} "
          f"@ {'auto' if MERGE_FPS is None else MERGE_FPS} fps")
    
    # Set global GPU streams limit
    MAX_GPU_STREAMS = max(1, args.gpustreams)  # Ensure at least 1
    print(f"[Startup] GPU streams limit set to: {MAX_GPU_STREAMS}")
    
    # Additional library paths are given relative to where the user started the server ...
    extra_library_paths = [os.path.abspath(lib_path) for lib_path in args.library_path]
    # ... while the default "Library" folder (and the paths stored in projects) are relative to the app, like
    # data/ and temp/: make that true regardless of the directory the server was started from.
    os.chdir(app.root_path)
    if extra_library_paths:
        for abs_path in extra_library_paths:
            if os.path.exists(abs_path):
                if os.path.realpath(abs_path) not in {os.path.realpath(root) for root in LIBRARY_ROOTS}:
                    LIBRARY_ROOTS.append(abs_path)
                    print(f"[Startup] Added library path: {abs_path}")
                else:
                    print(f"[Startup] Library path already exists: {abs_path}")
            else:
                print(f"[Startup] Warning: Library path does not exist: {abs_path}")
    
    print(f"[Startup] Library search paths: {LIBRARY_ROOTS}")
    
    # Initialize GPU at startup (only once)
    initialize_gpu_at_startup()
    
    # Create data and temp directories on startup
    ensure_project_store()
    
    # Get real CPU core count
    cpu_cores = get_max_workers()
    print(f"[Startup] Using {cpu_cores} CPU cores for parallel processing")
    
    # Parse listener address: HOST, HOST:PORT, [IPv6]:PORT or a bare IPv6 address
    try:
        listen = args.listen.strip()
        bracketed = re.fullmatch(r'\[([^\]]+)\](?::(\d+))?', listen)
        if bracketed:
            host, port = bracketed.group(1), int(bracketed.group(2) or 5000)
        elif listen.count(':') == 1:
            host, port = listen.split(':')
            port = int(port)
        else:
            host, port = listen, 5000
        if not host or not 1 <= port <= 65535:
            raise ValueError(listen)
    except ValueError:
        print(f"[Startup] Error: Invalid listener format '{args.listen}'. Expected HOST:PORT (e.g., 0.0.0.0:8080)")
        sys.exit(1)

    if args.debug and host not in ('127.0.0.1', 'localhost', '::1'):
        print("[Startup] WARNING: --debug exposes an interactive debugger to everyone who can reach "
              f"{host}:{port}. Do not use it on a network you do not fully trust.")
    print(f"[Startup] Starting server on {host}:{port}")
    ALLOWED_HOSTS.update(name.strip().lower().rstrip('.') for name in [*args.allowed_host, host] if name.strip())
    app.run(debug=args.debug, use_reloader=args.debug, threaded=True, host=host, port=port)

import argparse
import glob
import hashlib
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import traceback
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock, Semaphore
from typing import List, Optional, Sequence, Tuple

from flask import Flask, jsonify, render_template, request, send_from_directory, Response, stream_with_context
from videogrep import parse_transcript
from srt_word_boundaries import convert_subtitles

app = Flask(__name__)

DATA_DIR = os.path.join(app.root_path, 'data')
TEMP_DIR = os.path.join(app.root_path, 'temp')
PROJECTS_FILE = os.path.join(DATA_DIR, 'projects.json')
COLLECTIONS_FILE = os.path.join(DATA_DIR, 'collections.json')
PROJECT_STORE_LOCK = Lock()
COLLECTIONS_LOCK = Lock()

# Track skipped segments per search session
SKIP_SEGMENTS = {}  # Format: {search_id: set(segment_phrases)}
SKIP_LOCK = Lock()

# Track cancelled searches
CANCELLED_SEARCHES = set()  # Format: set of search_id strings
CANCEL_LOCK = Lock()

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

def detect_gpu_encoder():
    """
    Detect available GPU encoder for hardware acceleration.
    Returns tuple: (encoder_name, encoder_args) or (None, None) if no GPU available.
    Tests each encoder to ensure it actually works before using it.
    """
    global _GPU_ENCODER_CACHE, _GPU_DISABLED
    if _GPU_DISABLED:
        return (None, None)
    if _GPU_ENCODER_CACHE is not None:
        return _GPU_ENCODER_CACHE
    
    # Try NVIDIA NVENC (most common)
    try:
        result = subprocess.run(
            ['ffmpeg', '-hide_banner', '-encoders'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            encoders = result.stdout + result.stderr
            # Try NVIDIA NVENC with different parameter sets
            # Start with minimal parameters and work up to more complex ones
            if 'h264_nvenc' in encoders:
                # Test with different parameter sets (some may work when others don't)
                # Order matters: test simpler configs first
                test_configs = [
                    # Minimal - just preset (most compatible)
                    (['-preset', 'p4']),
                    # Preset + rate control
                    (['-preset', 'p4', '-rc', 'vbr']),
                    (['-preset', 'p1', '-rc', 'cbr', '-b:v', '5M']),
                    (['-preset', 'p4', '-rc', 'constqp', '-qp', '23']),
                    (['-preset', 'p4', '-rc', 'vbr', '-cq', '23']),
                ]
                for args in test_configs:
                    if test_gpu_encoder('h264_nvenc', args):
                        _GPU_ENCODER_CACHE = ('h264_nvenc', args)
                        print(f"[GPU] Detected and verified NVIDIA NVENC encoder with args: {args}")
                        return _GPU_ENCODER_CACHE
                print("[GPU] NVIDIA NVENC detected but not functional, using CPU")
            elif 'hevc_nvenc' in encoders:
                if test_gpu_encoder('hevc_nvenc', ['-preset', 'p4', '-rc', 'vbr', '-cq', '23']):
                    _GPU_ENCODER_CACHE = ('hevc_nvenc', ['-preset', 'p4', '-rc', 'vbr', '-cq', '23'])
                    print("[GPU] Detected and verified NVIDIA HEVC NVENC encoder")
                    return _GPU_ENCODER_CACHE
            # Try Intel Quick Sync (QSV)
            elif 'h264_qsv' in encoders:
                if test_gpu_encoder('h264_qsv', ['-preset', 'medium', '-global_quality', '23']):
                    _GPU_ENCODER_CACHE = ('h264_qsv', ['-preset', 'medium', '-global_quality', '23'])
                    print("[GPU] Detected and verified Intel Quick Sync Video encoder")
                    return _GPU_ENCODER_CACHE
            # Try AMD VCE/VCN
            elif 'h264_amf' in encoders:
                if test_gpu_encoder('h264_amf', ['-quality', 'balanced', '-rc', 'vbr_peak', '-qmin', '18', '-qmax', '28']):
                    _GPU_ENCODER_CACHE = ('h264_amf', ['-quality', 'balanced', '-rc', 'vbr_peak', '-qmin', '18', '-qmax', '28'])
                    print("[GPU] Detected and verified AMD AMF encoder")
                    return _GPU_ENCODER_CACHE
            # Try VAAPI (Linux)
            elif 'h264_vaapi' in encoders:
                if test_gpu_encoder('h264_vaapi', ['-qp', '23']):
                    _GPU_ENCODER_CACHE = ('h264_vaapi', ['-qp', '23'])
                    print("[GPU] Detected and verified VAAPI encoder")
                    return _GPU_ENCODER_CACHE
            # Try VideoToolbox (macOS)
            elif 'h264_videotoolbox' in encoders:
                if test_gpu_encoder('h264_videotoolbox', ['-allow_sw', '1', '-b:v', '5M']):
                    _GPU_ENCODER_CACHE = ('h264_videotoolbox', ['-allow_sw', '1', '-b:v', '5M'])
                    print("[GPU] Detected and verified VideoToolbox encoder (macOS)")
                    return _GPU_ENCODER_CACHE
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
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
    if not os.path.exists(PROJECTS_FILE):
        with open(PROJECTS_FILE, 'w', encoding='utf-8') as file:
            json.dump({'projects': []}, file, indent=2)


def timestamp():
    return datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'


def read_projects():
    ensure_project_store()
    with PROJECT_STORE_LOCK:
        with open(PROJECTS_FILE, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = {}
        projects = data.get('projects', [])
        if not isinstance(projects, list):
            projects = []
        return projects


def write_projects(projects):
    ensure_project_store()
    with PROJECT_STORE_LOCK:
        with open(PROJECTS_FILE, 'w', encoding='utf-8') as file:
            json.dump({'projects': projects}, file, indent=2)


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
    payload = request.json or {}
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
        'createdAt': now,
        'updatedAt': now
    }

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
    return jsonify(project)


@app.route('/api/projects/<project_id>', methods=['PUT'])
def update_project(project_id):
    projects = read_projects()
    project = find_project(projects, project_id)
    if not project:
        return jsonify({'error': 'Project not found'}), 404

    payload = request.json or {}

    if 'name' in payload:
        project['name'] = normalize_project_name(payload['name'], project.get('name', 'Untitled Project'))

    if isinstance(payload.get('data'), dict):
        project['data'] = json.loads(json.dumps(payload['data']))

    project['updatedAt'] = timestamp()
    write_projects(projects)
    return jsonify(project)


@app.route('/api/projects/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    projects = read_projects()
    updated_projects = [project for project in projects if project.get('id') != project_id]
    if len(updated_projects) == len(projects):
        return jsonify({'error': 'Project not found'}), 404
    write_projects(updated_projects)
    return jsonify({'status': 'deleted', 'id': project_id})


def read_collections():
    """Read collections from file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(COLLECTIONS_FILE):
        with open(COLLECTIONS_FILE, 'w', encoding='utf-8') as file:
            json.dump({'collections': []}, file, indent=2)
        return []
    try:
        with open(COLLECTIONS_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data.get('collections', [])
    except (json.JSONDecodeError, FileNotFoundError):
        return []


def write_collections(collections):
    """Write collections to file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(COLLECTIONS_FILE, 'w', encoding='utf-8') as file:
        json.dump({'collections': collections}, file, indent=2)


@app.route('/api/collections', methods=['GET'])
def get_collections():
    """Get all collections."""
    with COLLECTIONS_LOCK:
        collections = read_collections()
    return jsonify({'collections': collections})


@app.route('/api/collections', methods=['POST'])
def save_collection():
    """Save a new collection."""
    data = request.json or {}
    name = data.get('name', '').strip()
    files = data.get('files', [])
    
    if not name:
        return jsonify({'error': 'Collection name is required'}), 400
    
    if not files or not isinstance(files, list):
        return jsonify({'error': 'Files list is required'}), 400
    
    with COLLECTIONS_LOCK:
        collections = read_collections()
        
        # Check if name already exists
        if any(c.get('name') == name for c in collections):
            return jsonify({'error': 'Collection with this name already exists'}), 400
        
        collection = {
            'id': str(uuid.uuid4()),
            'name': name,
            'files': files,
            'createdAt': datetime.utcnow().isoformat() + 'Z',
            'updatedAt': datetime.utcnow().isoformat() + 'Z'
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

DEFAULT_CLIP_START_PADDING = 0.45
DEFAULT_CLIP_END_PADDING = 0.45
MAX_CLIPS_PER_SEGMENT = 25
CLIP_METADATA_FILENAME = 'metadata.json'


@app.route("/get_sentences", methods=['POST'])
def get_sentences():
    payload = request.json or {}
    selected_files = payload.get('files', [])
    if not isinstance(selected_files, list):
        selected_files = []

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
    for root in LIBRARY_ROOTS:
        for path, dirs, files in os.walk(root):
            for file in files:
                if file.lower().endswith(('.mp4', '.mkv', '.webm')):  # Filter for video files
                    file_list.append(os.path.join(path, file))
    return jsonify(file_list)


@app.route('/search', methods=['POST'])
def search():
    data = request.json or {}
    counter = sanitize_int(data.get('searchCounter'), 0)
    selected_files = data.get('files', [])
    phrases = data.get('phrases', [])
    min_silence, max_silence = extract_silence_preferences(data)
    silence_word_threshold = sanitize_int(data.get('silenceWordThreshold'), 2)
    results = process_videos(
        selected_files,
        phrases,
        counter,
        min_silence=min_silence,
        max_silence=max_silence,
        silence_word_threshold=max(silence_word_threshold, 1)
    )
    return jsonify(results)


@app.route('/search_longest_segments', methods=['POST'])
def search_longest_segments():
    data = request.json or {}
    counter = sanitize_int(data.get('sentenceSearchCounter'), 0)
    selected_files = data.get('files', [])
    sentence = (data.get('sentence') or '').strip()
    if not sentence:
        return jsonify([])

    min_silence, max_silence = extract_silence_preferences(data)
    silence_word_threshold = sanitize_int(data.get('silenceWordThreshold'), 2)

    matches = find_and_export_longest_matches(
        sentence,
        selected_files,
        counter,
        export_clips=True,
        clip_group=generate_clip_group_name('sentence', sentence, selected_files),
        min_silence=min_silence,
        max_silence=max_silence,
        silence_word_threshold=max(silence_word_threshold, 1)
    )
    print(f"Matches found: {matches}")
    response = format_matches_response(matches)

    return jsonify(response)


@app.route('/skip_segment', methods=['POST'])
def skip_segment():
    """
    Endpoint to signal that a segment should be skipped during processing.
    """
    data = request.json or {}
    search_id = data.get('search_id')
    segment_phrase = data.get('segment_phrase')
    
    if not search_id or not segment_phrase:
        return jsonify({'error': 'Missing search_id or segment_phrase'}), 400
    
    with SKIP_LOCK:
        if search_id not in SKIP_SEGMENTS:
            SKIP_SEGMENTS[search_id] = set()
        SKIP_SEGMENTS[search_id].add(segment_phrase)
    
    print(f"[Skip] ⏭️  Marked segment for skip: '{segment_phrase[:50]}...' (search_id: {search_id})")
    return jsonify({'status': 'ok', 'skipped': segment_phrase})


@app.route('/cancel_search', methods=['POST'])
def cancel_search():
    """
    Endpoint to signal that an entire search should be cancelled immediately.
    """
    data = request.json or {}
    search_id = data.get('search_id')
    
    if not search_id:
        return jsonify({'error': 'Missing search_id'}), 400
    
    with CANCEL_LOCK:
        CANCELLED_SEARCHES.add(search_id)
    
    print(f"[Cancel] ❌ Marked search for cancellation: {search_id}")
    return jsonify({'status': 'ok', 'cancelled': search_id})


def build_story_llm_prompt(user_prompt, selected_files, max_segments):
    """
    Build the LLM prompt for story generation.
    Returns tuple: (llm_prompt, corpus_entries, corpus_text)
    """
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
4. Prefer using longer segments when they fit naturally, but you can break them apart and recombine parts if needed for grammatical correctness.

Task: Create up to {max_segments} sentences that form a coherent story/narrative matching the user's prompt. You can:
- Use entire segments as-is (prefer longer segments when possible)
- Combine parts from different segments
- Rearrange words/phrases from the corpus to create grammatically correct sentences
- Focus on using longer segments where they fit naturally
- Find a good balance between long segments and short segments to create a coherent story.

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

@app.route('/generate_story_prompt', methods=['POST'])
def generate_story_prompt():
    """
    Generate the LLM prompt without calling Ollama.
    Returns the prompt that would be sent to the LLM.
    """
    data = request.json or {}
    prompt = data.get('prompt', '').strip()
    selected_files = data.get('files', [])
    max_segments = sanitize_int(data.get('maxSegments'), 10)
    
    if not prompt:
        return jsonify({'error': 'No prompt provided'}), 400
    
    if not selected_files:
        return jsonify({'error': 'No files selected'}), 400
    
    llm_prompt, corpus_entries, corpus_text = build_story_llm_prompt(prompt, selected_files, max_segments)
    
    if llm_prompt is None:
        return jsonify({'error': 'No corpus data found in selected files'}), 400
    
    return jsonify({'prompt': llm_prompt})

@app.route('/generate_story', methods=['POST'])
def generate_story():
    """
    Use Ollama LLM to generate a story by selecting segments from the corpus.
    """
    import requests
    
    data = request.json or {}
    prompt = data.get('prompt', '').strip()
    selected_files = data.get('files', [])
    max_segments = sanitize_int(data.get('maxSegments'), 10)
    prefer_long_segments = data.get('preferLongSegments', True)
    debug_mode = data.get('debugMode', False)  # Enable debug logging
    
    if not prompt:
        return jsonify({'error': 'No prompt provided'}), 400
    
    if not selected_files:
        return jsonify({'error': 'No files selected'}), 400
    
    print(f"[Story] Generating story with prompt: '{prompt[:100]}...'")
    print(f"[Story] Max segments: {max_segments}, Prefer long: {prefer_long_segments}")
    
    # Build LLM prompt using shared function
    llm_prompt, corpus_entries, corpus_text = build_story_llm_prompt(prompt, selected_files, max_segments)
    
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
        ollama_url = "http://dockerlxc.cmprmsd.local:11434/api/generate"
        ollama_payload = {
            "model": "llama3.2:latest",  # You can make this configurable
            "prompt": llm_prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_predict": 1000  # Increased for sentence generation (was 500 for numbers)
            }
        }
        
        response = requests.post(ollama_url, json=ollama_payload, timeout=120)
        response.raise_for_status()
        
        result = response.json()
        llm_response = result.get('response', '')
        
        print(f"[Story] Received LLM response ({len(llm_response)} chars)")
        print(f"[Story] LLM response: {llm_response}")
        
        # Debug: Log full LLM response
        if debug_mode:
            print("\n" + "="*80)
            print("[DEBUG] FULL LLM RESPONSE:")
            print("="*80)
            print(llm_response)
            print("="*80 + "\n")
        
        # Parse LLM response
        # Try to extract JSON from the response
        import re
        json_match = re.search(r'\{[\s\S]*\}', llm_response)
        if not json_match:
            print(f"[Story] ❌ No JSON found in LLM response: '{llm_response}'")
            return jsonify({'error': f'LLM did not return valid JSON. Response: {llm_response[:200]}'}), 500
        
        story_data = json.loads(json_match.group())
        explanation = story_data.get('explanation', '')
        llm_sentences_data = story_data.get('sentences', [])
        selected_indices = None  # For debug info
        
        # Support multiple formats for backward compatibility
        sentence_queries = []  # List of (sentence_text, search_queries) tuples
        
        if not llm_sentences_data:
            # Try legacy format with segment indices
            selected_indices = story_data.get('segments', [])
            if selected_indices:
                print(f"[Story] Using legacy format: mapping {len(selected_indices)} segment indices")
                for idx in selected_indices:
                    if 1 <= idx <= len(corpus_entries):
                        sentence_text = corpus_entries[idx - 1]['text']
                        sentence_queries.append((sentence_text, [sentence_text]))
        else:
            # Parse new format: sentences is array of objects with sentence and source_segments
            # Or old format: sentences is array of strings
            for item in llm_sentences_data:
                if isinstance(item, dict):
                    # New format: object with sentence and source_segments
                    sentence_text = item.get('sentence', '').strip()
                    source_segments = item.get('source_segments', [])
                    
                    if sentence_text and source_segments:
                        # Use source_segments for searching (exact corpus matches)
                        search_queries = [seg.strip() for seg in source_segments if seg.strip()]
                        if search_queries:
                            sentence_queries.append((sentence_text, search_queries))
                        else:
                            # Fallback: use the sentence itself if no source_segments provided
                            sentence_queries.append((sentence_text, [sentence_text]))
                    elif sentence_text:
                        # Has sentence but no source_segments - use sentence as search query
                        sentence_queries.append((sentence_text, [sentence_text]))
                elif isinstance(item, str):
                    # Old format: just a string
                    sentence_text = item.strip()
                    if sentence_text:
                        sentence_queries.append((sentence_text, [sentence_text]))
        
        if not sentence_queries:
            return jsonify({'error': 'LLM did not provide any valid sentences'}), 500
        
        print(f"[Story] LLM created {len(sentence_queries)} sentences:")
        for i, (sentence_text, search_queries) in enumerate(sentence_queries, 1):
            print(f"  {i}. '{sentence_text[:80]}...'")
            if len(search_queries) > 1 or (len(search_queries) == 1 and search_queries[0] != sentence_text):
                print(f"      (searching: {', '.join([q[:40] + '...' if len(q) > 40 else q for q in search_queries])})")
        
        # Now search for each sentence using its source_segments
        # This uses the sentence search without partial matches to find exact matches
        story_results = []
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
                        clip_group=generate_clip_group_name('story', sentence_text, selected_files),
                        min_silence=0.0,
                        max_silence=10.0,
                        silence_word_threshold=2,
                        search_id=None,  # No search_id needed for story generation
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
                    
            except Exception as e:
                print(f"[Story] Error searching for sentence '{sentence_text[:50]}...': {e}")
                traceback.print_exc()
                continue
        
        print(f"[Story] ✓ Generated story with {len(story_results)} segments")
        
        response_data = {
            'segments': story_results,
            'story_explanation': explanation
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
        else:
            # Even if debug mode is off, return the prompt so users can copy it
            response_data['debug'] = {
                'prompt': llm_prompt
            }
        
        return jsonify(response_data)
        
    except requests.exceptions.RequestException as e:
        print(f"[Story] ❌ Ollama API error: {e}")
        return jsonify({'error': f'Failed to connect to Ollama: {str(e)}'}), 500
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
    data = request.json or {}
    counter = sanitize_int(data.get('sentenceSearchCounter'), 0)
    selected_files = data.get('files', [])
    
    # Get the sentence groups if provided, otherwise fall back to single sentence
    sentence_groups = data.get('sentence_groups', [])
    if not sentence_groups:
        sentence = (data.get('sentence') or '').strip()
        if sentence:
            sentence_groups = [s.strip() for s in sentence.split(',') if s.strip()]
    
    if not sentence_groups:
        return Response('data: {"done": true}\n\n', mimetype='text/event-stream')

    min_silence, max_silence = extract_silence_preferences(data)
    silence_word_threshold = sanitize_int(data.get('silenceWordThreshold'), 2)
    include_partial_matches = data.get('includePartialMatches', False)
    all_partial_matches = data.get('allPartialMatches', False)
    max_results_per_segment = sanitize_int(data.get('maxResultsPerSegment'), 25)
    
    def generate():
        total_segment_count = 0
        total_skipped_count = 0
        
        # Process each sentence group separately
        for group_index, sentence in enumerate(sentence_groups, 1):
            if not sentence:
                continue
                
            clip_group = generate_clip_group_name('sentence', sentence, selected_files)
            search_id = f"{counter}_{clip_group}"  # Unique ID for this search
            
            # Clean up skip list for this search when starting
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
                # Clean up skip list and cancelled flag for this search
                with SKIP_LOCK:
                    SKIP_SEGMENTS.pop(search_id, None)
                with CANCEL_LOCK:
                    CANCELLED_SEARCHES.discard(search_id)
        
        # Signal completion of all searches
        print(f"[SSE] ✓ All searches completed - Total: {total_segment_count} segments, {total_skipped_count} skipped")
        yield 'data: {"done": true, "total_segments": ' + str(total_segment_count) + ', "total_skipped": ' + str(total_skipped_count) + '}\n\n'
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/search_silences_stream', methods=['POST'])
def search_silences_stream():
    """
    SSE endpoint that streams silence search results incrementally, one file at a time.
    """
    data = request.json or {}
    selected_files = data.get('files', [])
    if not selected_files:
        return Response('data: {"done": true}\n\n', mimetype='text/event-stream')

    min_silence = sanitize_float(data.get('minSilence'), 0.0)
    max_silence = sanitize_float(data.get('maxSilence'), 10.0)
    if max_silence < min_silence:
        max_silence = min_silence

    max_results_per_segment = sanitize_int(data.get('maxResultsPerSegment'), 25)
    if max_results_per_segment <= 0:
        max_results_per_segment = 25

    print(f"[Silence SSE] Searching for silences between {min_silence:.3f}s and {max_silence:.3f}s across {len(selected_files)} file(s)")

    def generate():
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
        for idx, entry in enumerate(all_silence_entries):
            filename = entry['file']
            silence_start = entry['silence_start']
            silence_end = entry['silence_end']
            gap = entry['gap']

            try:
                if not os.path.exists(filename):
                    print(f"[Silence SSE] Source video not found: {filename}")
                    continue

                video_duration = get_video_duration(filename)
                if video_duration is None:
                    print(f"[Silence SSE] Could not determine video duration for: {filename}")
                    continue

                clip_start = max(silence_start, 0.0)
                clip_end = min(silence_end, video_duration)
                if clip_end <= clip_start:
                    print(f"[Silence SSE] Invalid clip range for silence: start={clip_start}, end={clip_end}, video_duration={video_duration}")
                    continue

                clip_filename = f"{clip_group_name}_{idx:05d}.mp4"
                output_path = os.path.join(clip_output_dir, clip_filename)

                # Use ffmpeg to export the silence region
                ffmpeg_cmd = [
                    'ffmpeg', '-y',
                    '-ss', str(clip_start),
                    '-i', filename,
                    '-t', str(clip_end - clip_start),
                    '-c:v', 'libx264',
                    '-c:a', 'aac',
                    '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                    output_path
                ]

                process = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if process.returncode != 0:
                    stderr = process.stderr.decode(errors='ignore') if process.stderr else ''
                    print(f"[Silence SSE] FFmpeg failed for '{filename}' silence {idx}: {stderr}")
                    continue

                if not os.path.exists(output_path):
                    print(f"[Silence SSE] Exported clip not found at {output_path}")
                    continue

                relative_path = posixpath.join('temp', clip_output_dir_name, os.path.basename(output_path))

                # Get the actual duration of the exported clip
                clip_duration_ms = get_video_duration_ms(output_path) if os.path.exists(output_path) else None
                
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

                # Stream this bucket as a result (only when first file is added, or periodically)
                # Send update for this bucket
                result = {
                    'phrase': label,
                    'files': buckets[label]['files'].copy(),  # Send current state
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




def format_matches_response(matches):
    if not matches:
        return []

    grouped_matches = {}
    ordered_segments = []

    def register_match(segment, file_reference, source_video=None, duration_ms=None):
        if not segment or not file_reference:
            return
        if segment not in grouped_matches:
            grouped_matches[segment] = []
            ordered_segments.append(segment)
        
        # Create match object with source video info and duration
        match_obj = {'file': file_reference}
        if source_video:
            match_obj['source_video'] = source_video
        if duration_ms is not None:
            match_obj['duration_ms'] = duration_ms
        
        # Avoid duplicates
        if not any(m.get('file') == file_reference for m in grouped_matches[segment]):
            grouped_matches[segment].append(match_obj)

    def handle_entry(entry):
        if isinstance(entry, dict):
            register_match(entry.get('segment'), entry.get('file'), entry.get('source_video'), entry.get('duration_ms'))
        elif isinstance(entry, (list, tuple, set)):
            for nested_entry in entry:
                handle_entry(nested_entry)

    handle_entry(matches)

    # Apply MAX_CLIPS_PER_SEGMENT limit per segment/phrase card
    # This ensures each card shows up to 25 matches, but all segments are included
    result = []
    for segment in ordered_segments:
        files = grouped_matches[segment]
        # Sort by length (longest segments first) to prioritize better matches
        segment_word_count = len(segment.split())
        # Limit each segment to MAX_CLIPS_PER_SEGMENT matches
        limited_files = files[:MAX_CLIPS_PER_SEGMENT]
        result.append({
            'phrase': segment, 
            'files': limited_files,
            'word_count': segment_word_count  # For potential frontend sorting
        })
    
    # Sort results by segment length (longest first) for better UX
    result.sort(key=lambda x: x.get('word_count', 0), reverse=True)
    
    return result


def normalize_token(token):
    if not isinstance(token, str):
        return ''
    normalized = unicodedata.normalize('NFKC', token).lower().strip()
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
    except (TypeError, ValueError):
        return default


def get_video_duration(video_path):
    """Get the duration of a video file in seconds using ffprobe."""
    if not os.path.exists(video_path):
        return None
    try:
        duration_command = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{video_path}"'
        duration_result = subprocess.run(duration_command, shell=True, capture_output=True, text=True, timeout=10)
        if duration_result.returncode == 0 and duration_result.stdout.strip():
            return float(duration_result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError, subprocess.SubprocessError) as e:
        print(f"Error getting video duration for {video_path}: {e}")
    return None


def get_video_duration_ms(video_path):
    """Get the duration of a video file in milliseconds using ffprobe."""
    duration_sec = get_video_duration(video_path)
    if duration_sec is not None:
        return int(round(duration_sec * 1000))
    return None


def get_video_fps(video_path):
    """Get the fps of a video file using ffprobe."""
    if not os.path.exists(video_path):
        return None
    try:
        fps_command = f'ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of default=noprint_wrappers=1:nokey=1 "{video_path}"'
        fps_result = subprocess.run(fps_command, shell=True, capture_output=True, text=True, timeout=10)
        if fps_result.returncode == 0 and fps_result.stdout.strip():
            # fps might be in format like "24000/1001" or "25/1"
            fps_str = fps_result.stdout.strip()
            if '/' in fps_str:
                num, den = fps_str.split('/')
                return float(num) / float(den)
            else:
                return float(fps_str)
    except (subprocess.TimeoutExpired, ValueError, subprocess.SubprocessError) as e:
        print(f"Error getting video fps for {video_path}: {e}")
    return None


def extract_silence_preferences(payload, default_min=0.0, default_max=10.0):
    min_silence = sanitize_float(payload.get('minSilence', default_min), default_min)
    max_silence = sanitize_float(payload.get('maxSilence', default_max), default_max)
    if max_silence < min_silence:
        max_silence = min_silence
    return min_silence, max_silence


def _collect_name_variants(video_path: Path) -> List[str]:
    parts = video_path.name.split('.')
    variants: List[str] = []
    total = len(parts)
    for index in range(total):
        candidate = '.'.join(parts[: total - index])
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def _find_related_files(video_path: str, extensions: Sequence[str]) -> List[Path]:
    source_path = Path(video_path)
    parent = source_path.parent
    # Get base name without extension (e.g., "video.mp4" -> "video")
    base_name = source_path.stem
    
    candidates: List[Path] = []

    for ext in extensions:
        normalized_ext = ext if ext.startswith('.') else f'.{ext}'
        # Only look for exact match: base_name + extension
        # This prevents matching files with similar prefixes
        exact_match = parent / f'{base_name}{normalized_ext}'
        candidates.append(exact_match)

    unique: List[Path] = []
    seen = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(candidate)
    return unique


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


def load_transcript_sentences(video_path: str) -> Optional[List[dict]]:
    try:
        sentences = parse_transcript(video_path)
        if sentences:
            return sentences
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"Failed to parse transcript via videogrep for '{video_path}': {exc}")

    json_candidates = _find_related_files(video_path, ['.json'])
    for json_path in json_candidates:
        sentences = _load_json_transcript(json_path)
        if sentences:
            return sentences

    subtitle_candidates = _find_related_files(video_path, ['.vtt', '.srt'])
    for subtitle_path in subtitle_candidates:
        try:
            output_path = subtitle_path.with_suffix('.json')
        except ValueError:
            output_path = subtitle_path.parent / f"{subtitle_path.stem}.json"
        try:
            segments, _stats = convert_subtitles(subtitle_path, output_path)
        except Exception as exc:
            print(f"Failed to convert subtitles '{subtitle_path}' for '{video_path}': {exc}")
            continue
        if segments:
            return segments
        generated = _load_json_transcript(output_path)
        if generated:
            return generated

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


def process_videos(selected_files, phrases, counter, min_silence=0.0, max_silence=10.0, silence_word_threshold=2):
    results = []

    for index, phrase in enumerate(phrases):
        cleaned_phrase = phrase.strip()
        if not cleaned_phrase:
            continue

        matches = find_and_export_longest_matches(
            cleaned_phrase,
            selected_files,
            counter,
            export_clips=True,
            clip_group=generate_clip_group_name('phrase', cleaned_phrase, selected_files),
            min_silence=min_silence,
            max_silence=max_silence,
            silence_word_threshold=silence_word_threshold
        )

        grouped_matches = format_matches_response(matches)
        if not grouped_matches:
            continue

        results.extend(grouped_matches)

    return results


@app.route('/merge_videos', methods=['POST'])
def merge_videos_route():
    data = request.json
    video_info = data['videos']
    # 'video_info' should be a list of dictionaries with 'video', 'startTrim', and 'endTrim'

    print(f"[Merge] Received {len(video_info)} videos to merge")
    for idx, video in enumerate(video_info):
        print(f"[Merge]   [{idx+1}] {video.get('title', 'Unknown')}: {video.get('video', 'No file')} (trim: {video.get('startTrim', 0)}ms - {video.get('endTrim', 0)}ms)")

    # Generate filename: timestamp + first 5 words (to avoid "File name too long" error)
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    
    # Get all titles and join them, then take first 5 words
    all_titles = " ".join([video['title'] for video in video_info])
    words = all_titles.split()[:5]  # Take only first 5 words
    name_part = "_".join(words).replace(" ", "_").replace("/", "_").replace("\\", "_")
    
    # Limit name part length to 100 chars (additional safety)
    if len(name_part) > 100:
        name_part = name_part[:100]
    
    filename = f"{timestamp}_{name_part}.mp4"
    output_path = os.path.join(TEMP_DIR, filename)

    # Use the progress version but consume all updates
    for _ in merge_videos_with_progress(video_info, output_path):
        pass  # Consume all progress updates
    return jsonify({'merged_video': os.path.join('temp', filename)})


@app.route('/merge_videos_stream', methods=['POST'])
def merge_videos_stream():
    """SSE endpoint that streams merge progress updates."""
    data = request.json
    video_info = data['videos']
    
    print(f"[Merge SSE] Received {len(video_info)} videos to merge")
    
    # Generate filename
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    all_titles = " ".join([video['title'] for video in video_info])
    words = all_titles.split()[:5]
    name_part = "_".join(words).replace(" ", "_").replace("/", "_").replace("\\", "_")
    if len(name_part) > 100:
        name_part = name_part[:100]
    filename = f"{timestamp}_{name_part}.mp4"
    output_path = os.path.join(TEMP_DIR, filename)
    
    def generate():
        total_videos = len(video_info)
        
        try:
            # Stream progress updates during merge
            for progress_update in merge_videos_with_progress(video_info, output_path):
                try:
                    yield f'data: {json.dumps(progress_update)}\n\n'
                except (GeneratorExit, BrokenPipeError, ConnectionError):
                    print(f"[Merge SSE] ❌ Client disconnected")
                    return
            
            # Signal completion
            yield f'data: {{"done": true, "merged_video": "temp/{filename}"}}\n\n'
        except Exception as e:
            print(f"[Merge SSE] ❌ Error during merge: {e}")
            traceback.print_exc()
            try:
                yield f'data: {{"error": "{str(e)}"}}\n\n'
            except (GeneratorExit, BrokenPipeError, ConnectionError):
                return
    
    return Response(stream_with_context(generate()), mimetype='text/event-stream')


def merge_videos_with_progress(videos, output_path):
    """Generator version of merge_videos that yields progress updates."""
    temp_files = []
    total_videos = len(videos)
    
    # First pass: detect all video resolutions to determine target resolution
    yield {'stage': 'detecting_resolutions', 'message': 'Detecting video resolutions...', 'progress': 0, 'total': total_videos}
    
    resolutions = []
    for i, video in enumerate(videos):
        video_path = video['video']
        if video_path.startswith('temp/'):
            input_path = os.path.join(app.root_path, video_path)
        else:
            input_path = os.path.join(app.root_path, 'static', video_path)
        
        if not os.path.exists(input_path):
            continue
            
        # Get video resolution
        probe_command = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]
        probe_result = subprocess.run(probe_command, capture_output=True, text=True)
        if probe_result.returncode == 0:
            lines = probe_result.stdout.strip().split('\n')
            if len(lines) >= 2:
                width = int(lines[0])
                height = int(lines[1])
                resolutions.append((width, height))
    
    # Determine target resolution (use most common, or first if all different)
    if resolutions:
        # Count occurrences
        from collections import Counter
        resolution_counts = Counter(resolutions)
        target_resolution = resolution_counts.most_common(1)[0][0]
        target_width, target_height = target_resolution
        print(f"[Merge] Target resolution: {target_width}x{target_height} (most common: {resolution_counts[target_resolution]} videos)")
    else:
        # Fallback to 1920x1080 if we can't detect
        target_width, target_height = 1920, 1080
        print(f"[Merge] Using default target resolution: {target_width}x{target_height}")

    for i, video in enumerate(videos):
        # Handle both temp/ and static/ paths
        video_path = video['video']
        if video_path.startswith('temp/'):
            input_path = os.path.join(app.root_path, video_path)
        else:
            input_path = os.path.join(app.root_path, 'static', video_path)
        
        # Validate file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Video file not found: {input_path} (from path: {video_path})")
        
        start_trim = video['startTrim'] / 1000  # Convert ms to seconds
        end_trim = video['endTrim'] / 1000
        
        print(f"[Merge] Video {i+1}: {video_path}, start_trim={start_trim}s, end_trim={end_trim}s")
        
        # Yield progress update
        video_title = video.get('title', f'Video {i+1}')
        yield {
            'stage': 'processing_videos',
            'message': f'Processing: {video_title} ({i+1}/{total_videos})',
            'progress': i,
            'total': total_videos,
            'current_video': video_title
        }

        temp_file = os.path.join(TEMP_DIR, f'temp_merge_{i}.mp4')
        temp_files.append(temp_file)

        # Use ffprobe to get the total duration of the video
        duration_command = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]
        duration_result = subprocess.run(duration_command, capture_output=True, text=True)
        
        if duration_result.returncode != 0:
            raise RuntimeError(f"Failed to get video duration: {duration_result.stderr}")
        
        duration_str = duration_result.stdout.strip()
        if not duration_str:
            raise ValueError(f"Empty duration result for video: {input_path}")
        
        total_duration = float(duration_str)

        # Calculate the duration after applying both trims
        # Duration = total_duration - start_trim - end_trim
        output_duration = total_duration - start_trim - end_trim
        
        print(f"[Merge] Video {i+1}: total_duration={total_duration}s, output_duration={output_duration}s")
        
        # Ensure duration is positive
        if output_duration <= 0:
            raise ValueError(f"Invalid trim values: start_trim={start_trim}, end_trim={end_trim}, total_duration={total_duration}")
        
        # Ensure minimum duration for proper encoding (ffmpeg needs at least ~0.1s for video)
        MIN_DURATION = 0.1
        if output_duration < MIN_DURATION:
            print(f"[Merge] Warning: Video {i+1} output duration ({output_duration}s) is very short. Adjusting to minimum {MIN_DURATION}s")
            # Adjust start_trim to allow minimum duration if possible
            if total_duration >= start_trim + MIN_DURATION:
                start_trim = max(0, total_duration - MIN_DURATION - end_trim)
                output_duration = total_duration - start_trim - end_trim
                print(f"[Merge] Adjusted start_trim to {start_trim}s, new output_duration: {output_duration}s")
            else:
                # If video is too short, use the entire video
                start_trim = 0
                end_trim = 0
                output_duration = total_duration
                print(f"[Merge] Video too short, using entire video: {output_duration}s")

        # Get current video resolution to check if scaling is needed
        probe_command = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]
        probe_result = subprocess.run(probe_command, capture_output=True, text=True)
        current_width, current_height = target_width, target_height
        if probe_result.returncode == 0:
            lines = probe_result.stdout.strip().split('\n')
            if len(lines) >= 2:
                current_width = int(lines[0])
                current_height = int(lines[1])
        
        # Get speed (default 1.0 = 100%)
        speed = video.get('speed', 1.0)
        if speed is None or speed <= 0:
            speed = 1.0
        # Clamp speed to valid range (0.5 to 1.0)
        speed = max(0.5, min(1.0, speed))
        
        # Trim and re-encode video segment
        # Use -t before -i to limit INPUT duration (allows speed filter to extend output)
        # Add -vsync cfr and -r to ensure consistent frame rate for short segments
        # Scale to target resolution if needed
        trim_command = [
            'ffmpeg', '-y',
            '-ss', str(start_trim),
            '-t', str(output_duration),  # Limit INPUT duration (before -i)
            '-i', input_path,
        ]
        
        # Build video filter chain
        video_filters = []
        audio_filters = []
        
        # Calculate final output duration for logging
        final_output_duration = output_duration / speed
        
        # Add speed adjustment if not 1.0
        if speed != 1.0:
            # For video: setpts=PTS/speed (e.g., 0.5 speed -> setpts=PTS/0.5 = PTS*2)
            video_filters.append(f'setpts=PTS/{speed}')
            # For audio: atempo=speed (atempo accepts 0.5 to 2.0)
            audio_filters.append(f'atempo={speed}')
            print(f"[Merge] Video {i+1}: Applying speed={speed} ({int(speed*100)}%), input={output_duration:.2f}s -> output={final_output_duration:.2f}s")
        
        # Add scale filter if resolution differs from target
        # Use crop-to-fill instead of letterboxing to avoid black bars
        if current_width != target_width or current_height != target_height:
            # Scale up to cover target, then crop to exact size (no black bars)
            video_filters.append(f'scale={target_width}:{target_height}:force_original_aspect_ratio=increase,crop={target_width}:{target_height}')
            print(f"[Merge] Video {i+1}: Scaling from {current_width}x{current_height} to {target_width}x{target_height} (crop-to-fill)")
        
        # Apply video filters if any
        if video_filters:
            trim_command.extend(['-vf', ','.join(video_filters)])
        
        # Apply audio filters if any
        if audio_filters:
            trim_command.extend(['-af', ','.join(audio_filters)])
        
        trim_command.extend([
            '-c:v', 'libx264',
            '-preset', 'medium',  # Better quality (was ultrafast)
            '-crf', '18',  # High quality (lower = better, 18 is visually lossless)
            '-c:a', 'aac',
            '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
            '-b:a', '192k',  # Higher audio bitrate for quality
            '-vsync', 'cfr',  # Constant frame rate
            '-r', '30',  # 30fps for smoother playback
            '-pix_fmt', 'yuv420p',  # Ensure compatible pixel format
            temp_file
        ])
        trim_result = subprocess.run(trim_command, capture_output=True, text=True)
        if trim_result.returncode != 0:
            raise RuntimeError(f"Failed to trim video segment {i+1}: {trim_result.stderr}")
        
        # Verify the output file exists and has valid video/audio
        if not os.path.exists(temp_file):
            raise FileNotFoundError(f"Trimmed video file was not created: {temp_file}")
        
        # Check if file has video stream using ffprobe
        probe_command = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=codec_type',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            temp_file
        ]
        probe_result = subprocess.run(probe_command, capture_output=True, text=True)
        if probe_result.returncode != 0 or 'video' not in probe_result.stdout.lower():
            raise RuntimeError(f"Trimmed video {i+1} does not contain a valid video stream. FFmpeg output: {trim_result.stderr}")
        
        print(f"[Merge] ✓ Video {i+1} trimmed successfully: {temp_file} ({output_duration}s)")
        
        # Yield progress after each video is processed
        yield {
            'stage': 'processing_videos',
            'message': f'Completed: {video_title} ({i+1}/{total_videos})',
            'progress': i + 1,
            'total': total_videos,
            'current_video': video_title
        }

    # Merge re-encoded segments
    yield {'stage': 'merging', 'message': 'Merging all videos together...', 'progress': total_videos, 'total': total_videos}
    
    file_list_path = os.path.join(TEMP_DIR, 'file_list.txt')
    print(f"[Merge] Writing file list with {len(temp_files)} files:")
    with open(file_list_path, 'w') as file:
        for idx, temp_file in enumerate(temp_files):
            abs_path = os.path.abspath(temp_file)
            file.write(f"file '{abs_path}'\n")
            print(f"[Merge]   [{idx+1}] {abs_path}")
            # Verify file exists
            if not os.path.exists(temp_file):
                raise FileNotFoundError(f"Temp file missing: {temp_file}")

    # Use cached GPU encoder (detected at startup)
    gpu_encoder, gpu_args = get_gpu_encoder()
    
    merge_command = ['ffmpeg', '-y']
    
    # For NVENC, we don't use -hwaccel cuda as it can cause error 244
    # NVENC encoding works fine without hardware-accelerated decoding
    
    merge_command.extend([
        '-f', 'concat',
        '-safe', '0',
        '-i', file_list_path,
        '-c:a', 'aac',
        '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
        '-b:a', '192k',  # Higher audio bitrate for quality
        '-vsync', 'cfr',  # Constant frame rate for all segments
        '-r', '30',  # 30fps for smoother playback
        '-pix_fmt', 'yuv420p',  # Ensure compatible pixel format
    ])
    
    # Add video encoder (GPU or CPU) with high quality settings
    if gpu_encoder:
        merge_command.extend(['-c:v', gpu_encoder])
        merge_command.extend(gpu_args)
    else:
        merge_command.extend(['-c:v', 'libx264', '-preset', 'medium', '-crf', '18'])
    
    merge_command.append(output_path)
    merge_result = subprocess.run(merge_command, capture_output=True, text=True)
    if merge_result.returncode != 0:
        print(f"[Merge] FFmpeg merge error output: {merge_result.stderr}")
        raise RuntimeError(f"Failed to merge videos: {merge_result.stderr}")
    
    # Verify the merged file exists and has video
    if not os.path.exists(output_path):
        raise FileNotFoundError(f"Merged video file was not created: {output_path}")
    
    # Verify merged file has video stream
    probe_command = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_type',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        output_path
    ]
    probe_result = subprocess.run(probe_command, capture_output=True, text=True)
    if probe_result.returncode != 0 or 'video' not in probe_result.stdout.lower():
        raise RuntimeError(f"Merged video does not contain a valid video stream. FFmpeg output: {merge_result.stderr}")
    
    print(f"[Merge] ✓ Successfully merged {len(temp_files)} video segments to: {output_path}")
    
    yield {'stage': 'complete', 'message': 'Merge completed successfully!', 'progress': total_videos + 1, 'total': total_videos + 1}

    # Clean up temporary files
    for temp_file in temp_files:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except OSError:
            pass
    try:
        if os.path.exists(file_list_path):
            os.remove(file_list_path)
    except OSError:
        pass


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
    global_clip_counter = 0
    clip_output_dir = None
    clip_output_dir_name = None
    clip_group_name = clip_group or generate_clip_group_name('segment', input_sentence, filenames)
    silence_word_threshold = max(silence_word_threshold, 1)
    skip_export = False
    clip_metadata_path = None
    cached_clip_files = []
    cached_clip_index = 0

    if export_clips:
        clip_output_dir_name = f'{clip_group_name}_clips'
        clip_output_dir = os.path.join(TEMP_DIR, clip_output_dir_name)
        clip_metadata_path = os.path.join(clip_output_dir, CLIP_METADATA_FILENAME)
        current_metadata = {
            'version': 1,
            'source_files': sorted(map(str, filenames)),
            'input_sentence': input_sentence,
            'max_results': max_results_per_segment
        }
        stored_metadata = None
        if os.path.exists(clip_metadata_path):
            try:
                with open(clip_metadata_path, 'r', encoding='utf-8') as metadata_file:
                    stored_metadata = json.load(metadata_file)
            except (OSError, json.JSONDecodeError):
                stored_metadata = None

        if stored_metadata and stored_metadata.get('source_files') == current_metadata['source_files']:
            skip_export = True
            if os.path.isdir(clip_output_dir):
                try:
                    cached_clip_files = sorted(
                        [
                            name for name in os.listdir(clip_output_dir)
                            if name.lower().endswith('.mp4')
                        ]
                    )
                except OSError:
                    cached_clip_files = []
                if cached_clip_files:
                    verified_clip_files = []
                    for cached_name in cached_clip_files:
                        cached_path = os.path.join(clip_output_dir, cached_name)
                        if os.path.isfile(cached_path):
                            verified_clip_files.append(cached_name)
                    if len(verified_clip_files) != len(cached_clip_files):
                        cached_clip_files = verified_clip_files
                        if not cached_clip_files:
                            skip_export = False
                            shutil.rmtree(clip_output_dir, ignore_errors=True)
            else:
                skip_export = False
                cached_clip_files = []
        else:
            if os.path.isdir(clip_output_dir):
                shutil.rmtree(clip_output_dir)
            os.makedirs(clip_output_dir, exist_ok=True)

        if skip_export:
            required_count = min(len(cached_clip_files), max_results_per_segment)
            if required_count == 0:
                skip_export = False
                shutil.rmtree(clip_output_dir, ignore_errors=True)
                os.makedirs(clip_output_dir, exist_ok=True)
                cached_clip_files = []
            else:
                cached_clip_files = cached_clip_files[:max_results_per_segment]

    # First, search ALL files and collect ALL matches
    all_file_matches = []
    for filename in filenames:
        sentences = load_transcript_sentences(filename)
        # Skip files without valid transcript data
        if not sentences:
            continue
        # Validate that we have actual transcript segments with content
        if not isinstance(sentences, list) or len(sentences) == 0:
            continue
        # Check that at least one segment has actual content
        has_valid_content = False
        for segment in sentences:
            if isinstance(segment, dict):
                content = segment.get('content', '')
                words = segment.get('words', [])
                if (isinstance(content, str) and content.strip()) or (isinstance(words, list) and len(words) > 0):
                    has_valid_content = True
                    break
        if not has_valid_content:
            continue

        # Optimize: if only full matches needed, only search for the full sentence
        # This avoids searching all substrings like "aber", "das", etc.
        if not include_partial_matches:
            # Only search for the exact full sentence, not all substrings
            matches = find_matches_for_segment(
                input_sentence,
                sentences,
                min_duration=min_silence if len(input_sentence.split()) < silence_word_threshold else 0.0,
                max_duration=max_silence,
                silence_word_threshold=silence_word_threshold
            )
            # Wrap in the expected format (find_matches_for_segment already returns the right format)
        elif not all_partial_matches:
            # Include partial matches but filter out very short ones during search
            # Only search for segments that are 3+ words or are the full sentence
            matches = find_longest_matches_filtered(
                input_sentence,
                sentences,
                min_silence=min_silence,
                max_silence=max_silence,
                silence_word_threshold=silence_word_threshold,
                min_words=3  # Minimum word count for partial matches
            )
        else:
            # All partial matches - search everything
            matches = find_longest_matches(
                input_sentence,
                sentences,
                min_silence=min_silence,
                max_silence=max_silence,
                silence_word_threshold=silence_word_threshold
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
    
    # Use cached GPU encoder (detected at startup)
    gpu_encoder, gpu_args = get_gpu_encoder()
    
    def render_single_clip(clip_data):
        """
        Render a single clip. Returns (success, output_path, error) tuple.
        This function is designed to be called in parallel.
        """
        clip_idx, item, clip_filename, output_path = clip_data
        filename = item['file']
        match = item['match']
        
        try:
            # Validate file exists
            if not os.path.exists(filename):
                raise FileNotFoundError(f"Video file not found: {filename}")
            
            match_start = sanitize_float(match.get('start'), 0.0)
            match_end = sanitize_float(match.get('end'), match_start)
            
            # Validate that we have valid start and end times
            if match_start is None or match_end is None:
                raise ValueError(f"Invalid time values: start={match.get('start')}, end={match.get('end')}")
            
            # Get video duration to clamp clip times
            video_duration = get_video_duration(filename)
            if video_duration is None:
                raise ValueError(f"Could not determine video duration for: {filename}")
            
            clip_start = max(match_start - DEFAULT_CLIP_START_PADDING, 0.0)
            clip_end = match_end + DEFAULT_CLIP_END_PADDING
            
            # Clamp clip_start and clip_end to video duration
            clip_start = min(clip_start, video_duration - 0.1)  # Leave at least 0.1s
            clip_end = min(clip_end, video_duration)
            
            if clip_end <= clip_start:
                clip_duration = max(match_end - match_start, 0.0)
                min_extension = DEFAULT_CLIP_START_PADDING + DEFAULT_CLIP_END_PADDING
                clip_end = min(clip_start + max(clip_duration + min_extension, 0.1), video_duration)
            
            # Final validation
            if clip_start >= clip_end:
                raise ValueError(f"Invalid clip range: start={clip_start}, end={clip_end}, video_duration={video_duration}")
            
            # Get video fps for proper export
            video_fps = get_video_fps(filename)
            if video_fps is None:
                video_fps = 25
            
            # Try to use GPU encoding if available, but respect the concurrent session limit
            # Use a semaphore to limit GPU encoding to MAX_GPU_STREAMS concurrent sessions
            # If GPU is not available or semaphore is full, fall back to CPU encoding
            current_gpu_encoder, current_gpu_args = get_gpu_encoder()
            use_gpu = False
            
            if current_gpu_encoder and GPU_ENCODING_SEMAPHORE:
                # Try to acquire GPU semaphore (non-blocking check first)
                if GPU_ENCODING_SEMAPHORE.acquire(blocking=False):
                    use_gpu = True
                # If semaphore not available, will use CPU encoding
            
            ffmpeg_cmd = ['ffmpeg', '-y']
            
            # For NVENC, we don't use -hwaccel cuda by default as it can cause error 244
            # when the input format doesn't support CUDA decoding. NVENC encoding works
            # fine without hardware-accelerated decoding - FFmpeg will decode on CPU and
            # encode on GPU, which is still much faster than full CPU encoding.
            # If needed, we can add -hwaccel cuda conditionally based on input format detection.
            
            ffmpeg_cmd.extend([
                '-ss', str(clip_start),
                '-i', filename,
                '-t', str(clip_end - clip_start),
                '-c:a', 'aac',
                '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                '-r', str(video_fps),
            ])
            
            # Add video encoder (GPU or CPU)
            try:
                if use_gpu:
                    ffmpeg_cmd.extend(['-c:v', current_gpu_encoder])
                    ffmpeg_cmd.extend(current_gpu_args)
                else:
                    ffmpeg_cmd.extend(['-c:v', 'libx264', '-preset', 'ultrafast'])
            finally:
                # Always release the semaphore if we acquired it
                if use_gpu and GPU_ENCODING_SEMAPHORE:
                    GPU_ENCODING_SEMAPHORE.release()
            
            ffmpeg_cmd.append(output_path)
            
            # Run ffmpeg
            result = subprocess.run(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300  # 5 minute timeout per clip
            )
            
            if result.returncode != 0:
                stderr = result.stderr.decode() if result.stderr else ''
                
                # Log detailed error information for debugging (for both 187 and 244)
                if current_gpu_encoder and result.returncode in [187, 244]:
                    error_name = "187 (Encoder Initialization)" if result.returncode == 187 else "244 (Invalid Parameter)"
                    print(f"[GPU] Error {error_name} for {clip_filename}")
                    print(f"[GPU] Encoder: {current_gpu_encoder}, Args: {current_gpu_args}")
                    print(f"[GPU] FFmpeg command: {' '.join(ffmpeg_cmd)}")
                    
                    # Check for specific error types and provide helpful messages
                    if 'incompatible client key' in stderr.lower():
                        print(f"[GPU] ⚠️  DRIVER/SDK COMPATIBILITY ISSUE DETECTED")
                        print(f"[GPU] The NVIDIA driver version is incompatible with FFmpeg's NVENC SDK.")
                        print(f"[GPU] Solutions:")
                        print(f"[GPU]   1. Update your NVIDIA driver to the latest version")
                        print(f"[GPU]   2. Or recompile FFmpeg with a compatible NVENC SDK version")
                        print(f"[GPU]   3. GPU encoding will be disabled and CPU encoding will be used")
                    
                    print(f"[GPU] Full error output:")
                    print("=" * 80)
                    print(stderr)
                    print("=" * 80)
                
                # Check if it's a GPU-specific error
                # Return codes 187 and 244 often indicate encoder initialization/parameter failures
                if current_gpu_encoder and (result.returncode in [187, 244] or any(err.lower() in stderr.lower() for err in [
                    'nvenc', 'No capable devices', 'incompatible client key', 
                    'OpenEncodeSessionEx failed', 'No device', 'failed to initialize',
                    'Error while opening encoder', 'Could not open encoder', 'encoder initialization',
                    'Invalid parameter', 'invalid parameter'
                ])):
                    # Try to fix the issue by using simpler parameters instead of disabling GPU
                    if result.returncode == 244 and 'nvenc' in current_gpu_encoder.lower():
                        # Error 244 = Invalid parameter, try with minimal parameters
                        print(f"[GPU] Attempting to fix error 244 by using minimal NVENC parameters for: {clip_filename}")
                        ffmpeg_cmd_minimal = ['ffmpeg', '-y']
                        
                        # Try without -hwaccel cuda first (some inputs don't support it)
                        ffmpeg_cmd_minimal.extend([
                            '-ss', str(clip_start),
                            '-i', filename,
                            '-t', str(clip_end - clip_start),
                            '-c:v', 'h264_nvenc',
                            '-preset', 'p4',  # Use a more compatible preset
                            '-c:a', 'aac',
                            '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                            '-r', str(video_fps),
                            output_path
                        ])
                        
                        result_minimal = subprocess.run(
                            ffmpeg_cmd_minimal,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            timeout=300
                        )
                        
                        if result_minimal.returncode == 0:
                            print(f"[GPU] Successfully encoded with minimal parameters: {clip_filename}")
                            return (True, output_path, None)
                        else:
                            stderr_minimal = result_minimal.stderr.decode() if result_minimal.stderr else ''
                            print(f"[GPU] Minimal parameters also failed (code {result_minimal.returncode}): {stderr_minimal[-500:]}")
                    
                    # If we can't fix it, disable GPU and fall back to CPU
                    disable_gpu_encoder()
                    # Retry with CPU encoding
                    print(f"[GPU] GPU encoding failed (code {result.returncode}), retrying with CPU: {clip_filename}")
                    ffmpeg_cmd_cpu = [
                        'ffmpeg', '-y',
                        '-ss', str(clip_start),
                        '-i', filename,
                        '-t', str(clip_end - clip_start),
                        '-c:v', 'libx264',
                        '-preset', 'ultrafast',
                        '-c:a', 'aac',
                        '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                        '-r', str(video_fps),
                        output_path
                    ]
                    result = subprocess.run(
                        ffmpeg_cmd_cpu,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=300
                    )
                    if result.returncode != 0:
                        stderr_cpu = result.stderr.decode() if result.stderr else ''
                        raise RuntimeError(f"FFmpeg failed with return code {result.returncode}: {stderr_cpu[:500]}")
                else:
                    raise RuntimeError(f"FFmpeg failed with return code {result.returncode}: {stderr[:500]}")
            
            if not os.path.exists(output_path):
                raise FileNotFoundError(f"Exported clip not found: {output_path}")
            
            return (True, output_path, None)
        except Exception as e:
            return (False, None, str(e))
    
    # Process each segment and yield results incrementally
    for segment_idx, segment in enumerate(sorted_segments, 1):
        segment_matches = segment_groups[segment][:max_results_per_segment]
        
        progress_msg = f"Processing segment {segment_idx}/{len(sorted_segments)}: '{segment}' ({len(segment_matches)} matches)"
        print(progress_msg)
        
        # Yield progress update FIRST so user can skip
        yield {
            'progress': progress_msg,
            'segment_index': segment_idx,
            'total_segments': len(sorted_segments),
            'segment_phrase': segment  # Include phrase so frontend can use it
        }
        
        segment_results = []
        segment_skipped = False  # Track if segment was skipped during processing
        
        # Check for skip/cancel before starting parallel rendering
        if search_id:
            with CANCEL_LOCK:
                if search_id in CANCELLED_SEARCHES:
                    print(f"[Generator] ❌ CANCELLED before clip processing - exiting immediately")
                    raise GeneratorExit("Search cancelled by user")
            
            with SKIP_LOCK:
                if segment in SKIP_SEGMENTS.get(search_id, set()):
                    print(f"[Generator] ⏭️  SKIP detected before processing segment: '{segment}'")
                    segment_skipped = True
        
        if not segment_skipped and export_clips and clip_output_dir:
            # Prepare clip data for parallel rendering
            clip_tasks = []
            for clip_idx, item in enumerate(segment_matches):
                clip_filename = f"{clip_group_name}_{global_clip_counter + clip_idx:05d}.mp4"
                output_path = os.path.join(clip_output_dir, clip_filename)
                clip_tasks.append((clip_idx, item, clip_filename, output_path))
            
            # Render clips in parallel using both GPU and CPU
            # GPU encoding is limited to 2 concurrent sessions (NVENC limit) via semaphore
            # CPU encoding can use all remaining cores
            # This allows 2 GPU + (cores-2) CPU tasks to run in parallel
            max_workers = get_max_workers()  # Use all CPU cores
            current_gpu_encoder, _ = get_gpu_encoder()
            if current_gpu_encoder:
                gpu_limit = get_max_gpu_workers()
                print(f"[Parallel] Using hybrid encoding: {gpu_limit} GPU workers ({current_gpu_encoder}) + {max_workers - gpu_limit} CPU workers (libx264) = {max_workers} total workers")
            else:
                print(f"[Parallel] Using CPU encoding only, using {max_workers} concurrent workers")
            
            completed_count = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_clip = {
                    executor.submit(render_single_clip, task): task 
                    for task in clip_tasks
                }
                
                print(f"[Parallel] Submitted {len(future_to_clip)} clip rendering tasks with {max_workers} workers")
                
                # Process completed tasks as they finish
                for future in as_completed(future_to_clip):
                    # Check for cancel/skip periodically (but don't break - process all tasks)
                    should_cancel = False
                    if search_id:
                        with CANCEL_LOCK:
                            if search_id in CANCELLED_SEARCHES:
                                should_cancel = True
                                print(f"[Generator] ❌ CANCELLED during parallel rendering")
                        
                        if not should_cancel:
                            with SKIP_LOCK:
                                if segment in SKIP_SEGMENTS.get(search_id, set()):
                                    should_cancel = True
                                    segment_skipped = True
                                    print(f"[Generator] ⏭️  SKIP detected during parallel rendering: '{segment}'")
                    
                    if should_cancel:
                        # Cancel remaining tasks but continue processing already-started ones
                        for f in future_to_clip:
                            if not f.done():
                                f.cancel()
                    
                    if segment_skipped and should_cancel:
                        # Still process this task, but skip adding to results
                        try:
                            future.result()  # Wait for it to complete
                        except:
                            pass
                        continue
                    
                    task = future_to_clip[future]
                    clip_idx, item, clip_filename, output_path = task
                    
                    try:
                        success, final_path, error = future.result()
                        completed_count += 1
                        
                        # Yield progress update
                        clip_progress_msg = f"Processing segment {segment_idx}/{len(sorted_segments)}: '{segment}' ({completed_count}/{len(segment_matches)} matches)"
                        yield {
                            'progress': clip_progress_msg,
                            'segment_index': segment_idx,
                            'total_segments': len(sorted_segments),
                            'segment_phrase': segment,
                            'clip_index': completed_count,
                            'total_clips': len(segment_matches)
                        }
                        
                        if success and final_path:
                            relative_path = clip_filename
                            match = item['match']  # Get match from item
                            filename = item['file']
                            
                            # Calculate original clip boundaries (same as in render_single_clip)
                            match_start = sanitize_float(match.get('start'), 0.0)
                            match_end = sanitize_float(match.get('end'), match_start)
                            if match_start is not None and match_end is not None:
                                video_duration = get_video_duration(filename)
                                if video_duration:
                                    clip_start = max(match_start - DEFAULT_CLIP_START_PADDING, 0.0)
                                    clip_end = match_end + DEFAULT_CLIP_END_PADDING
                                    clip_start = min(clip_start, video_duration - 0.1)
                                    clip_end = min(clip_end, video_duration)
                                    if clip_end <= clip_start:
                                        clip_duration = max(match_end - match_start, 0.0)
                                        min_extension = DEFAULT_CLIP_START_PADDING + DEFAULT_CLIP_END_PADDING
                                        clip_end = min(clip_start + max(clip_duration + min_extension, 0.1), video_duration)
                                else:
                                    clip_start = match_start
                                    clip_end = match_end
                            else:
                                clip_start = match_start
                                clip_end = match_end
                            
                            segment_results.append({
                                'file': os.path.join('temp', clip_output_dir_name, relative_path),
                                'start': match.get('start'),
                                'end': match.get('end'),
                                'segment': match.get('segment', segment),
                                'source_video': filename,  # Use full path like sequential path
                                'original_start': clip_start,  # Original segment start in seconds
                                'original_end': clip_end  # Original segment end in seconds
                            })
                        else:
                            print(f"Failed to render clip {clip_idx+1}/{len(segment_matches)}: {error}")
                    except Exception as exc:
                        completed_count += 1
                        print(f"Exception rendering clip {clip_idx+1}/{len(segment_matches)}: {exc}")
                
                print(f"[Parallel] Completed {completed_count}/{len(segment_matches)} clips for segment '{segment}'")
            
            # Update global counter
            global_clip_counter += len(segment_matches)
        
        # Legacy sequential rendering for non-export or skipped export
        if segment_skipped or not export_clips or not clip_output_dir:
            for clip_idx, item in enumerate(segment_matches):
                # Check if entire search was cancelled
                if search_id:
                    with CANCEL_LOCK:
                        if search_id in CANCELLED_SEARCHES:
                            print(f"[Generator] ❌ CANCELLED during clip processing - exiting immediately")
                            raise GeneratorExit("Search cancelled by user")
                
                # Check for skip before processing each clip
                if search_id:
                    with SKIP_LOCK:
                        if segment in SKIP_SEGMENTS.get(search_id, set()):
                            print(f"[Generator] ⏭️  SKIP detected during clip {clip_idx+1}/{len(segment_matches)} of segment: '{segment}'")
                            segment_skipped = True
                            break  # Exit clip loop immediately
                
                filename = item['file']
                match = item['match']
                
                clip_filename = f"{clip_group_name}_{global_clip_counter:05d}.mp4"
                relative_path = clip_filename
                final_path = None
                
                if export_clips and clip_output_dir:
                    if skip_export:
                        if cached_clip_index < len(cached_clip_files):
                            clip_filename = cached_clip_files[cached_clip_index]
                            cached_clip_index += 1
                            final_path = os.path.join(clip_output_dir, clip_filename)
                            if not os.path.exists(final_path):
                                skip_export = False
                        else:
                            skip_export = False

                if not skip_export:
                    clip_filename = f"{clip_group_name}_{global_clip_counter:05d}.mp4"
                    output_path = os.path.join(clip_output_dir, clip_filename)
                    try:
                        # Validate file exists
                        if not os.path.exists(filename):
                            raise FileNotFoundError(f"Video file not found: {filename}")
                        
                        match_start = sanitize_float(match.get('start'), 0.0)
                        match_end = sanitize_float(match.get('end'), match_start)
                        
                        # Validate that we have valid start and end times
                        if match_start is None or match_end is None:
                            raise ValueError(f"Invalid time values: start={match.get('start')}, end={match.get('end')}")
                        
                        # Get video duration to clamp clip times
                        video_duration = get_video_duration(filename)
                        if video_duration is None:
                            raise ValueError(f"Could not determine video duration for: {filename}")
                        
                        clip_start = max(match_start - DEFAULT_CLIP_START_PADDING, 0.0)
                        clip_end = match_end + DEFAULT_CLIP_END_PADDING
                        
                        # Clamp clip_start and clip_end to video duration
                        clip_start = min(clip_start, video_duration - 0.1)  # Leave at least 0.1s
                        clip_end = min(clip_end, video_duration)
                        
                        if clip_end <= clip_start:
                            clip_duration = max(match_end - match_start, 0.0)
                            min_extension = DEFAULT_CLIP_START_PADDING + DEFAULT_CLIP_END_PADDING
                            clip_end = min(clip_start + max(clip_duration + min_extension, 0.1), video_duration)
                        
                        # Final validation
                        if clip_start >= clip_end:
                            raise ValueError(f"Invalid clip range: start={clip_start}, end={clip_end}, video_duration={video_duration}")
                        
                        # Validate clip_start and clip_end are valid numbers before export
                        if clip_start is None or clip_end is None or not isinstance(clip_start, (int, float)) or not isinstance(clip_end, (int, float)):
                            raise ValueError(f"Invalid clip times: clip_start={clip_start}, clip_end={clip_end}")
                        
                        # Ensure output directory exists
                        os.makedirs(clip_output_dir, exist_ok=True)
                        
                        # Get video fps for proper export
                        video_fps = get_video_fps(filename)
                        if video_fps is None:
                            print(f"Warning: Could not detect FPS for {filename}, using default 25")
                            video_fps = 25
                        
                        # Use ffmpeg with Popen so we can check for skip and terminate if needed
                        ffmpeg_cmd = [
                            'ffmpeg', '-y',
                            '-ss', str(clip_start),
                            '-i', filename,
                            '-t', str(clip_end - clip_start),
                            '-c:v', 'libx264',
                            '-c:a', 'aac',
                            '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                            '-r', str(video_fps),
                            output_path
                        ]
                        
                        # Start ffmpeg process
                        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        
                        # Poll process while checking for skip/cancel requests
                        import time
                        while process.poll() is None:  # While process is running
                            # Check if entire search was cancelled
                            if search_id:
                                with CANCEL_LOCK:
                                    if search_id in CANCELLED_SEARCHES:
                                        print(f"[Generator] ❌ CANCELLED - Terminating ffmpeg for search: {search_id}")
                                        process.terminate()
                                        try:
                                            process.wait(timeout=2)
                                        except subprocess.TimeoutExpired:
                                            process.kill()
                                            process.wait()
                                        # Clean up partial file
                                        if os.path.exists(output_path):
                                            try:
                                                os.remove(output_path)
                                            except OSError:
                                                pass
                                        # Set a flag to exit completely (not just this segment)
                                        raise GeneratorExit("Search cancelled by user")
                            
                            # Check if segment was skipped
                            if search_id:
                                with SKIP_LOCK:
                                    if segment in SKIP_SEGMENTS.get(search_id, set()):
                                        print(f"[Generator] ⏭️  Terminating ffmpeg for skipped segment: '{segment}'")
                                        process.terminate()
                                        try:
                                            process.wait(timeout=2)
                                        except subprocess.TimeoutExpired:
                                            process.kill()
                                            process.wait()
                                        # Clean up partial file
                                        if os.path.exists(output_path):
                                            try:
                                                os.remove(output_path)
                                            except OSError:
                                                pass
                                        segment_skipped = True
                                        break
                            time.sleep(0.1)  # Check every 100ms
                        
                        # If we skipped, break out of try block
                        if segment_skipped:
                            break
                        
                        # Check return code if process finished normally
                        if process.returncode != 0:
                            stderr = process.stderr.read().decode() if process.stderr else ''
                            print(f"FFmpeg stderr: {stderr}")
                            raise RuntimeError(f"FFmpeg failed with return code {process.returncode}")
                        
                        # Clean up any temp files created in the root directory
                        root_dir = os.path.dirname(__file__)
                        for temp_file in glob.glob(os.path.join(root_dir, "*TEMP_MPY*.mp3")):
                            try:
                                os.remove(temp_file)
                            except OSError:
                                pass
                        
                        # Also clean up temp files in the output directory
                        for temp_file in glob.glob(os.path.join(clip_output_dir, "*TEMP_MPY*.mp3")):
                            try:
                                os.remove(temp_file)
                            except OSError:
                                pass
                        
                        final_path = output_path
                        if not os.path.exists(final_path):
                            raise FileNotFoundError(f"Exported clip not found: {output_path}")
                    except Exception as exc:
                        # If segment was skipped, break out
                        if segment_skipped:
                            break
                        error_traceback = traceback.format_exc()
                        print(f"Failed to export clip for segment '{match['segment']}' in {filename}: {exc}")
                        print(f"Error details: {error_traceback}")
                        # Try to get debug info safely
                        try:
                            debug_info = f"clip_start={clip_start}, clip_end={clip_end}, video_duration={video_duration}, match_start={match_start}, match_end={match_end}"
                        except NameError:
                            debug_info = "Some variables not available"
                        print(f"Debug info: {debug_info}")
                        # Don't skip the match - still add it to results even if export failed
                        final_path = None

                # Break out if segment was skipped
                if segment_skipped:
                    break
                    
                if final_path:
                    relative_path = posixpath.join(
                        'temp',
                        clip_output_dir_name,
                        os.path.basename(final_path)
                    )
                else:
                    # If export failed, still use the intended filename so the match is shown
                    relative_path = posixpath.join(
                        'temp',
                        clip_output_dir_name,
                        clip_filename
                    )
            
            # Get the actual duration of the exported clip
            clip_duration_ms = None
            if final_path and os.path.exists(final_path):
                clip_duration_ms = get_video_duration_ms(final_path)
            elif relative_path:
                # Try to read from relative path (for cached clips)
                full_path = os.path.join(os.getcwd(), relative_path.replace('/', os.sep))
                if os.path.exists(full_path):
                    clip_duration_ms = get_video_duration_ms(full_path)
            
            segment_result = {
                'file': relative_path,
                'source_video': filename,
                'original_start': clip_start,  # Original segment start in seconds
                'original_end': clip_end  # Original segment end in seconds
            }
            if clip_duration_ms is not None:
                segment_result['duration_ms'] = clip_duration_ms
            
            segment_results.append(segment_result)
            global_clip_counter += 1
        
        # If segment was skipped, yield skip notification instead of results
        if segment_skipped:
            print(f"[Generator] ⏭️  Segment {segment_idx}/{len(sorted_segments)}: '{segment}' was skipped")
            yield {
                'phrase': segment,
                'files': [],
                'word_count': len(segment.split()),
                'skipped': True
            }
        else:
            # Yield this segment's result
            segment_word_count = len(segment.split())
            print(f"[Generator] ✓ Yielding segment {segment_idx}/{len(sorted_segments)}: '{segment}' with {len(segment_results)} clips")
            yield {
                'phrase': segment,
                'files': segment_results,
                'word_count': segment_word_count
            }
    
    # Write metadata after all segments have been processed
    if export_clips and clip_output_dir:
        if global_clip_counter == 0 and not skip_export:
            shutil.rmtree(clip_output_dir, ignore_errors=True)
        elif not skip_export and clip_metadata_path:
            try:
                with open(clip_metadata_path, 'w', encoding='utf-8') as metadata_file:
                    json.dump(current_metadata, metadata_file, indent=2)
            except OSError as exc:
                print(f"Failed to write clip metadata for '{clip_group_name}': {exc}")


def find_and_export_longest_matches(
    input_sentence,
    filenames,
    counter,
    export_clips=False,
    clip_group=None,
    min_silence=0.0,
    max_silence=10.0,
    silence_word_threshold=2
):
    all_matches = []
    global_clip_counter = 0  # Counter for all clips across files
    clip_output_dir = None
    clip_output_dir_name = None
    clip_group_name = clip_group or generate_clip_group_name('segment', input_sentence, filenames)
    silence_word_threshold = max(silence_word_threshold, 1)
    skip_export = False
    clip_metadata_path = None
    cached_clip_files = []
    cached_clip_index = 0

    if export_clips:
        clip_output_dir_name = f'{clip_group_name}_clips'
        clip_output_dir = os.path.join(TEMP_DIR, clip_output_dir_name)
        clip_metadata_path = os.path.join(clip_output_dir, CLIP_METADATA_FILENAME)
        current_metadata = {
            'version': 1,
            'source_files': sorted(map(str, filenames)),
            'input_sentence': input_sentence,
            'max_results': MAX_CLIPS_PER_SEGMENT
        }
        stored_metadata = None
        if os.path.exists(clip_metadata_path):
            try:
                with open(clip_metadata_path, 'r', encoding='utf-8') as metadata_file:
                    stored_metadata = json.load(metadata_file)
            except (OSError, json.JSONDecodeError):
                stored_metadata = None

        if stored_metadata and stored_metadata.get('source_files') == current_metadata['source_files']:
            skip_export = True
            if os.path.isdir(clip_output_dir):
                try:
                    cached_clip_files = sorted(
                        [
                            name for name in os.listdir(clip_output_dir)
                            if name.lower().endswith('.mp4')
                        ]
                    )
                except OSError:
                    cached_clip_files = []
                if cached_clip_files:
                    verified_clip_files = []
                    for cached_name in cached_clip_files:
                        cached_path = os.path.join(clip_output_dir, cached_name)
                        if os.path.isfile(cached_path):
                            verified_clip_files.append(cached_name)
                    if len(verified_clip_files) != len(cached_clip_files):
                        cached_clip_files = verified_clip_files
                        if not cached_clip_files:
                            skip_export = False
                            shutil.rmtree(clip_output_dir, ignore_errors=True)
            else:
                skip_export = False
                cached_clip_files = []
        else:
            if os.path.isdir(clip_output_dir):
                shutil.rmtree(clip_output_dir)
            os.makedirs(clip_output_dir, exist_ok=True)

        if skip_export:
            required_count = min(len(cached_clip_files), MAX_CLIPS_PER_SEGMENT)
            if required_count == 0:
                skip_export = False
                shutil.rmtree(clip_output_dir, ignore_errors=True)
                os.makedirs(clip_output_dir, exist_ok=True)
                cached_clip_files = []
            else:
                cached_clip_files = cached_clip_files[:MAX_CLIPS_PER_SEGMENT]

    collected_matches = []

    # First, search ALL files and collect ALL matches
    all_file_matches = []
    for filename in filenames:
        sentences = load_transcript_sentences(filename)
        # Skip files without valid transcript data
        if not sentences:
            continue
        # Validate that we have actual transcript segments with content
        if not isinstance(sentences, list) or len(sentences) == 0:
            continue
        # Check that at least one segment has actual content
        has_valid_content = False
        for segment in sentences:
            if isinstance(segment, dict):
                content = segment.get('content', '')
                words = segment.get('words', [])
                if (isinstance(content, str) and content.strip()) or (isinstance(words, list) and len(words) > 0):
                    has_valid_content = True
                    break
        if not has_valid_content:
            continue

        matches = find_longest_matches(
            input_sentence,
            sentences,
            min_silence=min_silence,
            max_silence=max_silence,
            silence_word_threshold=silence_word_threshold
        )

        if not matches:
            print("No matches found for the input sentence in:", filename)
            continue

        if matches:
            all_file_matches.append({'file': filename, 'matches': matches})
            video_name = os.path.basename(filename)
            print(f"Found {len(matches)} matches for '{input_sentence}' in {video_name}")
    
    # Collect ALL matches from all files to identify all segments
    # But limit exports to MAX_CLIPS_PER_SEGMENT per unique segment
    if all_file_matches:
        # First, group all matches by segment across all files
        segment_groups = {}
        for entry in all_file_matches:
            for match in entry['matches']:
                segment = match.get('segment', '')
                if segment not in segment_groups:
                    segment_groups[segment] = []
                segment_groups[segment].append({'file': entry['file'], 'match': match})
        
        # Now limit each segment to MAX_CLIPS_PER_SEGMENT for export
        # Sort segments by length (longest first) for better ordering
        sorted_segments = sorted(segment_groups.keys(), key=lambda s: len(s.split()), reverse=True)
        
        for segment in sorted_segments:
            segment_matches = segment_groups[segment][:MAX_CLIPS_PER_SEGMENT]  # Limit per segment
            
            # Group back by file
            file_match_dict = {}
            for item in segment_matches:
                filename = item['file']
                if filename not in file_match_dict:
                    file_match_dict[filename] = []
                file_match_dict[filename].append(item['match'])
            
            # Add to collected_matches in the expected format
            for filename, matches in file_match_dict.items():
                collected_matches.append({'file': filename, 'matches': matches})
        
        # Calculate statistics
        total_found = sum(len(segment_groups[seg]) for seg in segment_groups)
        total_exported = sum(len(entry['matches']) for entry in collected_matches)
        print(f"Total matches found: {total_found} across {len(all_file_matches)} video(s)")
        print(f"Exporting {total_exported} clips ({len(segment_groups)} unique segments, max {MAX_CLIPS_PER_SEGMENT} per segment)")
    
    collected_total = sum(len(entry['matches']) for entry in collected_matches)

    if export_clips and clip_output_dir:
        if skip_export and len(cached_clip_files) < collected_total:
            skip_export = False
            shutil.rmtree(clip_output_dir, ignore_errors=True)
            os.makedirs(clip_output_dir, exist_ok=True)
            cached_clip_files = []
        elif not skip_export and not os.path.isdir(clip_output_dir):
            os.makedirs(clip_output_dir, exist_ok=True)
        elif skip_export:
            cached_clip_files = cached_clip_files[:collected_total]

    # Prepare all clip tasks for parallel rendering
    clip_tasks = []
    clip_metadata = []  # Store metadata for each clip (filename, match, etc.)
    
    for entry in collected_matches:
        filename = entry['file']
        for match in entry['matches']:
            clip_filename = f"{clip_group_name}_{global_clip_counter:05d}.mp4"
            relative_path = clip_filename
            final_path = None

            if export_clips and clip_output_dir:
                if skip_export:
                    if cached_clip_index < len(cached_clip_files):
                        clip_filename = cached_clip_files[cached_clip_index]
                        cached_clip_index += 1
                        final_path = os.path.join(clip_output_dir, clip_filename)
                        if not os.path.exists(final_path):
                            skip_export = False
                    else:
                        skip_export = False

                if not skip_export:
                    output_path = os.path.join(clip_output_dir, clip_filename)
                    # Prepare task for parallel rendering
                    clip_tasks.append((global_clip_counter, {
                        'file': filename,
                        'match': match
                    }, clip_filename, output_path))
                    clip_metadata.append({
                        'clip_counter': global_clip_counter,
                        'filename': filename,
                        'match': match,
                        'clip_filename': clip_filename,
                        'relative_path': posixpath.join('temp', clip_output_dir_name, clip_filename)
                    })
                else:
                    # Using cached clip
                    clip_metadata.append({
                        'clip_counter': global_clip_counter,
                        'filename': filename,
                        'match': match,
                        'clip_filename': clip_filename,
                        'relative_path': posixpath.join('temp', clip_output_dir_name, clip_filename),
                        'cached': True,
                        'final_path': final_path
                    })
            else:
                # Not exporting clips, just metadata
                clip_metadata.append({
                    'clip_counter': global_clip_counter,
                    'filename': filename,
                    'match': match,
                    'clip_filename': clip_filename,
                    'relative_path': clip_filename
                })
            
            global_clip_counter += 1
    
    # Render clips in parallel if we have tasks
    if export_clips and clip_output_dir and clip_tasks:
        # Ensure output directory exists
        os.makedirs(clip_output_dir, exist_ok=True)
        
        # Use cached GPU encoder (detected at startup)
        gpu_encoder, gpu_args = get_gpu_encoder()
        
        def render_single_clip_phrase(clip_data):
            """Render a single clip for phrase search. Returns (success, output_path, error) tuple."""
            clip_idx, item, clip_filename, output_path = clip_data
            filename = item['file']
            match = item['match']
            
            try:
                # Validate file exists
                if not os.path.exists(filename):
                    raise FileNotFoundError(f"Video file not found: {filename}")
                
                match_start = sanitize_float(match.get('start'), 0.0)
                match_end = sanitize_float(match.get('end'), match_start)
                
                # Validate that we have valid start and end times
                if match_start is None or match_end is None:
                    raise ValueError(f"Invalid time values: start={match.get('start')}, end={match.get('end')}")
                
                # Get video duration to clamp clip times
                video_duration = get_video_duration(filename)
                if video_duration is None:
                    raise ValueError(f"Could not determine video duration for: {filename}")
                
                clip_start = max(match_start - DEFAULT_CLIP_START_PADDING, 0.0)
                clip_end = match_end + DEFAULT_CLIP_END_PADDING
                
                # Clamp clip_start and clip_end to video duration
                clip_start = min(clip_start, video_duration - 0.1)  # Leave at least 0.1s
                clip_end = min(clip_end, video_duration)
                
                if clip_end <= clip_start:
                    clip_duration = max(match_end - match_start, 0.0)
                    min_extension = DEFAULT_CLIP_START_PADDING + DEFAULT_CLIP_END_PADDING
                    clip_end = min(clip_start + max(clip_duration + min_extension, 0.1), video_duration)
                
                # Final validation
                if clip_start >= clip_end:
                    raise ValueError(f"Invalid clip range: start={clip_start}, end={clip_end}, video_duration={video_duration}")
                
                # Get video fps for proper export
                video_fps = get_video_fps(filename)
                if video_fps is None:
                    video_fps = 25
                
                # Try to use GPU encoding if available, but respect the concurrent session limit
                current_gpu_encoder, current_gpu_args = get_gpu_encoder()
                use_gpu = False
                
                if current_gpu_encoder and GPU_ENCODING_SEMAPHORE:
                    # Try to acquire GPU semaphore (non-blocking check first)
                    if GPU_ENCODING_SEMAPHORE.acquire(blocking=False):
                        use_gpu = True
                
                ffmpeg_cmd = ['ffmpeg', '-y']
                ffmpeg_cmd.extend([
                    '-ss', str(clip_start),
                    '-i', filename,
                    '-t', str(clip_end - clip_start),
                    '-c:a', 'aac',
                    '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                    '-r', str(video_fps),
                ])
                
                # Add video encoder (GPU or CPU)
                try:
                    if use_gpu:
                        ffmpeg_cmd.extend(['-c:v', current_gpu_encoder])
                        ffmpeg_cmd.extend(current_gpu_args)
                    else:
                        ffmpeg_cmd.extend(['-c:v', 'libx264', '-preset', 'ultrafast'])
                finally:
                    # Always release the semaphore if we acquired it
                    if use_gpu and GPU_ENCODING_SEMAPHORE:
                        GPU_ENCODING_SEMAPHORE.release()
                
                ffmpeg_cmd.append(output_path)
                
                # Run ffmpeg
                result = subprocess.run(
                    ffmpeg_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=300  # 5 minute timeout per clip
                )
                
                if result.returncode != 0:
                    stderr = result.stderr.decode() if result.stderr else ''
                    # If GPU encoding failed, retry with CPU
                    if use_gpu and current_gpu_encoder:
                        print(f"[GPU] GPU encoding failed (code {result.returncode}), retrying with CPU: {clip_filename}")
                        ffmpeg_cmd_cpu = [
                            'ffmpeg', '-y',
                            '-ss', str(clip_start),
                            '-i', filename,
                            '-t', str(clip_end - clip_start),
                            '-c:v', 'libx264',
                            '-preset', 'ultrafast',
                            '-c:a', 'aac',
                            '-ac', '2',  # Force stereo output (fixes mono and 5.1 issues)
                            '-r', str(video_fps),
                            output_path
                        ]
                        result = subprocess.run(
                            ffmpeg_cmd_cpu,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            timeout=300
                        )
                        if result.returncode != 0:
                            stderr_cpu = result.stderr.decode() if result.stderr else ''
                            raise RuntimeError(f"FFmpeg failed with return code {result.returncode}: {stderr_cpu[:500]}")
                    else:
                        raise RuntimeError(f"FFmpeg failed with return code {result.returncode}: {stderr[:500]}")
                
                if not os.path.exists(output_path):
                    raise FileNotFoundError(f"Exported clip not found: {output_path}")
                
                return (True, output_path, None)
            except Exception as e:
                return (False, None, str(e))
        
        # Render clips in parallel
        max_workers = get_max_workers()  # Use all CPU cores
        current_gpu_encoder, _ = get_gpu_encoder()
        if current_gpu_encoder:
            gpu_limit = get_max_gpu_workers()
            print(f"[Parallel] Using hybrid encoding: {gpu_limit} GPU workers ({current_gpu_encoder}) + {max_workers - gpu_limit} CPU workers (libx264) = {max_workers} total workers")
        else:
            print(f"[Parallel] Using CPU encoding only, using {max_workers} concurrent workers")
        
        print(f"[Parallel] Rendering {len(clip_tasks)} clips in parallel...")
        
        # Create a mapping from clip_idx to metadata index
        clip_idx_to_metadata = {task[0]: idx for idx, task in enumerate(clip_tasks)}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_clip = {
                executor.submit(render_single_clip_phrase, task): task 
                for task in clip_tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_clip):
                task = future_to_clip[future]
                clip_idx = task[0]
                metadata_idx = clip_idx_to_metadata[clip_idx]
                
                try:
                    success, output_path, error = future.result()
                    if success and output_path:
                        clip_metadata[metadata_idx]['final_path'] = output_path
                        clip_metadata[metadata_idx]['relative_path'] = posixpath.join(
                            'temp',
                            clip_output_dir_name,
                            os.path.basename(output_path)
                        )
                    elif error:
                        print(f"Failed to render clip {clip_idx}: {error}")
                except Exception as e:
                    print(f"Exception rendering clip {clip_idx}: {e}")
    
    # Build final results from metadata
    for meta in clip_metadata:
        if meta.get('cached') and meta.get('final_path'):
            relative_path = posixpath.join(
                'temp',
                clip_output_dir_name,
                os.path.basename(meta['final_path'])
            )
        else:
            relative_path = meta['relative_path']
        
        # Get the actual duration of the exported clip
        clip_duration_ms = None
        final_path = meta.get('final_path')
        if final_path and os.path.exists(final_path):
            clip_duration_ms = get_video_duration_ms(final_path)
        elif relative_path:
            # Try to read from relative path (for cached clips)
            full_path = os.path.join(os.getcwd(), relative_path.replace('/', os.sep))
            if os.path.exists(full_path):
                clip_duration_ms = get_video_duration_ms(full_path)
        
        match_data = {
            'file': relative_path, 
            'segment': meta['match']['segment'],
            'source_video': meta['filename']
        }
        if clip_duration_ms is not None:
            match_data['duration_ms'] = clip_duration_ms
        
        all_matches.append(match_data)

    if export_clips and clip_output_dir:
        if global_clip_counter == 0 and not skip_export:
            shutil.rmtree(clip_output_dir, ignore_errors=True)
        elif not skip_export and clip_metadata_path:
            try:
                with open(clip_metadata_path, 'w', encoding='utf-8') as metadata_file:
                    json.dump(current_metadata, metadata_file, indent=2)
            except OSError as exc:
                print(f"Failed to write clip metadata for '{clip_group_name}': {exc}")

    print(all_matches)
    return all_matches


def find_matches_for_segment(segment, sentences, min_duration=0.0, max_duration=10.0, silence_word_threshold=2):  # @jonny Hier 0.1 einsetzen und du siehst, dass die Blöcke winzig werden. Das Skript sollte die silences ignorieren für Segmente ab 2 Wörtern.
    # Preprocess sentences into a flat list of usable word entries
    all_words = []
    for sentence in sentences:
        for word in sentence.get('words', []):
            start_time = word.get('start')
            end_time = word.get('end')
            normalized_word = normalize_token(word.get('word', ''))
            if normalized_word and start_time is not None and end_time is not None:
                all_words.append({
                    'word': word.get('word', ''),
                    'normalized_word': normalized_word,
                    'start': start_time,
                    'end': end_time
                })

    # Prepare the segment tokens for comparison
    raw_segment_words = segment.split()
    segment_words = []
    for raw_word in raw_segment_words:
        normalized = normalize_token(raw_word)
        if normalized:
            segment_words.append(normalized)
    word_count = len(segment_words)

    if not segment_words or word_count == 0 or not all_words:
        return []

    # Initialize a list to store all matches
    all_matches = []

    total_available = len(all_words)

    # Iterate through the flattened list of words
    for i in range(total_available - word_count + 1):
        window = all_words[i:i + word_count]
        window_normalized = [word['normalized_word'] for word in window]
        
        if segment_words == window_normalized:
            # Determine the start and end times of the segment
            start_time = window[0]['start']
            end_time = window[-1]['end']

            # Check for silence before and after the segment
            if i > 0:
                first_silence = start_time - all_words[i - 1]['end']
            else:
                first_silence = start_time

            if i + word_count < total_available:
                second_silence = all_words[i + word_count]['start'] - end_time
            else:
                second_silence = max_duration

            first_silence = max(first_silence, 0.0)
            second_silence = max(second_silence, 0.0)

            if word_count < silence_word_threshold:
                if min_duration <= first_silence <= max_duration and min_duration <= second_silence <= max_duration:
                    print(f'The segment "{segment}" is surrounded by silences of {first_silence} and {second_silence} seconds.')
                    all_matches.append({
                        'segment': segment,
                        'start': start_time,
                        'end': end_time,
                        'first_silence': first_silence,
                        'second_silence': second_silence
                    })
                else:
                    print(f'No match for "{segment}" as its is surrounded by silences of {first_silence} and {second_silence} seconds.')
            else:
                all_matches.append({
                    'segment': segment,
                    'start': start_time,
                    'end': end_time,
                    'first_silence': first_silence,
                    'second_silence': second_silence
                })

    # Return all matches found
    return all_matches


def find_longest_matches_filtered(
    input_sentence,
    sentences,
    min_silence=0.0,
    max_silence=10.0,
    silence_word_threshold=2,
    min_words=3
):
    """
    Like find_longest_matches, but filters out segments shorter than min_words
    (except for the full input sentence).
    This avoids searching for very short partials like "das", "aber", etc.
    """
    # Split the input sentence into words
    input_words = input_sentence.split()
    # Initialize a list to store the matches
    matches = []

    total_words = len(input_words)
    if total_words == 0:
        return matches

    seen_ranges = set()
    silence_word_threshold = max(silence_word_threshold, 1)
    
    # Check substrings, but skip very short ones (unless it's the full sentence)
    for i in range(total_words):
        for j in range(i + 1, total_words + 1):
            segment_words = input_words[i:j]
            segment = " ".join(segment_words)
            word_count = len(segment_words)

            # Skip very short segments (unless it's the full input sentence)
            if word_count < min_words and segment != input_sentence:
                continue

            segment_min_silence = min_silence if word_count < silence_word_threshold else 0.0

            found_matches = find_matches_for_segment(
                segment,
                sentences,
                min_duration=segment_min_silence,
                max_duration=max_silence,
                silence_word_threshold=silence_word_threshold
            )

            new_matches = []
            for match in found_matches:
                key = (match['start'], match['end'], segment)
                if key in seen_ranges:
                    continue
                seen_ranges.add(key)
                new_matches.append(match)

            if new_matches:
                matches.extend(new_matches)

    return matches


# @jonny bugged - forciert aktuell auch Pausen bei segments mit 2 Wörtern+
def find_longest_matches(
    input_sentence,
    sentences,
    min_silence=0.0,
    max_silence=10.0,
    silence_word_threshold=2
):
    # Split the input sentence into words
    input_words = input_sentence.split()
    # Initialize a list to store the matches
    matches = []

    total_words = len(input_words)
    if total_words == 0:
        return matches

    seen_ranges = set()
    silence_word_threshold = max(silence_word_threshold, 1)
    
    # Check ALL possible substrings, not just longest non-overlapping ones
    # This ensures we find "finanziellen belastungen" and "belastungen" 
    # even if "an den finanziellen belastungen durch" was found
    for i in range(total_words):
        for j in range(i + 1, total_words + 1):
            segment_words = input_words[i:j]
            segment = " ".join(segment_words)
            word_count = len(segment_words)

            segment_min_silence = min_silence if word_count < silence_word_threshold else 0.0

            found_matches = find_matches_for_segment(
                segment,
                sentences,
                min_duration=segment_min_silence,
                max_duration=max_silence,
                silence_word_threshold=silence_word_threshold
            )

            new_matches = []
            for match in found_matches:
                key = (match['start'], match['end'], segment)
                if key in seen_ranges:
                    continue
                seen_ranges.add(key)
                new_matches.append(match)

            if new_matches:
                matches.extend(new_matches)

    return matches


@app.route('/get_waveform', methods=['POST'])
def get_waveform():
    """Extract audio waveform data from a video file."""
    try:
        data = request.json
        file_path = data.get('file_path', '')
        width = data.get('width', 800)  # Target width in pixels
        
        if not file_path:
            return jsonify({'error': 'No file path provided'}), 400
        
        # Construct absolute path - check temp directory first, then static
        if file_path.startswith('temp/'):
            full_path = os.path.join(app.root_path, file_path)
        else:
            full_path = os.path.join(app.root_path, 'static', file_path)
        
        if not os.path.exists(full_path):
            return jsonify({'error': f'File not found: {file_path}'}), 404
        
        # Extract waveform data
        waveform_data = extract_audio_waveform(full_path, width)
        
        if waveform_data is None:
            return jsonify({'error': 'Failed to extract waveform'}), 500
        
        return jsonify({
            'waveform': waveform_data,
            'width': len(waveform_data)
        })
    
    except Exception as e:
        print(f"Error in get_waveform: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/rerender_clip', methods=['POST'])
def rerender_clip():
    """Re-render a clip with new trim values for precise playback."""
    try:
        data = request.json
        clip_path = data.get('clip_path', '')
        source_video = data.get('source_video', '')
        original_start = data.get('original_start', 0)  # Original segment start in seconds
        original_end = data.get('original_end', 0)  # Original segment end in seconds
        new_start_trim_ms = data.get('start_trim_ms', 0)  # New start trim in milliseconds
        new_end_trim_ms = data.get('end_trim_ms', 0)  # New end trim in milliseconds
        
        if not clip_path or not source_video:
            return jsonify({'error': 'Missing clip_path or source_video'}), 400
        
        # Find the source video file
        source_path = None
        for lib_root in LIBRARY_ROOTS:
            potential_path = os.path.join(lib_root, source_video)
            if os.path.exists(potential_path):
                source_path = potential_path
                break
        
        if not source_path:
            return jsonify({'error': f'Source video not found: {source_video}'}), 404
        
        # Calculate new clip boundaries
        # original_start/end are in seconds, new trims are in milliseconds
        new_clip_start = original_start + (new_start_trim_ms / 1000)
        new_clip_end = original_end - (new_end_trim_ms / 1000)
        clip_duration = new_clip_end - new_clip_start
        
        if clip_duration <= 0:
            return jsonify({'error': 'Invalid trim values result in zero or negative duration'}), 400
        
        # Get video duration to validate
        video_duration = get_video_duration(source_path)
        if video_duration is None:
            return jsonify({'error': 'Could not determine video duration'}), 500
        
        # Validate clip boundaries
        new_clip_start = max(0.0, min(new_clip_start, video_duration))
        new_clip_end = max(new_clip_start + 0.1, min(new_clip_end, video_duration))
        clip_duration = new_clip_end - new_clip_start
        
        # Generate unique output filename
        # clip_path is relative (e.g., "temp/clip_group_00001.mp4")
        # Construct full path to original clip
        if clip_path.startswith('temp/'):
            clip_full_path = os.path.join(app.root_path, clip_path)
        else:
            clip_full_path = os.path.join(app.root_path, 'temp', clip_path)
        
        # Ensure the directory exists
        clip_dir = os.path.dirname(clip_full_path)
        os.makedirs(clip_dir, exist_ok=True)
        
        clip_basename = os.path.basename(clip_path)
        name_without_ext = os.path.splitext(clip_basename)[0]
        
        # Strip any existing _trimmed_* suffixes to prevent accumulation
        # Match pattern: _trimmed_<number>_<number> (can repeat)
        import re
        name_without_ext = re.sub(r'_trimmed_\d+_\d+(?:_trimmed_\d+_\d+)*$', '', name_without_ext)
        
        output_filename = f"{name_without_ext}_trimmed_{int(new_start_trim_ms)}_{int(new_end_trim_ms)}.mp4"
        output_path = os.path.join(clip_dir, output_filename)
        
        # Get video fps
        video_fps = get_video_fps(source_path)
        if video_fps is None:
            video_fps = 25
        
        # Use ffmpeg to re-render with new trim values
        ffmpeg_cmd = [
            'ffmpeg', '-y',
            '-ss', str(new_clip_start),
            '-i', source_path,
            '-t', str(clip_duration),
            '-c:v', 'libx264',
            '-preset', 'ultrafast',  # Fast rendering for responsiveness
            '-c:a', 'aac',
            '-ac', '2',
            '-r', str(video_fps),
            output_path
        ]
        
        process = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        if process.returncode != 0:
            stderr = process.stderr.decode(errors='ignore') if process.stderr else ''
            return jsonify({'error': f'FFmpeg failed: {stderr}'}), 500
        
        if not os.path.exists(output_path):
            return jsonify({'error': 'Rendered clip file was not created'}), 500
        
        # Return relative path (clips are always in temp/)
        # Get the relative path from app.root_path
        try:
            relative_path = os.path.relpath(output_path, app.root_path).replace(os.sep, '/')
            # Ensure it uses forward slashes and starts correctly
            if not relative_path.startswith('temp/'):
                # Extract temp/ part if it exists in the path
                parts = relative_path.split('/')
                if 'temp' in parts:
                    temp_idx = parts.index('temp')
                    relative_path = '/'.join(parts[temp_idx:])
                else:
                    # Fallback: construct manually
                    relative_path = 'temp/' + os.path.basename(output_path)
        except (ValueError, AttributeError):
            # Fallback: construct path manually based on clip_path structure
            if clip_path.startswith('temp/'):
                # Extract the directory structure from original clip_path
                clip_dir_part = os.path.dirname(clip_path).replace('\\', '/')
                relative_path = f"{clip_dir_part}/{output_filename}".replace('\\', '/')
            else:
                relative_path = f"temp/{output_filename}"
        
        clip_duration_ms = get_video_duration_ms(output_path)
        
        return jsonify({
            'clip_path': relative_path,
            'duration_ms': clip_duration_ms
        })
    except Exception as e:
        print(f"Error re-rendering clip: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def extract_audio_waveform(video_path, target_width=800):
    """
    Extract audio waveform data from a video file using ffmpeg.
    Returns a list of normalized amplitude values (0.0 to 1.0).
    Uses RMS (Root Mean Square) for more accurate representation.
    """
    try:
        # Get video duration first
        duration_cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        duration_result = subprocess.run(duration_cmd, capture_output=True, text=True, timeout=10)
        
        if duration_result.returncode != 0:
            print(f"Failed to get duration: {duration_result.stderr}")
            return None
        
        duration = float(duration_result.stdout.strip())
        
        # Use higher sample rate for better accuracy
        # At least 100 samples per pixel for accurate peak detection
        sample_rate = max(8000, int(target_width / duration * 100))
        
        # Extract audio as 16-bit PCM mono
        ffmpeg_cmd = [
            'ffmpeg', '-i', video_path,
            '-ac', '1',  # Mono
            '-ar', str(sample_rate),  # Sample rate
            '-f', 's16le',  # 16-bit PCM
            '-'
        ]
        
        result = subprocess.run(ffmpeg_cmd, capture_output=True, timeout=30)
        
        if result.returncode != 0:
            print(f"FFmpeg error: {result.stderr.decode()}")
            return None
        
        # Parse PCM data
        import struct
        audio_data = result.stdout
        
        # Convert bytes to 16-bit signed integers
        num_samples = len(audio_data) // 2
        samples = struct.unpack(f'{num_samples}h', audio_data)
        
        # Calculate RMS values for each pixel first (two-pass for auto-normalization)
        samples_per_pixel = max(1, num_samples // target_width)
        raw_rms_values = []
        
        for i in range(target_width):
            start_idx = i * samples_per_pixel
            end_idx = min(start_idx + samples_per_pixel, num_samples)
            
            if start_idx < num_samples:
                chunk = samples[start_idx:end_idx]
                if chunk:
                    # Use RMS (Root Mean Square) for better audio representation
                    rms = (sum(s * s for s in chunk) / len(chunk)) ** 0.5
                    raw_rms_values.append(rms)
                else:
                    raw_rms_values.append(0.0)
            else:
                raw_rms_values.append(0.0)
        
        # Find peak RMS value for auto-normalization (fills waveform to full height)
        peak_rms = max(raw_rms_values) if raw_rms_values else 1.0
        
        # Avoid division by zero and prevent over-amplification of near-silence
        # Use at least 5% of theoretical max as floor
        normalization_factor = max(peak_rms, 32768.0 * 0.05)
        
        # Normalize waveform to fill height based on actual peak amplitude
        waveform = []
        for rms in raw_rms_values:
            # Normalize to 0.0-1.0 based on the clip's actual peak
            normalized = rms / normalization_factor
            # Clamp to 1.0 max
            normalized = min(1.0, normalized)
            waveform.append(normalized)
        
        return waveform
    
    except Exception as e:
        print(f"Error extracting waveform: {e}")
        traceback.print_exc()
        return None


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
    args = parser.parse_args()
    
    # Set global GPU streams limit
    MAX_GPU_STREAMS = max(1, args.gpustreams)  # Ensure at least 1
    print(f"[Startup] GPU streams limit set to: {MAX_GPU_STREAMS}")
    
    # Add additional library paths from command line
    if args.library_path:
        for lib_path in args.library_path:
            # Convert to absolute path and validate
            abs_path = os.path.abspath(lib_path)
            if os.path.exists(abs_path):
                if abs_path not in LIBRARY_ROOTS:
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
    
    # Parse listener address
    try:
        if ':' in args.listen:
            host, port = args.listen.rsplit(':', 1)
            port = int(port)
        else:
            # If no port specified, assume default port 5000
            host = args.listen
            port = 5000
    except ValueError:
        print(f"[Startup] Error: Invalid listener format '{args.listen}'. Expected HOST:PORT (e.g., 0.0.0.0:8080)")
        sys.exit(1)
    
    print(f"[Startup] Starting server on {host}:{port}")
    app.run(debug=True, host=host, port=port)

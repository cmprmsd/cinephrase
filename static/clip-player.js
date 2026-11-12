/**
 * ClipPlayer - sample-accurate clip preview.
 *
 * The audio clock is the master. Every clip's audio is fetched as PCM (the very samples the merge will use),
 * decoded into an AudioBuffer and started with AudioBufferSourceNode.start(when, offset, duration): the Web Audio
 * engine begins and ends playback on exact sample frames, and consecutive clips are scheduled back to back on the
 * same clock, so a timeline preview is gapless. The pictures are muted <video> elements that follow that clock
 * (they are frame-quantised anyway); they never decide when sound starts or stops.
 *
 * Nothing in here knows about the rest of the app: it gets "items" and a stage element to draw into.
 *
 * item = {
 *   clip: 'temp/....mp4',                  // picture (and audio fallback)
 *   source, originalStart, originalEnd,    // canonical audio reference in the library (optional)
 *   startTrim, endTrim,                    // ms cut from the start / end of the clip
 *   speed,                                 // 0.5 .. 2 (pitch preserved, stretched server-side like the merge)
 *   title
 * }
 */
(function (root) {
    'use strict';

    const SAMPLE_RATE = 48000;
    const START_LEAD = 0.06;        // s between "go" and the first audible sample
    const UNDERRUN_LEAD = 0.05;     // s of headroom when a buffer arrives after its slot
    const SCHEDULE_AHEAD = 1.5;     // s of audio kept scheduled ahead of the play head
    const PRELOAD_ITEMS = 3;        // audio buffers requested ahead of the scheduler
    const SWAP_LEAD = 0.03;         // s: put the next picture (parked on its first frame) on screen this early
    const PLAY_LEAD_MAX = 0.08;     // s: upper bound of the learned <video>.play() start-up latency
    const PICTURE_SLOTS = 3;        // current picture + two prepared ones
    const PICTURE_TIMEOUT = 1500;   // ms to wait for a picture before starting sound without it
    const DRIFT_DEADBAND = 0.012;   // s of picture drift that is left alone
    const DRIFT_HARD = 0.2;         // s of picture drift that is fixed by seeking instead of nudging the rate
    const CACHE_BYTES = 160 * 1024 * 1024;
    const FADE_OUT = 0.008;         // s, only when playback is interrupted (never at a planned cut)

    function mediaUrl(path) {
        if (!path) return '';
        const encoded = String(path).split('/').map(encodeURIComponent).join('/');
        return path.startsWith('temp/') ? '/' + encoded : '/static/' + encoded;
    }

    function clamp(value, low, high) {
        return Math.min(high, Math.max(low, value));
    }

    function finiteOr(value, fallback) {
        const number = typeof value === 'number' ? value : parseFloat(value);
        return Number.isFinite(number) ? number : fallback;
    }

    function normaliseItem(item) {
        return {
            clip: item.clip || '',
            source: item.source || '',
            originalStart: finiteOr(item.originalStart, null),
            originalEnd: finiteOr(item.originalEnd, null),
            startTrim: Math.max(0, finiteOr(item.startTrim, 0)),
            endTrim: Math.max(0, finiteOr(item.endTrim, 0)),
            speed: clamp(finiteOr(item.speed, 1), 0.5, 2),
            title: item.title || ''
        };
    }

    /** Query string identifying the clip's canonical media (shared by /clip_audio and the cache key). */
    function referenceParams(item) {
        const params = new URLSearchParams();
        if (item.clip) params.set('clip', item.clip);
        if (item.source && item.originalStart !== null && item.originalEnd !== null) {
            params.set('source', item.source);
            params.set('original_start', String(item.originalStart));
            params.set('original_end', String(item.originalEnd));
        }
        return params;
    }

    /**
     * What to fetch and which part of it to play.
     * At normal speed the whole clip is cached once and cut on the audio clock, so trimming never refetches.
     * At other speeds the server returns the trimmed, time-stretched range (trim -> atempo, exactly like the merge).
     */
    function audioSpec(item) {
        const params = referenceParams(item);
        if (Math.abs(item.speed - 1) < 1e-6) {
            return {
                key: params.toString(),
                url: '/clip_audio?' + params.toString(),
                range(buffer) {
                    const offset = item.startTrim / 1000;
                    return { offset, duration: buffer.duration - offset - item.endTrim / 1000 };
                }
            };
        }
        params.set('start_ms', String(item.startTrim));
        params.set('end_trim_ms', String(item.endTrim));
        params.set('speed', item.speed.toFixed(4));
        return {
            key: params.toString(),
            url: '/clip_audio?' + params.toString(),
            range(buffer) {
                return { offset: 0, duration: buffer.duration };
            }
        };
    }

    // ----------------------------------------------------------------------------------------------------
    // Stage: the element pictures are drawn into
    // ----------------------------------------------------------------------------------------------------

    function createStage(host, options) {
        const settings = options || {};
        const element = document.createElement('div');
        element.className = 'clip-stage';
        element.hidden = true;
        element.tabIndex = 0;
        element.setAttribute('role', 'button');
        element.setAttribute('aria-label', 'Clip preview. Click to replay or stop.');

        const slots = [];
        for (let index = 0; index < PICTURE_SLOTS; index++) {
            const video = document.createElement('video');
            video.className = 'clip-stage-video';
            video.muted = true;
            video.playsInline = true;
            video.setAttribute('playsinline', '');
            video.preload = 'auto';
            video.disablePictureInPicture = true;
            element.appendChild(video);
            slots.push({ video, index: -1, url: '', ready: null, usable: false, token: 0 });
        }

        const hud = document.createElement('div');
        hud.className = 'clip-stage-hud';
        const title = document.createElement('span');
        title.className = 'clip-stage-title';
        const status = document.createElement('span');
        status.className = 'clip-stage-status';
        hud.appendChild(title);
        hud.appendChild(status);
        element.appendChild(hud);

        const progress = document.createElement('div');
        progress.className = 'clip-stage-progress';
        const progressFill = document.createElement('div');
        progress.appendChild(progressFill);
        element.appendChild(progress);

        const hint = document.createElement('div');
        hint.className = 'clip-stage-hint';
        element.appendChild(hint);

        host.appendChild(element);

        const stage = {
            element,
            slots,
            active: null,
            lastRequest: null,      // { items, options } of the last play() on this stage, for replay
            onActivate: null,       // set by the player: click / Enter / Space on the stage
            show() {
                element.hidden = false;
                if (settings.onShow) settings.onShow();
            },
            hide() {
                element.hidden = true;
                slots.forEach(slot => {
                    slot.video.pause();
                });
                if (settings.onHide) settings.onHide();
            },
            setTitle(text) {
                title.textContent = text || '';
            },
            setStatus(text) {
                status.textContent = text || '';
            },
            setProgress(fraction) {
                progressFill.style.width = (clamp(fraction, 0, 1) * 100).toFixed(2) + '%';
            },
            setPlaying(playing) {
                element.classList.toggle('is-playing', playing);
                hint.textContent = playing ? '' : '▶';
            },
            setMessage(text) {
                element.classList.toggle('has-message', Boolean(text));
                hint.textContent = text || (element.classList.contains('is-playing') ? '' : '▶');
            },
            slotFor(index) {
                return slots.find(slot => slot.index === index) || null;
            },
            /** Load `item`'s picture into a free slot and park it on the first frame of the cut. */
            prepare(index, item, restoreClip) {
                const existing = stage.slotFor(index);
                if (existing) return existing.ready;
                const url = mediaUrl(item.clip);
                const startSeconds = item.startTrim / 1000;
                // Replaying the clip that is on screen (trim tweak, loop): seek it in place instead of reloading.
                const onScreen = stage.active && stage.active.index < 0 && stage.active.url === url ? stage.active : null;
                const slot = onScreen
                    || slots.find(candidate => candidate !== stage.active && candidate.index < 0)
                    || slots.find(candidate => candidate !== stage.active && candidate.index < index - 1);
                if (!slot) return Promise.resolve(false);
                slot.index = index;
                slot.usable = false;
                const token = ++slot.token;
                const video = slot.video;

                const load = () => new Promise(resolve => {
                    let settled = false;
                    const finish = ok => {
                        if (settled) return;
                        settled = true;
                        video.removeEventListener('loadeddata', onData);
                        video.removeEventListener('seeked', onSeeked);
                        video.removeEventListener('error', onError);
                        clearTimeout(timer);
                        resolve(ok);
                    };
                    const onSeeked = () => finish(true);
                    const seek = () => {
                        const limit = Number.isFinite(video.duration) ? Math.max(0, video.duration - 0.01) : startSeconds;
                        const target = Math.min(startSeconds, limit);
                        if (Math.abs(video.currentTime - target) < 0.001 && !video.seeking) {
                            finish(true);
                            return;
                        }
                        video.addEventListener('seeked', onSeeked);
                        video.currentTime = target;
                    };
                    const onData = () => seek();
                    const onError = () => finish(false);
                    const timer = setTimeout(() => finish(video.readyState >= 2), 8000);
                    video.addEventListener('error', onError);
                    video.pause();
                    if (slot.url === url && video.readyState >= 2 && !video.error) {
                        seek();
                    } else {
                        slot.url = url;
                        video.addEventListener('loadeddata', onData);
                        video.src = url;
                        video.load();
                    }
                });

                slot.ready = load().then(async ok => {
                    if (!ok && restoreClip && token === slot.token) {
                        // The clip file is probably gone from temp/: let the app re-create it once, then retry.
                        const restored = await restoreClip(item).catch(() => false);
                        if (restored && token === slot.token) {
                            slot.url = '';
                            ok = await load();
                        }
                    }
                    if (token !== slot.token) return false;
                    slot.usable = ok;
                    return ok;
                });
                return slot.ready;
            },
            /** Bring the slot prepared for `index` to the front. Returns false if it is not ready yet. */
            activate(index) {
                const slot = stage.slotFor(index);
                if (!slot || !slot.usable) return false;
                if (stage.active && stage.active !== slot) {
                    stage.active.video.pause();
                    stage.active.video.classList.remove('is-active');
                    stage.active.index = -1;
                }
                slot.video.classList.add('is-active');
                stage.active = slot;
                return true;
            },
            /** Freeze the picture that is on screen (its clip must not run past the cut). */
            freeze() {
                if (stage.active) stage.active.video.pause();
            },
            /** Forget every prepared picture that is not on screen. */
            release() {
                slots.forEach(slot => {
                    slot.video.pause();
                    if (slot !== stage.active) {
                        slot.index = -1;
                        slot.token++;
                    }
                });
                if (stage.active) stage.active.index = -1;
            }
        };

        const activate = event => {
            if (event.type === 'keydown') {
                if (event.key !== 'Enter' && event.key !== ' ') return;
                event.preventDefault();
            }
            event.stopPropagation();
            if (stage.onActivate) stage.onActivate();
        };
        element.addEventListener('click', activate);
        element.addEventListener('keydown', activate);
        stage.setPlaying(false);
        return stage;
    }

    // ----------------------------------------------------------------------------------------------------
    // Player
    // ----------------------------------------------------------------------------------------------------

    function create(options) {
        const settings = options || {};
        const cache = new Map();      // key -> { promise, buffer, bytes }
        let cacheBytes = 0;
        let context = null;
        let output = null;
        let decoder = null;
        let session = null;
        let sessionCounter = 0;
        let playLead = 0;             // learned start-up latency of <video>.play() on this machine (s)

        function ensureContext() {
            if (context) return context;
            const Ctor = root.AudioContext || root.webkitAudioContext;
            if (!Ctor) throw new Error('This browser has no Web Audio support.');
            try {
                context = new Ctor({ sampleRate: SAMPLE_RATE, latencyHint: 'interactive' });
            } catch (error) {
                context = new Ctor();
            }
            output = context.createGain();
            output.connect(context.destination);
            return context;
        }

        /** Decode on an offline context: allowed before the first user gesture, always 48 kHz. */
        function decode(data) {
            if (!decoder) {
                const Offline = root.OfflineAudioContext || root.webkitOfflineAudioContext;
                decoder = Offline ? new Offline(2, 1, SAMPLE_RATE) : ensureContext();
            }
            return new Promise((resolve, reject) => {
                const pending = decoder.decodeAudioData(data, resolve, reject);
                if (pending && pending.catch) pending.catch(reject);
            });
        }

        /** Context time of the sample that is leaving the speakers right now. */
        function audibleTime() {
            if (typeof context.getOutputTimestamp === 'function') {
                const stamp = context.getOutputTimestamp();
                if (stamp && stamp.contextTime > 0 && stamp.performanceTime > 0) {
                    const estimate = stamp.contextTime + (performance.now() - stamp.performanceTime) / 1000;
                    if (Math.abs(estimate - context.currentTime) < 1) return estimate;
                }
            }
            return context.currentTime - (context.outputLatency || context.baseLatency || 0);
        }

        function quantise(seconds) {
            return Math.round(seconds * context.sampleRate) / context.sampleRate;
        }

        function touch(key) {
            const entry = cache.get(key);
            if (entry) {
                cache.delete(key);
                cache.set(key, entry);
            }
            return entry;
        }

        function evict(keepKey) {
            for (const [key, entry] of cache) {
                if (cacheBytes <= CACHE_BYTES) break;
                if (key === keepKey || !entry.buffer) continue;
                cacheBytes -= entry.bytes;
                cache.delete(key);
            }
        }

        function loadAudio(item) {
            const spec = audioSpec(item);
            const cached = touch(spec.key);
            if (cached) return cached.promise;
            const entry = { promise: null, buffer: null, bytes: 0 };
            entry.promise = fetch(spec.url)
                .then(async response => {
                    if (!response.ok) {
                        let message = `HTTP ${response.status}`;
                        try {
                            message = (await response.json()).error || message;
                        } catch (ignored) { /* not JSON */ }
                        throw new Error(message);
                    }
                    return response.arrayBuffer();
                })
                .then(decode)
                .then(buffer => {
                    entry.buffer = buffer;
                    entry.bytes = buffer.length * buffer.numberOfChannels * 4;
                    cacheBytes += entry.bytes;
                    evict(spec.key);
                    return buffer;
                })
                .catch(error => {
                    cache.delete(spec.key);
                    throw error;
                });
            cache.set(spec.key, entry);
            return entry.promise;
        }

        function loadedBuffer(item) {
            const entry = touch(audioSpec(item).key);
            return entry ? entry.buffer : null;
        }

        function stopSources(current, immediately) {
            const now = context.currentTime;
            if (!immediately) {
                current.gain.gain.cancelScheduledValues(now);
                current.gain.gain.setValueAtTime(1, now);
                current.gain.gain.linearRampToValueAtTime(0, now + FADE_OUT);
            }
            current.sources.forEach(node => {
                node.onended = null;
                try {
                    node.stop(immediately ? 0 : now + FADE_OUT);
                } catch (ignored) { /* already stopped */ }
            });
            const gain = current.gain;
            setTimeout(() => gain.disconnect(), 100);
        }

        function finish(current, completed, error) {
            if (current.finished) return;
            current.finished = true;
            if (session === current) session = null;
            cancelAnimationFrame(current.frame);
            clearInterval(current.pumpTimer);
            stopSources(current, completed);
            const stage = current.stage;
            stage.release();
            stage.setPlaying(false);
            stage.setStatus('');
            if (error) {
                stage.setMessage(error.message || String(error));
            } else {
                stage.setMessage('');
                if (completed) stage.setProgress(1);
            }
            if (current.options.onEnd) {
                try {
                    current.options.onEnd({ completed, error: error || null });
                } catch (callbackError) {
                    console.error('ClipPlayer onEnd failed', callbackError);
                }
            }
            current.resolve({ completed, error: error || null });
            if (completed && current.options.loop && stage.lastRequest && stage.lastRequest.session === current) {
                play(stage, stage.lastRequest.items, stage.lastRequest.options);
            }
        }

        /** Schedule audio for every item whose buffer is there, up to SCHEDULE_AHEAD seconds ahead. */
        function pump(current) {
            if (current.finished || current.startTime === null) return;
            const items = current.items;
            while (current.scheduled < items.length) {
                const index = current.scheduled;
                if (index > 0 && current.cursor > context.currentTime + SCHEDULE_AHEAD) break;
                const item = items[index];
                const buffer = loadedBuffer(item);
                if (!buffer) {
                    if (!current.waiting.has(index)) {
                        current.waiting.add(index);
                        loadAudio(item).then(
                            () => {
                                current.waiting.delete(index);
                                pump(current);
                            },
                            error => finish(current, false, new Error(`Audio of "${item.title || 'clip'}" failed: ${error.message}`))
                        );
                    }
                    break;
                }
                const range = audioSpec(item).range(buffer);
                const offset = quantise(clamp(range.offset, 0, buffer.duration));
                const duration = quantise(Math.min(range.duration, buffer.duration - offset));
                if (!(duration > 0)) {
                    finish(current, false, new Error(`"${item.title || 'Clip'}" is trimmed to nothing.`));
                    return;
                }
                let when = current.cursor;
                if (when < context.currentTime + 0.01) {
                    // The buffer arrived after its slot: restart the clock just ahead of now (audible gap).
                    when = quantise(context.currentTime + UNDERRUN_LEAD);
                    current.underruns += 1;
                }
                const node = context.createBufferSource();
                node.buffer = buffer;
                node.connect(current.gain);
                node.start(when, offset, duration);
                if (index === items.length - 1) {
                    // requestAnimationFrame stops in background tabs: let the last source end the session too.
                    node.onended = () => setTimeout(() => finish(current, true), 60);
                }
                current.sources.push(node);
                current.plan[index] = { when, duration, offset };
                current.cursor = quantise(when + duration);
                current.scheduled += 1;
                if (current.options.onSchedule) current.options.onSchedule(index, current.plan[index]);
            }
            for (let ahead = current.scheduled; ahead < Math.min(items.length, current.scheduled + PRELOAD_ITEMS); ahead++) {
                loadAudio(items[ahead]).catch(() => { /* reported when it is this item's turn */ });
            }
        }

        function syncPicture(current, slot, item, plan, now) {
            const video = slot.video;
            if (now < plan.when - playLead) return;       // parked on the first frame of the cut
            const elapsed = clamp(now - plan.when, 0, plan.duration);
            const target = item.startTrim / 1000 + elapsed * item.speed;
            if (video.paused && !video.ended) {
                video.playbackRate = item.speed;
                const attempt = video.play();
                if (attempt && attempt.catch) attempt.catch(() => { /* interrupted by the next swap */ });
                return;
            }
            if (video.seeking || video.ended) return;
            const drift = video.currentTime - target;
            if (!current.calibrated && elapsed > 0.08) {
                // Learn how long play() takes to get going so the next clip starts on the beat.
                current.calibrated = true;
                playLead = clamp(playLead - 0.5 * drift / item.speed, 0, PLAY_LEAD_MAX);
            }
            let rate = item.speed;
            if (Math.abs(drift) > DRIFT_HARD) {
                video.currentTime = target + 0.02 * item.speed;
            } else if (Math.abs(drift) > DRIFT_DEADBAND) {
                rate = item.speed * (1 - clamp(drift * 4, -0.3, 0.3));
            }
            rate = clamp(rate, 0.25, 4);
            if (Math.abs(video.playbackRate - rate) > 0.005) video.playbackRate = rate;
            current.lastDrift = drift;
        }

        function tick(current) {
            if (current.finished) return;
            current.frame = requestAnimationFrame(() => tick(current));
            if (current.startTime === null) return;
            const now = audibleTime();
            const items = current.items;
            const stage = current.stage;

            // Which item is on air? The last scheduled one whose slot has (almost) begun.
            let onAir = current.current;
            while (onAir + 1 < current.scheduled && now >= current.plan[onAir + 1].when - SWAP_LEAD) onAir += 1;
            if (onAir !== current.current) {
                current.current = onAir;
                current.pictureShown = false;
                current.calibrated = false;
                const item = items[onAir];
                stage.setTitle(item.title);
                stage.setStatus(items.length > 1 ? `${onAir + 1} / ${items.length}` : '');
                if (current.options.onItemStart) {
                    try {
                        current.options.onItemStart(onAir, item);
                    } catch (callbackError) {
                        console.error('ClipPlayer onItemStart failed', callbackError);
                    }
                }
            }
            if (onAir >= 0) {
                const plan = current.plan[onAir];
                if (!current.pictureShown) {
                    stage.prepare(onAir, items[onAir], settings.restoreClip);
                    current.pictureShown = stage.activate(onAir);
                    if (!current.pictureShown) stage.freeze();
                }
                for (let ahead = onAir + 1; ahead < Math.min(items.length, onAir + PICTURE_SLOTS); ahead++) {
                    stage.prepare(ahead, items[ahead], settings.restoreClip);
                }
                if (current.pictureShown && stage.active) syncPicture(current, stage.active, items[onAir], plan, now);
                const within = clamp((now - plan.when) / plan.duration, 0, 1);
                stage.setProgress((onAir + within) / items.length);
                if (current.options.onProgress) current.options.onProgress(onAir, within);
            }
            if (current.scheduled === items.length) {
                const last = current.plan[items.length - 1];
                if (now >= last.when + last.duration) finish(current, true);
            }
        }

        function stop() {
            if (session) finish(session, false);
        }

        /**
         * Play `items` back to back on `stage`. Resolves with {completed, error} when playback ends or is
         * replaced by another play()/stop(). Must be called from a user gesture the first time (autoplay policy).
         */
        function play(stage, rawItems, playOptions) {
            const items = (Array.isArray(rawItems) ? rawItems : [rawItems]).filter(Boolean).map(normaliseItem);
            const opts = playOptions || {};
            stop();
            let ctx;
            try {
                ctx = ensureContext();
            } catch (error) {
                stage.show();
                stage.setMessage(error.message);
                return Promise.resolve({ completed: false, error });
            }
            if (!items.length) return Promise.resolve({ completed: false, error: null });
            // resume() must be called synchronously inside the user gesture; it settles a few ms later.
            const running = ctx.state === 'running' ? Promise.resolve() : Promise.race([
                Promise.resolve(ctx.resume()).catch(() => { /* surfaced by the state check below */ }),
                new Promise(resolve => setTimeout(resolve, 1000))
            ]);

            const current = {
                id: ++sessionCounter,
                stage,
                items,
                options: opts,
                gain: ctx.createGain(),
                sources: [],
                plan: [],
                waiting: new Set(),
                scheduled: 0,
                current: -1,
                cursor: 0,
                startTime: null,
                underruns: 0,
                lastDrift: 0,
                pictureShown: false,
                calibrated: false,
                finished: false,
                frame: 0,
                pumpTimer: 0,
                resolve: null
            };
            current.gain.connect(output);
            session = current;
            stage.lastRequest = { items: rawItems, options: opts, session: current };
            stage.onActivate = () => {
                if (session && session.stage === stage) {
                    stop();
                } else if (stage.lastRequest) {
                    play(stage, stage.lastRequest.items, stage.lastRequest.options);
                }
            };

            stage.show();
            stage.release();
            stage.setMessage('');
            stage.setPlaying(true);
            stage.setProgress(0);
            stage.setTitle(items[0].title);
            stage.setStatus(items.length > 1 ? `1 / ${items.length}` : '');

            const done = new Promise(resolve => {
                current.resolve = resolve;
            });

            const picture = stage.prepare(0, items[0], settings.restoreClip);
            const pictureOrTimeout = Promise.race([
                picture,
                new Promise(resolve => setTimeout(() => resolve(false), PICTURE_TIMEOUT))
            ]);
            for (let ahead = 1; ahead < Math.min(items.length, PRELOAD_ITEMS); ahead++) {
                loadAudio(items[ahead]).catch(() => { /* reported when it is this item's turn */ });
                if (ahead < PICTURE_SLOTS - 1) stage.prepare(ahead, items[ahead], settings.restoreClip);
            }

            Promise.all([loadAudio(items[0]), pictureOrTimeout, running]).then(
                () => {
                    if (current.finished) return;
                    if (ctx.state !== 'running') {
                        finish(current, false, new Error('Audio is blocked by the browser. Click to play.'));
                        return;
                    }
                    current.startTime = quantise(ctx.currentTime + START_LEAD);
                    current.cursor = current.startTime;
                    pump(current);
                    current.pumpTimer = setInterval(() => pump(current), 100);
                },
                error => finish(current, false, new Error(`Audio of "${items[0].title || 'clip'}" failed: ${error.message}`))
            );
            current.frame = requestAnimationFrame(() => tick(current));
            return done;
        }

        return {
            play,
            stop,
            /** Warm the audio cache (e.g. for the first match of a new result card). */
            prefetch(item) {
                try {
                    return loadAudio(normaliseItem(item)).then(() => true, () => false);
                } catch (error) {
                    return Promise.resolve(false);
                }
            },
            isPlaying(stage) {
                return Boolean(session) && (!stage || session.stage === stage);
            },
            /** Diagnostics of the running (or given) session: used by the timing tests. */
            get session() {
                return session;
            },
            get context() {
                return context;
            },
            get output() {
                return output;
            },
            ensureContext,
            audibleTime
        };
    }

    root.ClipPlayer = { create, createStage, mediaUrl, audioSpec, normaliseItem };
})(window);

const AUTOSAVE_DELAY = 800;
const MAX_SAVED_SENTENCES = 50;
const TIMELINE_TRIM_MAX = 8000;
const TIMELINE_TRIM_STEP = 50;

let videoTimeoutId = null;
let searchCounter = 0;
let sentenceSearchCounter = 0;
let globalPhraseContainerIDCounter = 0;

let sentences = [];
let combinedTranscriptions = {}; // Map of file path -> combined transcription text
const CORPUS_DEFAULT_BUTTON_TEXT = 'Load corpus';
const CORPUS_RELOAD_BUTTON_TEXT = 'Reload corpus';
let libraryFiles = [];
let projectList = [];
let activeProject = null;
let autosaveTimer = null;
let isApplyingProject = false;
let isAutosaving = false;

const elements = {};
const dragState = { type: null, data: null, originalItemPositions: null };

// Sentence autocomplete state
let sentenceAutocompleteContainer = null;
let sentenceAutocompleteState = {
    selectedIndex: -1,
    suggestions: [],
    currentAcceptedText: '',
    isVisible: false,
    isTabCompletion: false,  // Flag to track if change came from Tab
    // Auto-semicolon undo support
    lastAutoSemicolon: null,  // { previousValue: string, timestamp: number } or null
    skipAutoSemicolon: false  // Flag to skip auto-semicolon after undo
};

// Floating preview state
let floatingPreviewContainer = null;
let currentSelectedCard = null;

// Helper function to get correct URL for video files
function getVideoUrl(filePath) {
    if (!filePath) return '';
    // Files in temp/ directory should use /temp/ route, others use /static/
    if (filePath.startsWith('temp/')) {
        return '/' + filePath;
    }
    return '/static/' + filePath;
}

// Helper function to format trimmed play length
// Calculates actual playable duration by subtracting default trim values
function formatTrimmedLength(clipDurationMs, startTrimMs = 450, endTrimMs = 450) {
    // Handle null, undefined, or invalid values
    if (clipDurationMs == null || clipDurationMs === '') {
        return '';
    }
    // Convert string to number if needed
    const clipDuration = typeof clipDurationMs === 'string' ? parseFloat(clipDurationMs) : clipDurationMs;
    // Check if it's a valid positive number
    if (typeof clipDuration !== 'number' || isNaN(clipDuration) || clipDuration <= 0) {
        return '';
    }
    // Calculate actual playable duration by subtracting default trims
    const playableDuration = Math.max(0, clipDuration - startTrimMs - endTrimMs);
    if (playableDuration <= 0) {
        return '';
    }
    // If >= 1 second, show as seconds with 1 decimal place, otherwise show as milliseconds
    if (playableDuration >= 1000) {
        return `${(playableDuration / 1000).toFixed(1)}s `;
    } else {
        return `${Math.round(playableDuration)}ms `;
    }
}

// Trigger a sentence search configured for a specific silence duration range
// selected via the "Search silence" slider.
function handleSearchSilence() {
    if (!activeProject) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        return;
    }

    if (!elements.silenceRangeMinInput || !elements.silenceRangeMaxInput) {
        return;
    }

    let minMs = parseInt(elements.silenceRangeMinInput.value, 10) || 0;
    let maxMs = parseInt(elements.silenceRangeMaxInput.value, 10) || 0;
    minMs = Math.max(0, Math.min(10000, minMs));
    maxMs = Math.max(0, Math.min(10000, maxMs));
    if (minMs > maxMs) {
        maxMs = minMs;
        elements.silenceRangeMaxInput.value = String(maxMs);
    }

    const minSilence = minMs / 1000;
    const maxSilence = maxMs / 1000;

    // For a focused "search silence" experience, search for pauses whose
    // duration falls between [minSilence, maxSilence]. We set the threshold
    // to 0 so all gaps are evaluated purely by silence duration.
    const silenceWordThreshold = 0;

    // Reflect in the existing numeric inputs so the backend uses them
    if (elements.minSilenceInput) {
        elements.minSilenceInput.value = String(minSilence);
    }
    if (elements.maxSilenceInput) {
        elements.maxSilenceInput.value = String(maxSilence);
    }
    if (elements.silenceWordThresholdInput) {
        elements.silenceWordThresholdInput.value = String(silenceWordThreshold);
    }

    // Persist these preferences to the active project
    handleSilencePreferenceChange();

    // Determine max results per segment once so both the backend and
    // frontend grouping logic use the same cap.
    let maxPerSegment = 25;
    const maxInput = document.getElementById('maxResultsPerSegment');
    if (maxInput) {
        const parsed = parseInt(maxInput.value, 10);
        if (Number.isInteger(parsed) && parsed > 0) {
            maxPerSegment = parsed;
        }
    }

    // Clear previous results before starting new search
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }

    // Show loading state with cancel option
    let abortController = new AbortController();
    let reader = null;

    showSearchLoading(`Searching silences between ${minSilence.toFixed(1)}s and ${maxSilence.toFixed(1)}s...`, () => {
        console.log('[Client] ❌ CANCEL SILENCE SEARCH requested by user');
        
        // Send cancellation request to backend
        if (abortController) {
            fetch('/cancel_search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ search_id: 'silence_search' })
            }).catch(err => {
                console.warn('Failed to send cancel request:', err);
            });
        }
        
        // Abort the fetch and close the stream
        try {
            abortController.abort();
            if (reader) {
                reader.cancel().catch(() => {}); // Ignore errors during cancellation
            }
        } catch (e) {
            // Ignore errors during abort
        }
        hideSearchLoading();
        console.log('[Client] ✓ Silence search cancelled, connection closed');
    });

    // Use SSE endpoint for incremental results
    fetch('/search_silences_stream', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            files: selectedFiles,
            minSilence,
            maxSilence,
            maxResultsPerSegment: maxPerSegment
        }),
        signal: abortController.signal
    })
        .then(response => {
            reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            function readStream() {
                return reader.read().then(({ done, value }) => {
                    if (done) {
                        hideSearchLoading();
                        return;
                    }

                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    for (const line of lines) {
                        if (!line.trim() || !line.startsWith('data: ')) {
                            continue;
                        }

                        try {
                            const jsonStr = line.substring(6); // Remove 'data: ' prefix
                            const data = JSON.parse(jsonStr);

                            if (data.done) {
                                hideSearchLoading();
                                const totalSilences = data.total_silences || 0;
                                if (totalSilences === 0) {
                                    showNoResults(
                                        `No silences found between ${minSilence.toFixed(1)}s and ${maxSilence.toFixed(1)}s.`
                                    );
                                }
                                return;
                            }

                            if (data.error) {
                                hideSearchLoading();
                                showNoResults(`Error: ${data.error}`);
                                return;
                            }

                            // Process result (data contains phrase and files)
                            if (data.phrase && data.files) {
                                // For silence search, check if bucket already exists and update it
                                const existingContainer = Array.from(elements.resultsContainer.children).find(
                                    container => {
                                        const title = container.querySelector('.phrase-title');
                                        return title && title.textContent === data.phrase;
                                    }
                                );

                                if (existingContainer) {
                                    // Update existing container - find the listbox and update its options
                                    const listbox = existingContainer.querySelector('select');
                                    if (listbox) {
                                        // Clear existing options except the first one (if it's a placeholder)
                                        const currentOptions = Array.from(listbox.options);
                                        const existingFiles = currentOptions
                                            .map(opt => opt.value)
                                            .filter(val => val && val !== '');
                                        
                                        // Add new files that don't already exist
                                        data.files.forEach(fileObj => {
                                            const filePath = typeof fileObj === 'object' ? fileObj.file : fileObj;
                                            if (!existingFiles.includes(filePath)) {
                                                const option = document.createElement('option');
                                                option.value = filePath;
                                                if (typeof fileObj === 'object' && fileObj.source_video) {
                                                    const videoName = fileObj.source_video.split('/').pop();
                                                    // For silence searches, clips have 0/0 trims, so use full duration
                                                    const defaultStartTrim = (typeof fileObj.silence_start === 'number' && typeof fileObj.silence_end === 'number') ? 0 : 450;
                                                    const defaultEndTrim = (typeof fileObj.silence_start === 'number' && typeof fileObj.silence_end === 'number') ? 0 : 450;
                                                    const trimmedLength = formatTrimmedLength(fileObj.duration_ms, defaultStartTrim, defaultEndTrim);
                                                    option.text = `Match ${listbox.options.length} (${trimmedLength}${videoName})`;
                                                    if (fileObj.duration_ms) {
                                                        option.dataset.durationMs = String(fileObj.duration_ms);
                                                    }
                                                } else {
                                                    option.text = `Match ${listbox.options.length}`;
                                                }
                                                listbox.appendChild(option);
                                            }
                                        });
                                    }
                                } else {
                                    // Create new container for this bucket
                                    updateDropdowns([data]);
                                }
                            }

                        } catch (err) {
                            console.error('[Client] Error parsing SSE data:', err, 'Line:', line);
                        }
                    }

                    return readStream();
                });
            }

            return readStream();
        })
        .catch(error => {
            if (error.name === 'AbortError') {
                console.log('[Client] Silence search was cancelled');
                return;
            }
            hideSearchLoading();
            console.error('[Client] Error searching silences:', error);
            showNoResults('An error occurred while searching silences. Please try again.');
        });
}

function generateId(prefix = 'id') {
    return `${prefix}_${Math.random().toString(36).slice(2, 10)}_${Date.now()}`;
}

// Mobile Navigation Functions
function isMobileLayout() {
    return window.innerWidth <= 1024;
}

function switchMobileSection(sectionName) {
    if (!isMobileLayout()) {
        return;
    }

    // Get workspace reference
    const workspace = document.querySelector('.workspace[data-mobile-section="search"]');
    
    // Remove mobile-showing classes from workspace (will be re-added if needed)
    if (workspace) {
        workspace.classList.remove('mobile-showing-results');
        workspace.classList.remove('mobile-showing-viewer');
    }

    // Hide all mobile sections
    document.querySelectorAll('[data-mobile-section]').forEach(section => {
        section.classList.remove('mobile-active');
    });

    // Special handling for results section (nested inside workspace)
    if (sectionName === 'results') {
        // Show workspace and mark it as showing results (but don't mark workspace as mobile-active)
        if (workspace) {
            workspace.classList.add('mobile-showing-results');
        }
        // Mark results section as active
        const resultsSection = document.querySelector('[data-mobile-section="results"]');
        if (resultsSection) {
            resultsSection.classList.add('mobile-active');
        }
    } else if (sectionName === 'viewer') {
        // Show workspace and mark it as showing viewer
        if (workspace) {
            workspace.classList.add('mobile-showing-viewer');
        }
        // Mark viewer section as active
        const viewerSection = document.querySelector('[data-mobile-section="viewer"]');
        if (viewerSection) {
            viewerSection.classList.add('mobile-active');
        }
    } else {
        // Show selected section normally
        const targetSection = document.querySelector(`[data-mobile-section="${sectionName}"]`);
        if (targetSection) {
            targetSection.classList.add('mobile-active');
        }
    }

    // Update nav buttons
    document.querySelectorAll('.mobile-nav-button').forEach(button => {
        button.classList.remove('active');
    });
    const activeButton = document.querySelector(`[data-mobile-nav="${sectionName}"]`);
    if (activeButton) {
        activeButton.classList.add('active');
    }

    // Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });

    // Store active section
    try {
        sessionStorage.setItem('mobileActiveSection', sectionName);
    } catch (e) {
        // Ignore storage errors
    }
}

function initializeMobileNavigation() {
    const bottomNav = document.getElementById('mobileBottomNav');
    if (!bottomNav) {
        return;
    }

    // Bind click handlers to nav buttons
    document.querySelectorAll('.mobile-nav-button').forEach(button => {
        button.addEventListener('click', () => {
            const section = button.dataset.mobileNav;
            if (section) {
                switchMobileSection(section);
            }
        });
    });

    // Handle window resize
    let resizeTimeout;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            if (isMobileLayout()) {
                // Ensure mobile layout is active
                const currentSection = document.querySelector('[data-mobile-section].mobile-active');
                if (!currentSection) {
                    // Restore from sessionStorage or default to search
                    const savedSection = sessionStorage.getItem('mobileActiveSection') || 'search';
                    switchMobileSection(savedSection);
                }
            } else {
                // Desktop layout - remove mobile-active classes (CSS handles visibility)
                document.querySelectorAll('[data-mobile-section]').forEach(section => {
                    section.classList.remove('mobile-active');
                });
            }
        }, 100);
    });

    // Initialize on load
    if (isMobileLayout()) {
        // Restore saved section or default to search
        const savedSection = sessionStorage.getItem('mobileActiveSection') || 'search';
        switchMobileSection(savedSection);
    }
    // Desktop layout doesn't need mobile-active classes - CSS handles it
}

document.addEventListener('DOMContentLoaded', () => {
    cacheElements();
    bindEventListeners();
    initializeUI();
    initializeSearchTabs();
    fetchLibraryFiles();
    bootstrapProjects();
    initializeMobileNavigation();
});

function initializeSearchTabs() {
    const tabs = document.querySelectorAll('.search-tab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            // Remove active from all tabs and content
            document.querySelectorAll('.search-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.search-tab-content').forEach(c => c.classList.remove('active'));
            
            // Add active to clicked tab
            tab.classList.add('active');
            
            // Show corresponding content
            const tabName = tab.dataset.tab;
            const content = document.getElementById(`tab-${tabName}`);
            if (content) content.classList.add('active');
        });
    });
}

function resetCorpusState() {
    sentences = [];
    combinedTranscriptions = {}; // Clear combined transcriptions
    if (elements.suggestions) {
        elements.suggestions.innerHTML = '';
    }
    if (elements.loadSentencesButton) {
        elements.loadSentencesButton.textContent = CORPUS_DEFAULT_BUTTON_TEXT;
        elements.loadSentencesButton.disabled = false;
    }
}

function buildCorpusEntry(prev, current, next, source = null) {
    const safePrev = typeof prev === 'string' ? prev : '';
    const safeCurrent = typeof current === 'string' ? current : '';
    const safeNext = typeof next === 'string' ? next : '';
    return {
        prev: safePrev,
        current: safeCurrent,
        next: safeNext,
        prevNormalized: safePrev.toLowerCase(),
        currentNormalized: safeCurrent.toLowerCase(),
        nextNormalized: safeNext.toLowerCase(),
        source
    };
}

function normalizeCorpusEntry(entry) {
    if (!entry) {
        return null;
    }
    if (Array.isArray(entry)) {
        const [prev, current, next] = entry;
        return buildCorpusEntry(prev, current, next);
    }
    if (typeof entry === 'object') {
        return buildCorpusEntry(entry.prev, entry.current, entry.next, entry.file || entry.source || null);
    }
    return null;
}

function escapeRegExp(value) {
    return typeof value === 'string' ? value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') : '';
}

function highlightMatch(text, query) {
    if (typeof text !== 'string' || typeof query !== 'string' || !query.trim()) {
        return text || '';
    }
    const pattern = escapeRegExp(query.trim());
    if (!pattern) {
        return text;
    }
    const regex = new RegExp(pattern, 'gi');
    return text.replace(regex, match => `<b>${match}</b>`);
}

// Highlight only the prefix match at word boundaries (faster, no fuzzy)
function highlightPrefixMatch(text, query) {
    if (typeof text !== 'string' || typeof query !== 'string' || !query.trim()) {
        return escapeHtml(text || '');
    }
    
    const queryLower = query.trim().toLowerCase();
    const textLower = text.toLowerCase();
    
    // Find where the query matches at word boundary
    const queryWords = queryLower.split(/\s+/);
    const textWords = text.split(/\s+/);
    const textWordsLower = textLower.split(/\s+/);
    
    // Find the starting word index where query matches
    let matchStartWordIndex = -1;
    for (let i = 0; i <= textWordsLower.length - queryWords.length; i++) {
        let matches = true;
        for (let j = 0; j < queryWords.length; j++) {
            const textWord = textWordsLower[i + j];
            const queryWord = queryWords[j];
            // Last word can be prefix match, others must be exact
            if (j === queryWords.length - 1) {
                if (!textWord.startsWith(queryWord)) {
                    matches = false;
                    break;
                }
            } else if (textWord !== queryWord) {
                matches = false;
                break;
            }
        }
        if (matches) {
            matchStartWordIndex = i;
            break;
        }
    }
    
    if (matchStartWordIndex === -1) {
        // No word boundary match, try simple indexOf for substring
        const idx = textLower.indexOf(queryLower);
        if (idx !== -1) {
            const before = escapeHtml(text.substring(0, idx));
            const match = escapeHtml(text.substring(idx, idx + query.trim().length));
            const after = escapeHtml(text.substring(idx + query.trim().length));
            return `${before}<b>${match}</b>${after}`;
        }
        return escapeHtml(text);
    }
    
    // Build highlighted text
    const result = [];
    for (let i = 0; i < textWords.length; i++) {
        if (i > 0) result.push(' ');
        
        if (i >= matchStartWordIndex && i < matchStartWordIndex + queryWords.length) {
            const queryWordIndex = i - matchStartWordIndex;
            const queryWord = queryWords[queryWordIndex];
            const textWord = textWords[i];
            
            // Highlight the matching prefix of this word
            const matchLen = queryWord.length;
            const matchedPart = escapeHtml(textWord.substring(0, matchLen));
            const restPart = escapeHtml(textWord.substring(matchLen));
            result.push(`<b>${matchedPart}</b>${restPart}`);
        } else {
            result.push(escapeHtml(textWords[i]));
        }
    }
    
    return result.join('');
}

// Helper to escape HTML entities
function escapeHtml(text) {
    if (typeof text !== 'string') return '';
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function cacheElements() {
    elements.autosaveStatus = document.getElementById('autosaveStatus');
    elements.projectSelect = document.getElementById('projectSelect');
    elements.projectNameInput = document.getElementById('projectNameInput');
    elements.newProjectButton = document.getElementById('newProjectButton');
    elements.duplicateProjectButton = document.getElementById('duplicateProjectButton');
    elements.deleteProjectButton = document.getElementById('deleteProjectButton');
    elements.saveSentenceButton = document.getElementById('saveSentenceButton');
    elements.projectSentenceList = document.getElementById('projectSentenceList');
    elements.loadSentencesButton = document.getElementById('loadSentencesButton');
    elements.inputText = document.getElementById('inputText');
    elements.suggestions = document.getElementById('suggestions');
    elements.phraseInput = document.getElementById('phraseInput');
    elements.sentenceInput = document.getElementById('sentenceInput');
    elements.storyPromptInput = document.getElementById('storyPromptInput');
    elements.generateStoryButton = document.getElementById('generateStoryButton');
    elements.copyLLMPromptButton = document.getElementById('copyLLMPromptButton');
    elements.insertResponseButton = document.getElementById('insertResponseButton');
    elements.insertResponseContainer = document.getElementById('insertResponseContainer');
    elements.llmResponseInput = document.getElementById('llmResponseInput');
    elements.processResponseButton = document.getElementById('processResponseButton');
    elements.cancelInsertResponseButton = document.getElementById('cancelInsertResponseButton');
    elements.searchPhrasesButton = document.getElementById('searchPhrasesButton');
    elements.searchSentenceButton = document.getElementById('searchLongestSegmentsButton');
    elements.generateStoryButton = document.getElementById('generateStoryButton');
    elements.startOverButton = document.getElementById('startOverButton');
    elements.availableFileSelector = document.getElementById('availableFileSelector');
    elements.chosenFileSelector = document.getElementById('chosenFileSelector');
    elements.selectAllAvailableButton = document.getElementById('selectAllAvailableButton');
    elements.addToChosenButton = document.getElementById('addToChosenButton');
    elements.removeFromChosenButton = document.getElementById('removeFromChosenButton');
    elements.clearChosenButton = document.getElementById('clearChosenButton');
    elements.refreshFilesButton = document.getElementById('refreshFilesButton');
    elements.minSilenceInput = document.getElementById('minSilenceInput');
    elements.maxSilenceInput = document.getElementById('maxSilenceInput');
    elements.silenceWordThresholdInput = document.getElementById('silenceWordThresholdInput');
    elements.silenceRangeMinInput = document.getElementById('silenceRangeMinInput');
    elements.silenceRangeMaxInput = document.getElementById('silenceRangeMaxInput');
    elements.silenceRangeMinLabel = document.getElementById('silenceRangeMinLabel');
    elements.silenceRangeMaxLabel = document.getElementById('silenceRangeMaxLabel');
    elements.searchSilenceButton = document.getElementById('searchSilenceButton');
    elements.playAllButton = document.getElementById('playAllButton');
    elements.addAllMatchesButton = document.getElementById('addAllMatchesButton');
    elements.timelineList = document.getElementById('timelineList');
    elements.timelinePlayButton = document.getElementById('timelinePlayButton');
    elements.timelineMergeButton = document.getElementById('timelineMergeButton');
    elements.timelineClearButton = document.getElementById('timelineClearButton');
    elements.loopMergedVideo = document.getElementById('loopMergedVideo');
    elements.resultsContainer = document.getElementById('resultsContainer');
    elements.videoPlayer = document.getElementById('videoPlayer');
}

function bindEventListeners() {
    if (elements.projectSelect) {
        elements.projectSelect.addEventListener('change', event => {
            const projectId = event.target.value;
            if (projectId && (!activeProject || activeProject.id !== projectId)) {
                loadProject(projectId);
            }
        });
    }

    // Bind search silence controls (dual range: from/to)
    if (elements.silenceRangeMinInput && elements.silenceRangeMaxInput &&
        elements.silenceRangeMinLabel && elements.silenceRangeMaxLabel) {
        const clampMs = value => Math.max(0, Math.min(10000, value));
        const silenceRangeFill = document.getElementById('silenceRangeFill');
        const minRange = 0;
        const maxRange = 10000;

        const updateSilenceRange = () => {
            let minMs = parseInt(elements.silenceRangeMinInput.value, 10) || 0;
            let maxMs = parseInt(elements.silenceRangeMaxInput.value, 10) || 0;
            minMs = clampMs(minMs);
            maxMs = clampMs(maxMs);
            
            // Ensure min <= max
            if (minMs > maxMs) {
                // If min exceeds max, swap them
                const temp = minMs;
                minMs = maxMs;
                maxMs = temp;
                elements.silenceRangeMinInput.value = String(minMs);
                elements.silenceRangeMaxInput.value = String(maxMs);
            }
            
            // Update labels
            const minSec = minMs / 1000;
            const maxSec = maxMs / 1000;
            elements.silenceRangeMinLabel.textContent = `${minSec.toFixed(1)} s`;
            elements.silenceRangeMaxLabel.textContent = `${maxSec.toFixed(1)} s`;
            
            // Update fill bar position
            if (silenceRangeFill) {
                const slider = silenceRangeFill.parentElement;
                const sliderWidth = slider.offsetWidth;
                const padding = 9; // 9px padding on each side
                const trackWidth = sliderWidth - (padding * 2);
                
                const minPercent = (minMs - minRange) / (maxRange - minRange);
                const maxPercent = (maxMs - minRange) / (maxRange - minRange);
                
                const leftPos = padding + (minPercent * trackWidth);
                const rightPos = padding + (maxPercent * trackWidth);
                const width = rightPos - leftPos;
                
                silenceRangeFill.style.left = `${leftPos}px`;
                silenceRangeFill.style.width = `${width}px`;
            }
        };

        // Prevent min from exceeding max and vice versa
        elements.silenceRangeMinInput.addEventListener('input', () => {
            const minMs = parseInt(elements.silenceRangeMinInput.value, 10) || 0;
            const maxMs = parseInt(elements.silenceRangeMaxInput.value, 10) || 0;
            if (minMs > maxMs) {
                elements.silenceRangeMinInput.value = String(maxMs);
            }
            updateSilenceRange();
        });

        elements.silenceRangeMaxInput.addEventListener('input', () => {
            const minMs = parseInt(elements.silenceRangeMinInput.value, 10) || 0;
            const maxMs = parseInt(elements.silenceRangeMaxInput.value, 10) || 0;
            if (maxMs < minMs) {
                elements.silenceRangeMaxInput.value = String(minMs);
            }
            updateSilenceRange();
        });

        // Initialize
        updateSilenceRange();
        
        // Update on window resize to recalculate fill bar position
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(updateSilenceRange, 100);
        });
    }

    if (elements.searchSilenceButton) {
        elements.searchSilenceButton.addEventListener('click', handleSearchSilence);
    }

    if (elements.newProjectButton) {
        elements.newProjectButton.addEventListener('click', handleCreateProject);
    }

    if (elements.duplicateProjectButton) {
        elements.duplicateProjectButton.addEventListener('click', handleDuplicateProject);
    }

    if (elements.deleteProjectButton) {
        elements.deleteProjectButton.addEventListener('click', handleDeleteProject);
    }

    if (elements.projectNameInput) {
        elements.projectNameInput.addEventListener('input', handleProjectNameInput);
        elements.projectNameInput.addEventListener('blur', handleProjectNameBlur);
    }

    if (elements.saveSentenceButton) {
        elements.saveSentenceButton.addEventListener('click', handleSaveSentence);
    }

    if (elements.projectSentenceList) {
        elements.projectSentenceList.addEventListener('click', handleProjectSentenceListClick);
    }

    if (elements.loadSentencesButton) {
        elements.loadSentencesButton.addEventListener('click', handleLoadSentences);
    }

    if (elements.inputText) {
        elements.inputText.addEventListener('input', updateAutocomplete);
    }

    if (elements.phraseInput) {
        elements.phraseInput.addEventListener('input', () => {
            updateProjectData(data => {
                data.phraseInput = elements.phraseInput.value;
            });
        });
        elements.phraseInput.addEventListener('keydown', event => {
            if (event.key === 'Enter') {
                event.preventDefault();
                searchPhrases();
            }
        });
    }

    // Initialize autocomplete container for sentence input
    if (elements.sentenceInput) {
        // Create autocomplete container
        sentenceAutocompleteContainer = document.createElement('div');
        sentenceAutocompleteContainer.id = 'sentenceAutocomplete';
        sentenceAutocompleteContainer.className = 'autocomplete-suggestions';
        sentenceAutocompleteContainer.style.cssText = 'position: absolute; z-index: 1000; max-height: 300px; overflow-y: auto; background: var(--bg-secondary, #1a1a1a); border: 1px solid var(--border, #333); border-radius: 4px; display: none; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);';
        elements.sentenceInput.parentElement.style.position = 'relative';
        elements.sentenceInput.parentElement.appendChild(sentenceAutocompleteContainer);

        elements.sentenceInput.addEventListener('input', (e) => {
            const currentValue = elements.sentenceInput.value.trim();
            // Reset accepted text if user manually edited (not from Tab completion)
            if (!sentenceAutocompleteState.isTabCompletion) {
                sentenceAutocompleteState.currentAcceptedText = currentValue;
            }
            sentenceAutocompleteState.isTabCompletion = false;  // Reset flag
            
            // Clear auto-semicolon undo state on manual input (but not if it just happened)
            // Check if this input is NOT from the auto-semicolon we just set (within 50ms)
            const autoSemicolon = sentenceAutocompleteState.lastAutoSemicolon;
            if (autoSemicolon && (Date.now() - autoSemicolon.timestamp > 50)) {
                sentenceAutocompleteState.lastAutoSemicolon = null;
            }
            
            updateProjectData(data => {
                data.currentSentence = currentValue;
            });
            updateSentenceAutocomplete();
        });
        
        elements.sentenceInput.addEventListener('keydown', event => {
            if (event.key === 'Enter') {
                event.preventDefault();
                // If a suggestion is selected, use it; otherwise search with current input
                if (sentenceAutocompleteState.isVisible && 
                    sentenceAutocompleteState.selectedIndex >= 0 && 
                    sentenceAutocompleteState.suggestions[sentenceAutocompleteState.selectedIndex]) {
                    const selectedEntry = sentenceAutocompleteState.suggestions[sentenceAutocompleteState.selectedIndex];
                    // Get the text without context prefix (main text only)
                    let textToInsert = selectedEntry.current;
                    if (selectedEntry.prevContext && textToInsert.toLowerCase().startsWith(selectedEntry.prevContext.toLowerCase())) {
                        textToInsert = textToInsert.substring(selectedEntry.prevContext.length).trimStart();
                    }
                    selectSentenceSuggestion(textToInsert);
                    // After selecting suggestion, trigger search
                    handleSentenceSearch();
                } else {
                    handleSentenceSearch();
                }
                hideSentenceAutocomplete();
            } else if (event.key === 'Tab') {
                event.preventDefault();
                // If autocomplete not visible, try to show it first
                if (!sentenceAutocompleteState.isVisible) {
                    updateSentenceAutocomplete();
                }
                // Accept next word if we have suggestions
                if (sentenceAutocompleteState.suggestions.length > 0) {
                    acceptNextWord();
                }
            } else if (event.key === 'ArrowDown') {
                event.preventDefault();
                navigateSentenceAutocomplete(1);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                navigateSentenceAutocomplete(-1);
            } else if (event.key === 'Escape') {
                hideSentenceAutocomplete();
            } else if (event.key === 'Backspace') {
                // Check if we can undo an auto-semicolon insertion
                if (undoAutoSemicolon()) {
                    event.preventDefault();
                }
            }
        });

        // Hide autocomplete when clicking outside
        document.addEventListener('click', (e) => {
            if (sentenceAutocompleteContainer && !sentenceAutocompleteContainer.contains(e.target) && e.target !== elements.sentenceInput) {
                hideSentenceAutocomplete();
            }
        });
    }

    if (elements.searchPhrasesButton) {
        elements.searchPhrasesButton.addEventListener('click', searchPhrases);
    }

    if (elements.searchSentenceButton) {
        elements.searchSentenceButton.addEventListener('click', handleSentenceSearch);
    }

    if (elements.generateStoryButton) {
        elements.generateStoryButton.addEventListener('click', handleGenerateStory);
        if (elements.copyLLMPromptButton) {
            elements.copyLLMPromptButton.addEventListener('click', handleCopyLLMPrompt);
        }
        if (elements.insertResponseButton) {
            elements.insertResponseButton.addEventListener('click', handleShowInsertResponse);
        }
        if (elements.processResponseButton) {
            elements.processResponseButton.addEventListener('click', handleProcessInsertedResponse);
        }
        if (elements.cancelInsertResponseButton) {
            elements.cancelInsertResponseButton.addEventListener('click', handleCancelInsertResponse);
        }
    }

    if (elements.startOverButton) {
        elements.startOverButton.addEventListener('click', handleStartOver);
    }

    if (elements.selectAllAvailableButton) {
        elements.selectAllAvailableButton.addEventListener('click', handleSelectAllAvailable);
    }

    if (elements.addToChosenButton) {
        elements.addToChosenButton.addEventListener('click', handleAddFiles);
    }

    if (elements.removeFromChosenButton) {
        elements.removeFromChosenButton.addEventListener('click', handleRemoveFiles);
    }

    if (elements.clearChosenButton) {
        elements.clearChosenButton.addEventListener('click', handleClearFiles);
    }

    if (elements.refreshFilesButton) {
        elements.refreshFilesButton.addEventListener('click', fetchLibraryFiles);
    }

    if (elements.minSilenceInput) {
        elements.minSilenceInput.addEventListener('input', handleSilencePreferenceChange);
    }
    if (elements.maxSilenceInput) {
        elements.maxSilenceInput.addEventListener('input', handleSilencePreferenceChange);
    }
    if (elements.silenceWordThresholdInput) {
        elements.silenceWordThresholdInput.addEventListener('input', handleSilencePreferenceChange);
    }

    if (elements.playAllButton) {
        elements.playAllButton.addEventListener('click', playAllVideos);
    }

    if (elements.addAllMatchesButton) {
        elements.addAllMatchesButton.addEventListener('click', addAllMatchesToTimeline);
    }
    
    // Clear results button
    const clearResultsButton = document.getElementById('clearResultsButton');
    if (clearResultsButton) {
        clearResultsButton.addEventListener('click', handleStartOver);
    }

    if (elements.timelinePlayButton) {
        elements.timelinePlayButton.addEventListener('click', playTimeline);
    }

    if (elements.timelineMergeButton) {
        elements.timelineMergeButton.addEventListener('click', mergeTimeline);
    }

    if (elements.timelineClearButton) {
        elements.timelineClearButton.addEventListener('click', clearTimeline);
    }

    if (elements.timelineList) {
        elements.timelineList.addEventListener('click', handleTimelineListClick);
        elements.timelineList.addEventListener('dragenter', handleTimelineDragEnter);
        elements.timelineList.addEventListener('dragover', handleTimelineDragOver);
        elements.timelineList.addEventListener('dragleave', handleTimelineDragLeave);
        elements.timelineList.addEventListener('drop', handleTimelineDrop);
        elements.timelineList.addEventListener('scroll', handleTimelineScrollDuringDrag);
    }
}

function initializeUI() {
    updateProjectControlsState();
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }
    resetCorpusState();
    setAutosaveStatus('Loading projects…', 'saving');
}

function updateProjectControlsState() {
    const hasProject = Boolean(activeProject);
    const toggledControls = [
        'projectNameInput',
        'duplicateProjectButton',
        'deleteProjectButton',
        'saveSentenceButton',
        'addToChosenButton',
        'removeFromChosenButton',
        'clearChosenButton',
        'searchPhrasesButton',
        'searchSentenceButton',
        'startOverButton',
        'playAllButton',
        'addAllMatchesButton',
        'timelinePlayButton',
        'timelineMergeButton',
        'timelineClearButton',
        'phraseInput',
        'sentenceInput'
    ];

    toggledControls.forEach(key => {
        if (elements[key]) {
            elements[key].disabled = !hasProject;
        }
    });

    if (elements.availableFileSelector) {
        elements.availableFileSelector.disabled = !hasProject;
    }
    if (elements.chosenFileSelector) {
        elements.chosenFileSelector.disabled = !hasProject;
    }
    if (elements.timelineList) {
        if (hasProject) {
            delete elements.timelineList.dataset.disabled;
        } else {
            elements.timelineList.dataset.disabled = 'true';
        }
    }
}

async function fetchLibraryFiles() {
    try {
        const response = await fetch('/get_files');
        if (!response.ok) {
            throw new Error(`Failed to fetch files: ${response.status}`);
        }
        const data = await response.json();
        libraryFiles = Array.isArray(data) ? data : [];
        renderFilePickers();
    } catch (error) {
        console.error('Error fetching files:', error);
    }
}

async function bootstrapProjects() {
    try {
        const response = await fetch('/api/projects');
        if (!response.ok) {
            throw new Error(`Failed to list projects: ${response.status}`);
        }
        const data = await response.json();
        projectList = Array.isArray(data) ? data.map(item => buildProjectSummary(item)) : [];
        renderProjectPicker();

        if (projectList.length) {
            // Try to restore the last selected project from localStorage
            const savedProjectId = localStorage.getItem('lastSelectedProjectId');
            let projectToLoad = null;
            
            if (savedProjectId) {
                // Check if the saved project still exists
                projectToLoad = projectList.find(p => p.id === savedProjectId);
            }
            
            // If saved project not found or doesn't exist, use first project
            if (!projectToLoad) {
                projectToLoad = projectList[0];
            }
            
            await loadProject(projectToLoad.id);
        } else {
            const created = await createProjectOnServer('Untitled Project');
            if (created) {
                projectList.push(buildProjectSummary(created));
                renderProjectPicker();
                await loadProject(created.id);
            }
        }
    } catch (error) {
        console.error('Failed to bootstrap projects:', error);
        setAutosaveStatus('Failed to load projects', 'error');
    }
}

async function loadProject(projectId) {
    if (!projectId) {
        return;
    }
    try {
        setAutosaveStatus('Loading project…', 'saving');
        const response = await fetch(`/api/projects/${projectId}`);
        if (!response.ok) {
            throw new Error(`Failed to load project: ${response.status}`);
        }
        const project = await response.json();
        activeProject = project;
        
        // Save the selected project ID to localStorage for restoration on F5
        localStorage.setItem('lastSelectedProjectId', projectId);
        
        updateProjectListEntry(project);
        renderProjectPicker();
        applyProjectToUI(project);
        // Autoload corpus after project is loaded
        handleLoadSentences();
    } catch (error) {
        console.error('Failed to load project', error);
        setAutosaveStatus('Failed to load project', 'error');
    }
}

async function createProjectOnServer(name, data) {
    const payload = { name };
    if (data) {
        payload.data = data;
    }
    const response = await fetch('/api/projects', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
    });
    if (!response.ok) {
        throw new Error(`Failed to create project: ${response.status}`);
    }
    return response.json();
}

async function handleCreateProject() {
    try {
        const created = await createProjectOnServer('Untitled Project');
        if (!created) {
            return;
        }
        projectList.push(buildProjectSummary(created));
        renderProjectPicker();
        await loadProject(created.id);
    } catch (error) {
        console.error('Failed to create project', error);
        setAutosaveStatus('Failed to create project', 'error');
    }
}

async function handleDuplicateProject() {
    if (!activeProject) {
        return;
    }
    try {
        const baseName = activeProject.name || 'Untitled Project';
        const existingNames = new Set(projectList.map(project => project.name));
        let candidate = `${baseName} Copy`;
        let counter = 2;
        while (existingNames.has(candidate)) {
            candidate = `${baseName} Copy ${counter}`;
            counter += 1;
        }
        const dataClone = JSON.parse(JSON.stringify(activeProject.data || {}));
        const created = await createProjectOnServer(candidate, dataClone);
        projectList.push(buildProjectSummary(created));
        renderProjectPicker();
        await loadProject(created.id);
    } catch (error) {
        console.error('Failed to duplicate project', error);
        setAutosaveStatus('Failed to duplicate project', 'error');
    }
}

async function handleDeleteProject() {
    if (!activeProject) {
        return;
    }
    const confirmed = window.confirm(`Delete project "${activeProject.name || 'Untitled Project'}"? This cannot be undone.`);
    if (!confirmed) {
        return;
    }

    try {
        const response = await fetch(`/api/projects/${activeProject.id}`, { method: 'DELETE' });
        if (!response.ok) {
            throw new Error(`Failed to delete project: ${response.status}`);
        }

        projectList = projectList.filter(project => project.id !== activeProject.id);
        activeProject = null;
        renderProjectPicker();
        clearWorkspaceInputs();
        setAutosaveStatus('Project deleted. Creating a new project…', 'dirty');

        if (projectList.length) {
            await loadProject(projectList[0].id);
        } else {
            await handleCreateProject();
        }
    } catch (error) {
        console.error('Failed to delete project', error);
        setAutosaveStatus('Failed to delete project', 'error');
    }
}

function applyProjectToUI(project) {
    if (!project) {
        return;
    }

    isApplyingProject = true;

    const data = project.data || {};

    if (elements.projectSelect) {
        elements.projectSelect.value = project.id;
    }

    if (elements.projectNameInput) {
        elements.projectNameInput.value = project.name || '';
    }

    if (elements.phraseInput) {
        elements.phraseInput.value = data.phraseInput || '';
    }

    if (elements.sentenceInput) {
        elements.sentenceInput.value = data.currentSentence || '';
    }

    if (elements.minSilenceInput) {
        const min = (data.silencePreferences && typeof data.silencePreferences.minSilence === 'number') ? data.silencePreferences.minSilence : 0;
        elements.minSilenceInput.value = min;
    }
    if (elements.maxSilenceInput) {
        const max = (data.silencePreferences && typeof data.silencePreferences.maxSilence === 'number') ? data.silencePreferences.maxSilence : 10;
        elements.maxSilenceInput.value = max;
    }
    if (elements.silenceWordThresholdInput) {
        const threshold = (data.silencePreferences && typeof data.silencePreferences.silenceWordThreshold === 'number') ? data.silencePreferences.silenceWordThreshold : 2;
        elements.silenceWordThresholdInput.value = threshold;
    }
    
    // Restore includePartialMatches checkbox (default false)
    const includePartialMatchesCheckbox = document.getElementById('includePartialMatches');
    if (includePartialMatchesCheckbox) {
        includePartialMatchesCheckbox.checked = data.includePartialMatches === true;
    }
    
    // Restore allPartialMatches checkbox (default false)
    const allPartialMatchesCheckbox = document.getElementById('allPartialMatches');
    if (allPartialMatchesCheckbox) {
        allPartialMatchesCheckbox.checked = data.allPartialMatches === true;
    }
    
    // Restore maxResultsPerSegment input (default 25)
    const maxResultsInput = document.getElementById('maxResultsPerSegment');
    if (maxResultsInput) {
        maxResultsInput.value = (typeof data.maxResultsPerSegment === 'number') ? data.maxResultsPerSegment : 25;
    }

    // Restore story prompt and settings
    const storyPromptInput = document.getElementById('storyPromptInput');
    if (storyPromptInput && data.storyPrompt) {
        storyPromptInput.value = data.storyPrompt;
    }
    
    const maxStorySegmentsInput = document.getElementById('maxStorySegments');
    if (maxStorySegmentsInput) {
        maxStorySegmentsInput.value = (typeof data.maxStorySegments === 'number') ? data.maxStorySegments : 10;
    }
    
    const preferLongSegmentsCheckbox = document.getElementById('preferLongSegments');
    if (preferLongSegmentsCheckbox) {
        preferLongSegmentsCheckbox.checked = data.preferLongSegments !== false; // Default true
    }
    
    const debugModeCheckbox = document.getElementById('debugMode');
    if (debugModeCheckbox) {
        debugModeCheckbox.checked = data.debugMode === true; // Default false
    }

    if (!Array.isArray(data.timeline)) {
        data.timeline = [];
    }
    data.timeline.forEach(entry => {
        if (!entry || typeof entry !== 'object') {
            return;
        }
        if (!entry.id) {
            entry.id = generateId('timeline');
        }
        entry.startTrim = Number.isFinite(entry.startTrim) ? entry.startTrim : parseInt(entry.startTrim, 10) || 0;
        entry.endTrim = Number.isFinite(entry.endTrim) ? entry.endTrim : parseInt(entry.endTrim, 10) || 0;
    });

    renderProjectSentences(Array.isArray(data.sentences) ? data.sentences : []);
    renderFilePickers();
    resetCorpusState();
    renderTimeline(data.timeline);
    updateProjectControlsState();
    setAutosaveStatus('All changes saved', 'ready');

    isApplyingProject = false;
}

function clearWorkspaceInputs() {
    if (elements.projectNameInput) {
        elements.projectNameInput.value = '';
    }
    if (elements.phraseInput) {
        elements.phraseInput.value = '';
    }
    if (elements.sentenceInput) {
        elements.sentenceInput.value = '';
    }
    if (elements.projectSentenceList) {
        elements.projectSentenceList.innerHTML = '';
    }
    if (elements.availableFileSelector) {
        elements.availableFileSelector.innerHTML = '';
    }
    if (elements.chosenFileSelector) {
        elements.chosenFileSelector.innerHTML = '';
    }
    if (elements.timelineList) {
        elements.timelineList.innerHTML = '';
    }
    updateProjectControlsState();
}

function renderProjectPicker() {
    if (!elements.projectSelect) {
        return;
    }
    const currentValue = activeProject ? activeProject.id : elements.projectSelect.value;
    elements.projectSelect.innerHTML = '';

    projectList.forEach(project => {
        const option = new Option(project.name || 'Untitled Project', project.id);
        elements.projectSelect.appendChild(option);
    });

    if (currentValue) {
        elements.projectSelect.value = currentValue;
    }
}

function buildProjectSummary(project) {
    if (!project) {
        return null;
    }
    return {
        id: project.id,
        name: project.name || 'Untitled Project',
        createdAt: project.createdAt,
        updatedAt: project.updatedAt
    };
}

function updateProjectListEntry(project) {
    const summary = buildProjectSummary(project);
    if (!summary) {
        return;
    }
    const index = projectList.findIndex(item => item.id === summary.id);
    if (index === -1) {
        projectList.push(summary);
    } else {
        projectList[index] = { ...projectList[index], ...summary };
    }
}

function refreshActiveProjectOption() {
    if (!elements.projectSelect || !activeProject) {
        return;
    }
    const option = Array.from(elements.projectSelect.options).find(opt => opt.value === activeProject.id);
    if (option) {
        option.textContent = activeProject.name || 'Untitled Project';
    }
}

function setAutosaveStatus(message, state) {
    if (!elements.autosaveStatus) {
        return;
    }
    elements.autosaveStatus.classList.remove('ready', 'saving', 'dirty', 'error');
    if (state) {
        elements.autosaveStatus.classList.add(state);
    }
    elements.autosaveStatus.textContent = message;
}

function scheduleAutosave(immediate = false) {
    if (!activeProject || isApplyingProject) {
        return;
    }

    if (autosaveTimer) {
        clearTimeout(autosaveTimer);
        autosaveTimer = null;
    }

    setAutosaveStatus('Unsaved changes', 'dirty');

    const delay = immediate ? 0 : AUTOSAVE_DELAY;
    autosaveTimer = setTimeout(runAutosave, delay);
}

async function runAutosave() {
    if (!activeProject || isApplyingProject || isAutosaving) {
        return;
    }

    autosaveTimer = null;
    isAutosaving = true;
    setAutosaveStatus('Saving…', 'saving');

    try {
        const payload = {
            name: normalizeProjectName(activeProject.name),
            data: activeProject.data || {}
        };
        const response = await fetch(`/api/projects/${activeProject.id}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`Autosave failed: ${response.status}`);
        }

        const updated = await response.json();
        activeProject = updated;
        updateProjectListEntry(updated);
        renderProjectPicker();
        if (elements.projectSelect) {
            elements.projectSelect.value = updated.id;
        }
        setAutosaveStatus('All changes saved', 'ready');
    } catch (error) {
        console.error('Autosave failed', error);
        setAutosaveStatus('Autosave failed', 'error');
    } finally {
        isAutosaving = false;
    }
}

function updateProjectData(mutator, options = {}) {
    if (!activeProject || typeof mutator !== 'function') {
        return;
    }
    const data = activeProject.data || {};
    activeProject.data = data;
    mutator(data);

    if (isApplyingProject || options.skipAutosave) {
        return;
    }
    scheduleAutosave(Boolean(options.immediate));
}

function normalizeProjectName(name) {
    if (typeof name !== 'string') {
        return 'Untitled Project';
    }
    const trimmed = name.trim();
    return trimmed || 'Untitled Project';
}

function handleProjectNameInput() {
    if (!activeProject || isApplyingProject) {
        return;
    }
    activeProject.name = elements.projectNameInput.value;
    updateProjectListEntry(activeProject);
    refreshActiveProjectOption();
    scheduleAutosave();
}

function handleProjectNameBlur() {
    if (!activeProject) {
        return;
    }
    const normalized = normalizeProjectName(elements.projectNameInput.value);
    if (normalized !== activeProject.name) {
        activeProject.name = normalized;
        elements.projectNameInput.value = normalized;
        updateProjectListEntry(activeProject);
        renderProjectPicker();
        if (elements.projectSelect) {
            elements.projectSelect.value = activeProject.id;
        }
        scheduleAutosave(true);
    }
}

function renderProjectSentences(entries) {
    if (!elements.projectSentenceList) {
        return;
    }
    elements.projectSentenceList.innerHTML = '';

    if (!entries || !entries.length) {
        return;
    }

    const fragment = document.createDocumentFragment();

    entries.forEach((sentence, index) => {
        const item = document.createElement('li');
        const text = document.createElement('span');
        text.className = 'item-text';
        text.textContent = sentence;

        const actions = document.createElement('div');
        actions.className = 'item-actions';

        const loadButton = document.createElement('button');
        loadButton.type = 'button';
        loadButton.className = 'ghost-button';
        loadButton.dataset.action = 'load';
        loadButton.dataset.index = String(index);
        loadButton.textContent = 'Load';

        const deleteButton = document.createElement('button');
        deleteButton.type = 'button';
        deleteButton.className = 'ghost-button danger';
        deleteButton.dataset.action = 'delete';
        deleteButton.dataset.index = String(index);
        deleteButton.textContent = 'Remove';

        actions.appendChild(loadButton);
        actions.appendChild(deleteButton);
        item.appendChild(text);
        item.appendChild(actions);
        fragment.appendChild(item);
    });

    elements.projectSentenceList.appendChild(fragment);
}

function handleProjectSentenceListClick(event) {
    const target = event.target;
    if (!target || target.tagName !== 'BUTTON') {
        return;
    }
    const action = target.dataset.action;
    const index = parseInt(target.dataset.index || '-1', 10);
    if (Number.isNaN(index) || index < 0 || !activeProject) {
        return;
    }

    const entries = Array.isArray(activeProject.data?.sentences) ? activeProject.data.sentences : [];

    if (action === 'load') {
        const sentence = entries[index];
        if (sentence !== undefined && elements.sentenceInput) {
            elements.sentenceInput.value = sentence;
            updateProjectData(data => {
                data.currentSentence = sentence;
            });
            elements.sentenceInput.focus();
        }
    }

    if (action === 'delete') {
        entries.splice(index, 1);
        updateProjectData(data => {
            data.sentences = entries;
        });
        renderProjectSentences(entries);
    }
}

function handleSaveSentence() {
    if (!activeProject || !elements.sentenceInput) {
        return;
    }
    const rawSentence = elements.sentenceInput.value.trim();
    if (!rawSentence) {
        return;
    }

    // Split into semicolon-separated groups and keep non-empty trimmed ones
    const sentenceGroups = rawSentence
        .split(';')
        .map(part => part.trim())
        .filter(part => part.length > 0);

    if (sentenceGroups.length === 0) {
        return;
    }

    // Expose current groups globally so progress UI can show group context
    window.currentSentenceGroups = sentenceGroups;

    const entries = Array.isArray(activeProject.data?.sentences) ? [...activeProject.data.sentences] : [];
    // Add all sentence groups to saved sentences if not already present
    let updated = false;
    
    for (const group of sentenceGroups) {
        if (!entries.includes(group)) {
            entries.unshift(group);
            updated = true;
        }
    }
    
    if (updated) {
        // Trim to max allowed entries, keeping the most recent ones
        if (entries.length > MAX_SAVED_SENTENCES) {
            entries.length = MAX_SAVED_SENTENCES;
        }
        
        updateProjectData(data => {
            data.sentences = entries;
        });
        renderProjectSentences(entries);
    }
}

function renderFilePickers() {
    if (!elements.availableFileSelector || !elements.chosenFileSelector) {
        return;
    }

    const chosenFiles = Array.isArray(activeProject?.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    const chosenSet = new Set(chosenFiles);
    const availableFragment = document.createDocumentFragment();
    const chosenFragment = document.createDocumentFragment();

    chosenFiles.forEach(file => {
        const option = new Option(file, file);
        if (!libraryFiles.includes(file)) {
            option.textContent = `${file} (missing)`;
            option.dataset.missing = 'true';
        }
        chosenFragment.appendChild(option);
    });

    if (libraryFiles.length) {
        libraryFiles.forEach(file => {
            if (!chosenSet.has(file)) {
                availableFragment.appendChild(new Option(file, file));
            }
        });
    }

    elements.availableFileSelector.innerHTML = '';
    elements.availableFileSelector.appendChild(availableFragment);
    elements.chosenFileSelector.innerHTML = '';
    elements.chosenFileSelector.appendChild(chosenFragment);
}

function handleAddFiles() {
    if (!activeProject) {
        return;
    }
    const selected = getSelectedValues(elements.availableFileSelector);
    if (!selected.length) {
        return;
    }
    updateProjectData(data => {
        const current = Array.isArray(data.selectedFiles) ? data.selectedFiles : [];
        selected.forEach(file => {
            if (!current.includes(file)) {
                current.push(file);
            }
        });
        data.selectedFiles = current;
    });
    renderFilePickers();
    resetCorpusState();
    // Automatically reload corpus with new file list
    handleLoadSentences();
}

function handleRemoveFiles() {
    if (!activeProject) {
        return;
    }
    const selected = getSelectedValues(elements.chosenFileSelector);
    if (!selected.length) {
        return;
    }
    updateProjectData(data => {
        const current = Array.isArray(data.selectedFiles) ? data.selectedFiles : [];
        const removeSet = new Set(selected);
        data.selectedFiles = current.filter(file => !removeSet.has(file));
    });
    renderFilePickers();
    resetCorpusState();
    // Automatically reload corpus with updated file list
    handleLoadSentences();
}

function handleClearFiles() {
    if (!activeProject) {
        return;
    }
    updateProjectData(data => {
        data.selectedFiles = [];
    });
    renderFilePickers();
    resetCorpusState();
    // Automatically reload corpus (will be empty after clearing files)
    handleLoadSentences();
}

function handleSelectAllAvailable() {
    if (!elements.availableFileSelector) {
        return;
    }
    Array.from(elements.availableFileSelector.options).forEach(option => {
        option.selected = true;
    });
}

function getSelectedValues(selectElement) {
    if (!selectElement) {
        return [];
    }
    return Array.from(selectElement.selectedOptions || []).map(option => option.value);
}

function getSilencePreferences() {
    let minSilence = elements.minSilenceInput ? parseFloat(elements.minSilenceInput.value) : 0;
    if (!Number.isFinite(minSilence) || minSilence < 0) {
        minSilence = 0;
    }

    let maxSilence = elements.maxSilenceInput ? parseFloat(elements.maxSilenceInput.value) : 10;
    if (!Number.isFinite(maxSilence) || maxSilence < minSilence) {
        maxSilence = minSilence;
    }

    let threshold = elements.silenceWordThresholdInput ? parseInt(elements.silenceWordThresholdInput.value, 10) : 2;
    if (!Number.isInteger(threshold) || threshold < 1) {
        threshold = 1;
    }

    return {
        minSilence,
        maxSilence,
        silenceWordThreshold: threshold
    };
}

function handleSilencePreferenceChange() {
    if (!activeProject) {
        return;
    }
    const preferences = getSilencePreferences();
    updateProjectData(data => {
        data.silencePreferences = preferences;
    });
}

function showSearchLoading(message, onCancel) {
    if (!elements.resultsContainer) {
        return;
    }
    
    // Create loading indicator
    const loadingDiv = document.createElement('div');
    loadingDiv.id = 'searchLoadingIndicator';
    loadingDiv.className = 'search-loading';
    loadingDiv.innerHTML = `
        <div class="loading-spinner"></div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <p style="margin: 0; flex: 1;">${message}</p>
        </div>
    `;
    
    // Add cancel button if callback provided
    if (onCancel) {
        const messageContainer = loadingDiv.querySelector('div:last-child');
        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'button ghost-button';
        cancelBtn.textContent = 'Cancel Search';
        cancelBtn.style.cssText = 'padding: 4px 12px; font-size: 12px; min-width: auto;';
        cancelBtn.onclick = onCancel;
        messageContainer.appendChild(cancelBtn);
    }
    
    elements.resultsContainer.appendChild(loadingDiv);
}

function hideSearchLoading() {
    if (!elements.resultsContainer) {
        return;
    }
    
    const loadingDiv = document.getElementById('searchLoadingIndicator');
    if (loadingDiv) {
        loadingDiv.remove();
    }
    
    // Auto-jump to results on mobile after search completes
    if (isMobileLayout()) {
        // Check if there are any results
        const hasResults = elements.resultsContainer && elements.resultsContainer.children.length > 0;
        if (hasResults) {
            // Small delay to ensure results are rendered
            setTimeout(() => {
                switchMobileSection('results');
            }, 300);
        }
    }
}

function showMergeProgress(message, progress, total) {
    // Remove existing progress indicator if any
    const existing = document.getElementById('mergeProgressIndicator');
    if (existing) {
        existing.remove();
    }
    
    // Create progress indicator
    const progressDiv = document.createElement('div');
    progressDiv.id = 'mergeProgressIndicator';
    progressDiv.className = 'search-loading';
    progressDiv.style.cssText = 'position: fixed; top: 20px; right: 20px; z-index: 10000; background: #1a1a1a !important; padding: 12px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.7); width: 240px; border: 1px solid rgba(255,255,255,0.2); opacity: 1 !important;';
    
    const percentage = total > 0 ? Math.round((progress / total) * 100) : 0;
    progressDiv.innerHTML = `
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
            <div class="loading-spinner" style="width: 20px; height: 20px; flex-shrink: 0;"></div>
            <div style="flex: 1;">
                <p style="margin: 0; font-weight: 500; font-size: 14px;">Merging Timeline</p>
                <p style="margin: 2px 0 0 0; font-size: 12px; color: var(--text-secondary);">${progress} / ${total}</p>
            </div>
        </div>
        <div style="background: var(--bg-secondary); border-radius: 4px; height: 6px; overflow: hidden;">
            <div style="background: var(--accent); height: 100%; width: ${percentage}%; transition: width 0.3s ease;"></div>
        </div>
    `;
    
    document.body.appendChild(progressDiv);
}

function updateMergeProgress(message, progress, total) {
    const progressDiv = document.getElementById('mergeProgressIndicator');
    if (!progressDiv) {
        showMergeProgress(message, progress, total);
        return;
    }
    
    const percentage = total > 0 ? Math.round((progress / total) * 100) : 0;
    const countP = progressDiv.querySelector('p:first-of-type + p');
    const progressBar = progressDiv.querySelector('div[style*="background: var(--accent)"]');
    
    if (countP) {
        countP.textContent = `${progress} / ${total}`;
    }
    if (progressBar) {
        progressBar.style.width = `${percentage}%`;
    }
}

function hideMergeProgress() {
    const progressDiv = document.getElementById('mergeProgressIndicator');
    if (progressDiv) {
        progressDiv.remove();
    }
}

function updateSearchLoadingMessage(message, segmentData) {
    const loadingDiv = document.getElementById('searchLoadingIndicator');
    if (!loadingDiv) {
        console.warn('[updateSearchLoadingMessage] Loading div not found');
        return;
    }
    
    const messageContainer = loadingDiv.querySelector('div:last-child');
    if (!messageContainer) {
        console.warn('[updateSearchLoadingMessage] Message container not found');
        return;
    }
    
    const messageEl = messageContainer.querySelector('p');
    if (messageEl) {
        // Start from the base message
        let displayMessage = message;

        // If we have clip progress data, adjust the matches part
        if (segmentData && segmentData.clip_index && segmentData.total_clips) {
            displayMessage = displayMessage.replace(
                /\((\d+) matches\)/,
                `(${segmentData.clip_index}/${segmentData.total_clips} matches)`
            );
        }

        // If we have multi-group progress info, prefix with group context
        if (segmentData && typeof segmentData.group_index === 'number' && typeof segmentData.total_groups === 'number') {
            const groupIndex = segmentData.group_index;
            const totalGroups = segmentData.total_groups;

            // Strip any existing group prefix to avoid accumulating prefixes
            displayMessage = displayMessage.replace(/^Group \d+\/\d+[^:]*:\s*/, '');

            let prefix = `Group ${groupIndex}/${totalGroups}`;

            const groups = window.currentSentenceGroups;
            if (Array.isArray(groups) && groups[groupIndex - 1]) {
                prefix += ` – "${groups[groupIndex - 1]}"`;
            }

            displayMessage = `${prefix}: ${displayMessage}`;
        }

        console.log(`[updateSearchLoadingMessage] Updating message: "${displayMessage}"`);
        messageEl.textContent = displayMessage;
    } else {
        console.warn('[updateSearchLoadingMessage] Message element not found');
    }
    
    // Remove existing skip button if any
    const existingBtn = messageContainer.querySelector('.skip-segment-btn');
    if (existingBtn) {
        existingBtn.remove();
    }
    
    // Add skip button if we have segment data
    if (segmentData && segmentData.segment_index) {
        const skipBtn = document.createElement('button');
        skipBtn.className = 'button ghost-button skip-segment-btn';
        skipBtn.textContent = 'Skip Segment';
        skipBtn.title = 'Skip this segment immediately (no clips will be rendered)';
        skipBtn.style.cssText = 'padding: 4px 12px; font-size: 12px; min-width: auto;';
        skipBtn.onclick = () => {
            // Extract segment text from progress message (between quotes)
            const match = message.match(/'([^']+)'/);
            if (!match || !window.currentSearchId) {
                console.error('[Client] Cannot skip: missing segment text or search ID');
                return;
            }
            
            const segmentPhrase = match[1];
            skipBtn.textContent = 'Skipping...';
            skipBtn.disabled = true;
            
            // Send skip request to server
            fetch('/skip_segment', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    search_id: window.currentSearchId,
                    segment_phrase: segmentPhrase
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'ok') {
                    console.log(`[Client] ⏭️  SKIP sent to server: '${segmentPhrase.substring(0, 50)}...'`);
                    skipBtn.textContent = 'Skipped';
                    // Update message to indicate skip is in progress
                    const messageEl = messageContainer.querySelector('p');
                    if (messageEl) {
                        messageEl.textContent = message + ' → skip requested (will skip processing...)';
                    }
                } else {
                    console.error('[Client] Skip failed:', data);
                    skipBtn.textContent = 'Skip Failed';
                }
            })
            .catch(error => {
                console.error('[Client] Error sending skip request:', error);
                skipBtn.textContent = 'Skip Failed';
            });
        };
        messageContainer.appendChild(skipBtn);
    }
}

function showNoResults(message) {
    if (!elements.resultsContainer) {
        return;
    }
    
    // Remove any existing no results message
    const existingMessage = document.getElementById('noResultsMessage');
    if (existingMessage) {
        existingMessage.remove();
    }
    
    // Create no results message
    const messageDiv = document.createElement('div');
    messageDiv.id = 'noResultsMessage';
    messageDiv.className = 'no-results-message';
    messageDiv.innerHTML = `
        <p>${message}</p>
    `;
    
    elements.resultsContainer.appendChild(messageDiv);
}

// Helper function to get video duration
function getVideoDuration(videoPath) {
    return new Promise((resolve, reject) => {
        const video = document.createElement('video');
        video.preload = 'metadata';
        video.onloadedmetadata = () => {
            resolve(video.duration * 1000); // Return duration in milliseconds
            video.remove();
        };
        video.onerror = () => {
            reject(new Error('Failed to load video metadata'));
            video.remove();
        };
        video.src = getVideoUrl(videoPath);
    });
}

// Cache for waveform data
const waveformCache = new Map();

// Helper function to fetch real audio waveform data
function fetchWaveformData(videoPath, width = 800) {
    return new Promise((resolve, reject) => {
        // Check cache first
        const cacheKey = `${videoPath}_${width}`;
        if (waveformCache.has(cacheKey)) {
            resolve(waveformCache.get(cacheKey));
            return;
        }
        
        // Fetch from server
        fetch('/get_waveform', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                file_path: videoPath,
                width: width
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Waveform error:', data.error);
                reject(new Error(data.error));
            } else {
                // Cache the result
                waveformCache.set(cacheKey, data.waveform);
                resolve(data.waveform);
            }
        })
        .catch(error => {
            console.error('Failed to fetch waveform:', error);
            reject(error);
        });
    });
}

// Waveform visualization component
function createWaveformVisualization(container, initialStartTrim = 450, initialEndTrim = 450, maxTrim = 8000, onTrimChange = null, videoPath = null, initialSpeed = 1.0, onTrimRelease = null) {
    const waveformContainer = document.createElement('div');
    waveformContainer.className = 'waveform-container';
    
    let clipDuration = null; // Will be set when video metadata loads
    let realWaveformData = null; // Will be set when waveform loads
    let speed = initialSpeed; // Playback speed (0.5 to 1.0)
    
    // Load video duration and waveform if path provided
    if (videoPath) {
        getVideoDuration(videoPath).then(duration => {
            clipDuration = duration;
            // Update handle positions now that we know the clip duration
            updateSelection();
        }).catch(err => {
            console.warn('Could not load video duration:', err);
        });
        
        // Fetch real waveform data
        fetchWaveformData(videoPath, 800).then(data => {
            realWaveformData = data;
            // Redraw with real data
            regenerateWaveform();
        }).catch(err => {
            console.warn('Could not load waveform data, using synthetic:', err);
        });
    }
    
    const canvas = document.createElement('canvas');
    canvas.className = 'waveform-canvas';
    // Set initial size, will be resized to container
    canvas.width = 800;
    canvas.height = 120;
    
    // Make canvas responsive
    function resizeCanvas() {
        const rect = waveformContainer.getBoundingClientRect();
        if (rect.width > 0) {
            canvas.width = rect.width;
            canvas.height = rect.height;
            drawWaveform();
        }
    }
    
    const selectionOverlay = document.createElement('div');
    selectionOverlay.className = 'waveform-selection';
    
    const startHandle = document.createElement('div');
    startHandle.className = 'waveform-handle waveform-handle--start';
    startHandle.setAttribute('role', 'slider');
    startHandle.setAttribute('aria-label', 'Start trim');
    startHandle.setAttribute('tabindex', '0');
    
    const endHandle = document.createElement('div');
    endHandle.className = 'waveform-handle waveform-handle--end';
    endHandle.setAttribute('role', 'slider');
    endHandle.setAttribute('aria-label', 'End trim');
    endHandle.setAttribute('tabindex', '0');
    
    // Create MS indicators
    const startLabel = document.createElement('div');
    startLabel.className = 'waveform-label waveform-label--start';
    startLabel.textContent = `${initialStartTrim}ms`;
    
    const endLabel = document.createElement('div');
    endLabel.className = 'waveform-label waveform-label--end';
    endLabel.textContent = `${initialEndTrim}ms`;
    
    // Create duration info label
    const durationInfo = document.createElement('div');
    durationInfo.className = 'waveform-duration-info';
    durationInfo.textContent = 'Loading...';
    
    waveformContainer.appendChild(canvas);
    waveformContainer.appendChild(selectionOverlay);
    waveformContainer.appendChild(startHandle);
    waveformContainer.appendChild(endHandle);
    waveformContainer.appendChild(startLabel);
    waveformContainer.appendChild(endLabel);
    waveformContainer.appendChild(durationInfo);
    
    // Generate waveform data (synthetic initially, real when loaded)
    let waveformData = generateWaveformData(canvas.width);
    
    // Draw waveform
    const ctx = canvas.getContext('2d');
    let startTrim = initialStartTrim;
    let endTrim = initialEndTrim;
    
    function drawWaveform() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#6b5ce6';
        ctx.strokeStyle = '#6b5ce6';
        
        const centerY = canvas.height / 2;
        const scale = canvas.height * 0.4;
        
        // Use real waveform data if available, otherwise regenerate synthetic
        if (realWaveformData && realWaveformData.length > 0) {
            // Scale real waveform data to canvas width
            waveformData = scaleWaveformToWidth(realWaveformData, canvas.width);
        } else if (waveformData.length !== canvas.width) {
            waveformData = generateWaveformData(canvas.width);
        }
        
        for (let i = 0; i < waveformData.length; i++) {
            const x = (i / waveformData.length) * canvas.width;
            const amplitude = waveformData[i] * scale;
            
            // Make line thickness proportional to amplitude for better silence visualization
            // Minimum 0.5px for very quiet parts, up to 1.5px for loud parts
            ctx.lineWidth = 0.5 + (waveformData[i] * 1.0);
            
            ctx.beginPath();
            ctx.moveTo(x, centerY - amplitude);
            ctx.lineTo(x, centerY + amplitude);
            ctx.stroke();
        }
    }
    
    function regenerateWaveform() {
        if (realWaveformData) {
            // If we have real data, just redraw it
            drawWaveform();
        } else {
            // Otherwise generate new synthetic data
            waveformData = generateWaveformData(canvas.width);
            drawWaveform();
        }
    }
    
    function loadNewWaveform(newVideoPath) {
        realWaveformData = null; // Reset
        fetchWaveformData(newVideoPath, 800).then(data => {
            realWaveformData = data;
            regenerateWaveform();
        }).catch(err => {
            console.warn('Could not load new waveform data:', err);
            realWaveformData = null;
            regenerateWaveform(); // Fall back to synthetic
        });
    }
    
    // Make canvas responsive
    function resizeCanvas() {
        const rect = waveformContainer.getBoundingClientRect();
        if (rect.width > 0) {
            canvas.width = rect.width;
            canvas.height = rect.height;
            drawWaveform();
        }
    }
    
    // Resize on container resize
    const resizeObserver = new ResizeObserver(resizeCanvas);
    resizeObserver.observe(waveformContainer);
    
    function updateSelection() {
        // Use actual clip duration if available, otherwise use maxTrim
        const effectiveMax = clipDuration || maxTrim;
        
        // Start trim is from the left (beginning)
        const startPercent = (startTrim / effectiveMax) * 100;
        // End trim is from the right (end), so we need to invert it
        const endPercent = 100 - (endTrim / effectiveMax) * 100;
        
        // Ensure minimum visual separation if values are too close or identical
        const minSeparation = 1.0; // 1% minimum separation for visibility
        let adjustedStartPercent = startPercent;
        let adjustedEndPercent = endPercent;
        
        if (Math.abs(endPercent - startPercent) < minSeparation) {
            // If they're too close or identical, separate them visually
            const center = (startPercent + endPercent) / 2;
            adjustedStartPercent = Math.max(0, center - minSeparation / 2);
            adjustedEndPercent = Math.min(100, center + minSeparation / 2);
        }
        
        selectionOverlay.style.left = `${adjustedStartPercent}%`;
        selectionOverlay.style.width = `${Math.max(minSeparation, adjustedEndPercent - adjustedStartPercent)}%`;
        
        // Always position handles at their actual values
        startHandle.style.left = `${startPercent}%`;
        endHandle.style.left = `${endPercent}%`;
        
        // Update labels - show effective times based on speed
        // At 50% speed, 1000ms of original content plays for 2000ms
        const effectiveStartTrim = Math.round(startTrim / speed);
        const effectiveEndTrim = Math.round(endTrim / speed);
        startLabel.textContent = speed !== 1.0 ? `${effectiveStartTrim}ms` : `${startTrim}ms`;
        endLabel.textContent = speed !== 1.0 ? `${effectiveEndTrim}ms` : `${endTrim}ms`;
        startLabel.style.left = `${startPercent}%`;
        endLabel.style.left = `${endPercent}%`;
        
        // Update duration info - show effective playback time
        if (clipDuration) {
            const trimmedLength = clipDuration - startTrim - endTrim;
            const effectiveTrimmedLength = Math.round(trimmedLength / speed);
            if (speed !== 1.0) {
                durationInfo.textContent = `Original: ${trimmedLength.toFixed(0)}ms | Playback: ${effectiveTrimmedLength}ms (${Math.round(speed * 100)}%)`;
            } else {
                durationInfo.textContent = `Full: ${clipDuration.toFixed(0)}ms | Trimmed: ${trimmedLength.toFixed(0)}ms`;
            }
        } else {
            if (speed !== 1.0) {
                durationInfo.textContent = `Trim: ${effectiveStartTrim}ms / ${effectiveEndTrim}ms (${Math.round(speed * 100)}%)`;
            } else {
                durationInfo.textContent = `Trim: ${startTrim}ms / ${endTrim}ms`;
            }
        }
        
        // If handles are at same position, offset them slightly for visibility
        if (Math.abs(endPercent - startPercent) < 0.1) {
            startHandle.style.transform = 'translateX(-2px)';
            endHandle.style.transform = 'translateX(2px)';
        } else {
            startHandle.style.transform = '';
            endHandle.style.transform = '';
        }
    }
    
    function pixelToTrim(x, isEndHandle = false) {
        // Use actual clip duration if available, otherwise use maxTrim
        const effectiveMax = clipDuration || maxTrim;
        
        const rect = canvas.getBoundingClientRect();
        const percent = Math.max(0, Math.min(100, ((x - rect.left) / rect.width) * 100));
        if (isEndHandle) {
            // For end handle, convert from right-side position to trim value
            return Math.round(((100 - percent) / 100) * effectiveMax);
        } else {
            // For start handle, convert from left-side position to trim value
            return Math.round((percent / 100) * effectiveMax);
        }
    }
    
    function updateTrim(newStartTrim, newEndTrim, skipCallback = false) {
        // Use actual clip duration if available, otherwise use maxTrim
        const effectiveMax = clipDuration || maxTrim;
        
        startTrim = Math.max(0, Math.min(effectiveMax, newStartTrim));
        endTrim = Math.max(0, Math.min(effectiveMax, newEndTrim));
        
        // Validate against clip duration if available
        if (clipDuration && (startTrim + endTrim >= clipDuration)) {
            console.warn(`Trim values exceed clip duration. Adjusting...`);
            // Cap the values to leave at least 100ms of playback
            const available = clipDuration - 100;
            if (startTrim + endTrim > available) {
                const ratio = startTrim / (startTrim + endTrim);
                startTrim = Math.floor(available * ratio);
                endTrim = Math.floor(available * (1 - ratio));
            }
        }
        
        updateSelection();
        // On mobile, only trigger callback when not dragging (skipCallback is false)
        // On desktop, always trigger immediately
        if (onTrimChange && !skipCallback) {
            onTrimChange(startTrim, endTrim);
        }
    }
    
    // Public method to update clip duration
    waveformContainer.setClipDuration = (duration) => {
        clipDuration = duration;
        updateSelection(); // Refresh display with new duration
    };
    
    let isDragging = null;
    let dragOffset = 0;
    
    function startDrag(handle, clientX) {
        isDragging = handle;
        // Use actual clip duration if available, otherwise use maxTrim
        const effectiveMax = clipDuration || maxTrim;
        
        const rect = canvas.getBoundingClientRect();
        const handleX = handle === startHandle 
            ? (startTrim / effectiveMax) * rect.width
            : (1 - (endTrim / effectiveMax)) * rect.width; // End is from right
        dragOffset = clientX - rect.left - handleX;
    }
    
    function handleDrag(clientX) {
        if (!isDragging) return;
        
        const rect = canvas.getBoundingClientRect();
        const x = clientX - rect.left - dragOffset;
        
        // On mobile, skip callback during drag to prevent autoplay blocking
        const isMobile = window.innerWidth <= 1024;
        const skipCallback = isMobile;
        
        if (isDragging === startHandle) {
            const newTrim = pixelToTrim(rect.left + x, false);
            updateTrim(newTrim, endTrim, skipCallback);
        } else {
            const newTrim = pixelToTrim(rect.left + x, true);
            updateTrim(startTrim, newTrim, skipCallback);
        }
    }
    
    function stopDrag() {
        // Always trigger trim release callback when drag ends (for re-rendering)
        if (isDragging && onTrimRelease) {
            onTrimRelease(startTrim, endTrim);
        } else if (isDragging && onTrimChange) {
            // Fallback to onTrimChange if onTrimRelease not provided
            onTrimChange(startTrim, endTrim);
        }
        isDragging = null;
    }
    
    // Mouse events on handles
    startHandle.addEventListener('mousedown', (e) => {
        e.stopPropagation();
        e.preventDefault(); // Prevent drag and drop
        startDrag(startHandle, e.clientX);
    });
    
    endHandle.addEventListener('mousedown', (e) => {
        e.stopPropagation();
        e.preventDefault(); // Prevent drag and drop
        startDrag(endHandle, e.clientX);
    });
    
    // Prevent drag and drop on handles
    startHandle.setAttribute('draggable', 'false');
    endHandle.setAttribute('draggable', 'false');
    
    // Also prevent dragstart events
    startHandle.addEventListener('dragstart', (e) => {
        e.preventDefault();
        e.stopPropagation();
        return false;
    });
    
    endHandle.addEventListener('dragstart', (e) => {
        e.preventDefault();
        e.stopPropagation();
        return false;
    });
    
    // Mouse events on canvas (for clicking between handles)
    canvas.addEventListener('mousedown', (e) => {
        const effectiveMax = clipDuration || maxTrim;
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const startHandleX = (startTrim / effectiveMax) * rect.width;
        const endHandleX = (1 - (endTrim / effectiveMax)) * rect.width; // End is from right
        
        if (Math.abs(x - startHandleX) < Math.abs(x - endHandleX)) {
            startDrag(startHandle, e.clientX);
        } else {
            startDrag(endHandle, e.clientX);
        }
    });
    
    document.addEventListener('mousemove', (e) => {
        if (isDragging) {
            handleDrag(e.clientX);
        }
    });
    
    document.addEventListener('mouseup', stopDrag);
    
    // Touch events on handles
    startHandle.addEventListener('touchstart', (e) => {
        e.stopPropagation();
        e.preventDefault();
        startDrag(startHandle, e.touches[0].clientX);
    });
    
    endHandle.addEventListener('touchstart', (e) => {
        e.stopPropagation();
        e.preventDefault();
        startDrag(endHandle, e.touches[0].clientX);
    });
    
    // Touch events on canvas
    canvas.addEventListener('touchstart', (e) => {
        e.preventDefault();
        const effectiveMax = clipDuration || maxTrim;
        const touch = e.touches[0];
        const rect = canvas.getBoundingClientRect();
        const x = touch.clientX - rect.left;
        const startHandleX = (startTrim / effectiveMax) * rect.width;
        const endHandleX = (1 - (endTrim / effectiveMax)) * rect.width; // End is from right
        
        if (Math.abs(x - startHandleX) < Math.abs(x - endHandleX)) {
            startDrag(startHandle, touch.clientX);
        } else {
            startDrag(endHandle, touch.clientX);
        }
    });
    
    document.addEventListener('touchmove', (e) => {
        if (isDragging && e.touches.length > 0) {
            e.preventDefault();
            handleDrag(e.touches[0].clientX);
        }
    });
    
    document.addEventListener('touchend', stopDrag);
    
    // Keyboard navigation
    startHandle.addEventListener('keydown', (e) => {
        const step = 50;
        if (e.key === 'ArrowLeft') {
            e.preventDefault();
            updateTrim(startTrim - step, endTrim);
        } else if (e.key === 'ArrowRight') {
            e.preventDefault();
            updateTrim(startTrim + step, endTrim);
        }
    });
    
    endHandle.addEventListener('keydown', (e) => {
        const step = 50;
        if (e.key === 'ArrowLeft') {
            e.preventDefault();
            updateTrim(startTrim, endTrim - step);
        } else if (e.key === 'ArrowRight') {
            e.preventDefault();
            updateTrim(startTrim, endTrim + step);
        }
    });
    
    // Initial draw
    resizeCanvas(); // This will draw waveform
    updateSelection();
    
    // Public API
    waveformContainer.updateTrim = updateTrim;
    waveformContainer.getTrim = () => ({ start: startTrim, end: endTrim });
    waveformContainer.setClipDuration = (duration) => {
        clipDuration = duration;
        updateSelection(); // Refresh display with new duration
    };
    waveformContainer.regenerateWaveform = regenerateWaveform;
    waveformContainer.loadNewWaveform = loadNewWaveform;
    waveformContainer.setSpeed = (newSpeed) => {
        speed = newSpeed;
        updateSelection(); // Refresh display with new speed
    };
    waveformContainer.getSpeed = () => speed;
    
    return waveformContainer;
}

// Helper function to scale waveform data to a specific width
function scaleWaveformToWidth(sourceData, targetWidth) {
    if (sourceData.length === targetWidth) {
        return sourceData;
    }
    
    const scaled = [];
    const ratio = sourceData.length / targetWidth;
    
    for (let i = 0; i < targetWidth; i++) {
        const sourceIndex = Math.floor(i * ratio);
        scaled.push(sourceData[sourceIndex] || 0);
    }
    
    return scaled;
}

function generateWaveformData(length) {
    const data = [];
    for (let i = 0; i < length; i++) {
        // Generate varied waveform with some randomness
        const base = Math.sin(i * 0.1) * 0.3;
        const noise = (Math.random() - 0.5) * 0.2;
        const burst = Math.random() > 0.95 ? Math.random() * 0.5 : 0;
        data.push(Math.max(0.1, Math.min(1, base + noise + burst)));
    }
    return data;
}

function searchPhrases() {
    if (!activeProject || !elements.phraseInput) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        return;
    }

    const phrases = elements.phraseInput.value
        .split(',')
        .map(phrase => phrase.trim())
        .filter(phrase => phrase.length > 0);

    if (!phrases.length) {
        return;
    }

    const silencePreferences = getSilencePreferences();
    updateProjectData(data => {
        data.phraseInput = elements.phraseInput.value;
        data.silencePreferences = silencePreferences;
    });

    searchCounter += 1;

    // Show loading indicator
    showSearchLoading(`Searching for ${phrases.length} phrase${phrases.length > 1 ? 's' : ''}...`);

    fetch('/search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            files: selectedFiles,
            phrases,
            searchCounter,
            minSilence: silencePreferences.minSilence,
            maxSilence: silencePreferences.maxSilence,
            silenceWordThreshold: silencePreferences.silenceWordThreshold
        })
    })
        .then(response => response.json())
        .then(data => {
            hideSearchLoading();
            if (!data || data.length === 0) {
                showNoResults('No matches found for any of the searched phrases.');
            } else {
            updateDropdowns(data);
            }
        })
        .catch(error => {
            hideSearchLoading();
            const errorMessage = error.message || 'An error occurred while searching. Please try again.';
            console.error('Error searching phrases:', errorMessage);
            showNoResults(`Error: ${errorMessage}`);
        });
}

function handleSentenceSearch() {
    if (!activeProject || !elements.sentenceInput) {
        return;
    }

    const rawSentence = elements.sentenceInput.value.trim();
    if (!rawSentence) {
        return;
    }
    
    // Split into semicolon-separated groups and keep non-empty trimmed ones
    const sentenceGroups = rawSentence
        .split(';')
        .map(part => part.trim())
        .filter(part => part.length > 0);
        
    if (sentenceGroups.length === 0) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        return;
    }
    
    // Clear previous results before starting new search
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }
    
    // Immediately jump to results on mobile when search is initiated
    if (isMobileLayout()) {
        switchMobileSection('results');
    }

    // Get the checkbox values for partial matches
    const includePartialMatchesCheckbox = document.getElementById('includePartialMatches');
    const includePartialMatches = includePartialMatchesCheckbox ? includePartialMatchesCheckbox.checked : false;
    const allPartialMatchesCheckbox = document.getElementById('allPartialMatches');
    const allPartialMatches = allPartialMatchesCheckbox ? allPartialMatchesCheckbox.checked : false;
    
    // Get the max results per segment value
    const maxResultsInput = document.getElementById('maxResultsPerSegment');
    const maxResultsPerSegment = maxResultsInput ? parseInt(maxResultsInput.value, 10) : 25;
    
    updateProjectData(data => {
        data.currentSentence = rawSentence;
        data.silencePreferences = getSilencePreferences();
        data.includePartialMatches = includePartialMatches;
        data.allPartialMatches = allPartialMatches;
        data.maxResultsPerSegment = maxResultsPerSegment;
    });

    sentenceSearchCounter += 1;

    // Clear skipped segments from previous search
    window.skippedSegments = new Set();
    window.currentSearchId = null;  // Will be set from server progress messages
    
    // Create AbortController for cancellation
    const abortController = new AbortController();
    let reader = null;
    
    // Show loading indicator with cancel button
    const displaySentence = sentenceGroups.join('; ');

    showSearchLoading(`Searching for "${displaySentence}"...`, () => {
        console.log('[Client] ❌ CANCEL SEARCH requested by user');
        
        // Send cancellation request to backend to stop rendering immediately
        if (window.currentSearchId) {
            fetch('/cancel_search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    search_id: window.currentSearchId
                })
            }).then(() => {
                console.log('[Client] ✓ Cancellation request sent to server');
            }).catch(err => {
                console.warn('[Client] Failed to send cancellation request:', err);
            });
        }
        
        // Abort the fetch and close the stream
        try {
            abortController.abort();
            if (reader) {
                reader.cancel().catch(() => {}); // Ignore errors during cancellation
            }
        } catch (e) {
            // Ignore errors during abort
        }
        hideSearchLoading();
        console.log('[Client] ✓ Search cancelled, connection closed');
    });

    // Use SSE endpoint for incremental results
    fetch('/search_longest_segments_stream', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            files: selectedFiles,
            sentence: rawSentence,
            sentence_groups: sentenceGroups,
            sentenceSearchCounter,
            minSilence: getSilencePreferences().minSilence,
            maxSilence: getSilencePreferences().maxSilence,
            silenceWordThreshold: getSilencePreferences().silenceWordThreshold,
            includePartialMatches: includePartialMatches,
            allPartialMatches: allPartialMatches,
            maxResultsPerSegment: maxResultsPerSegment
        }),
        signal: abortController.signal
    })
        .then(response => {
            reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            let receivedAnyResult = false;

            function processStream() {
                return reader.read().then(({ done, value }) => {
                    if (done) {
                        if (!receivedAnyResult) {
                            hideSearchLoading();
                            showNoResults(`No matches found for "${displaySentence}".`);
                        } else {
                            hideSearchLoading();
                        }
                        return;
                    }

                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop(); // Keep incomplete line in buffer

                    for (const line of lines) {
                        if (line.startsWith('data: ')) {
                            const data = JSON.parse(line.slice(6));
                            
                            if (data.done) {
                                if (!receivedAnyResult) {
                                    showNoResults(`No matches found for "${displaySentence}".`);
                                }
                                hideSearchLoading();
                                return;
                            }
                            
                            if (data.error) {
                                console.error('Search error:', data.error);
                                hideSearchLoading();
                                if (!receivedAnyResult) {
                                    showNoResults('An error occurred during search. Please try again.');
                                }
                                return;
                            }
                            
                            // Handle progress updates
                            if (data.progress) {
                                console.log(`[Client] Received progress: ${data.segment_index}/${data.total_segments} - "${data.progress}"`);
                                // Store search_id from progress messages
                                if (data.search_id) {
                                    window.currentSearchId = data.search_id;
                                }
                                updateSearchLoadingMessage(data.progress, data);
                                // Don't return - continue processing other lines in this chunk
                                continue;
                            }
                            
                            // Received a segment result - add it incrementally
                            if (data.phrase) {
                                // Check if this segment was skipped on the server
                                if (data.skipped) {
                                    console.log(`[Client] ⏭️  Segment skipped by server: '${data.phrase.substring(0, 50)}...'`);
                                    updateSearchLoadingMessage(`⏭️ Skipped: '${data.phrase}' (no clips rendered)`);
                                    // Don't return - continue processing other lines in this chunk
                                    continue;
                                }
                                
                                console.log(`[Client] ✓ Received segment: '${data.phrase.substring(0, 50)}...' (${data.files ? data.files.length : 0} clips)`);
                                receivedAnyResult = true;
                                addSegmentResult(data);
                                // Don't return - continue processing other lines in this chunk
                                continue;
                            }
                        }
                    }

                    // After processing all lines in this chunk, continue with next chunk
                    return processStream();
                });
            }

            return processStream();
        })
        .catch(error => {
            // Ignore abort errors and connection errors (user cancelled)
            if (error.name === 'AbortError' || error.message.includes('aborted') || error.message.includes('input stream')) {
                console.log('[Client] Search cancelled - error handled gracefully');
                return;
            }
            hideSearchLoading();
            console.error('[Client] ❌ Error searching sentence:', error);
            showNoResults('An error occurred while searching. Please try again.');
        });
}

function handleGenerateStory() {
    if (!activeProject || !elements.storyPromptInput) {
        return;
    }

    const prompt = elements.storyPromptInput.value.trim();
    if (!prompt) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        alert('Please select at least one video file.');
        return;
    }

    // Get configuration options
    const maxStorySegmentsInput = document.getElementById('maxStorySegments');
    const maxStorySegments = maxStorySegmentsInput ? parseInt(maxStorySegmentsInput.value, 10) : 10;
    
    const preferLongSegmentsCheckbox = document.getElementById('preferLongSegments');
    const preferLongSegments = preferLongSegmentsCheckbox ? preferLongSegmentsCheckbox.checked : true;
    
    const debugModeCheckbox = document.getElementById('debugMode');
    const debugMode = debugModeCheckbox ? debugModeCheckbox.checked : false;

    // Clear previous results before generating new story
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }
    
    // Save to project data
    updateProjectData(data => {
        data.storyPrompt = prompt;
        data.maxStorySegments = maxStorySegments;
        data.preferLongSegments = preferLongSegments;
        data.debugMode = debugMode;
    });

    // Show loading indicator
    showSearchLoading(`Generating story with AI...`, null);

    // Call backend to generate story
    fetch('/generate_story', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            prompt: prompt,
            files: selectedFiles,
            maxSegments: maxStorySegments,
            preferLongSegments: preferLongSegments,
            debugMode: debugMode
        })
    })
        .then(response => response.json())
        .then(data => {
        hideSearchLoading();
        
        if (data.type === 'error') {
            console.error('[Client] ❌ Error generating story:', data.error);
            showNoResults(`Error: ${data.error}`);
            return;
        } else if (data.type === 'no_matches') {
            console.log(`[Client] No matches found for: ${sentenceGroups.join('; ')}`);
            hideSearchLoading();
            showNoResults(`No matches found for: ${sentenceGroups.join('; ')}`);
            return;
        }

        if (!data.segments || data.segments.length === 0) {
            showNoResults('AI could not generate a story with the available segments.');
            return;
        }

        // Clear existing results
        if (elements.resultsContainer) {
            elements.resultsContainer.innerHTML = '';
        }

        // Display the generated story segments
        console.log(`[Client] ✓ Received ${data.segments.length} story segments from AI`);
        
        // Store LLM prompt for copying (from debug info)
        if (data.debug && data.debug.prompt) {
            window.lastLLMPrompt = data.debug.prompt;
        }
        
        // Display debug information if available
        if (data.debug) {
            console.log('[Client] 🔍 DEBUG INFO:', data.debug);
            displayDebugInfo(data.debug);
        }
        
        displayStorySegments(data.segments, data.story_explanation);
        })
        .catch(error => {
        hideSearchLoading();
        console.error('[Client] ❌ Error generating story:', error);
        showNoResults('An error occurred while generating the story. Please try again.');
    });
}

// Store prompt when generating (before API call)
let lastLLMPromptData = null;

function handleCopyLLMPrompt() {
    // If we already have a prompt from a previous generation, use it
    if (window.lastLLMPrompt) {
        navigator.clipboard.writeText(window.lastLLMPrompt).then(() => {
            alert('LLM prompt copied to clipboard!');
        }).catch(err => {
            console.error('Failed to copy prompt:', err);
            alert('Failed to copy prompt to clipboard.');
        });
        return;
    }
    
    // Otherwise, generate the prompt without calling Ollama
    if (!activeProject || !elements.storyPromptInput) {
        alert('Please enter a story prompt first.');
        return;
    }
    
    const prompt = elements.storyPromptInput.value.trim();
    if (!prompt) {
        alert('Please enter a story prompt first.');
        return;
    }
    
    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        alert('Please select at least one video file.');
        return;
    }
    
    // Get configuration options
    const maxStorySegmentsInput = document.getElementById('maxStorySegments');
    const maxStorySegments = maxStorySegmentsInput ? parseInt(maxStorySegmentsInput.value, 10) : 10;
    
    // Show loading indicator
    showSearchLoading(`Generating LLM prompt...`, null);
    
    // Call backend to generate prompt (without calling Ollama)
    fetch('/generate_story_prompt', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            prompt: prompt,
            files: selectedFiles,
            maxSegments: maxStorySegments
        })
    })
    .then(response => response.json())
    .then(data => {
        hideSearchLoading();
        
        if (data.error) {
            console.error('Prompt generation error:', data.error);
            alert(`Error: ${data.error}`);
            return;
        }
        
        if (!data.prompt) {
            alert('Failed to generate prompt.');
            return;
        }
        
        // Store the prompt and copy it
        window.lastLLMPrompt = data.prompt;
        navigator.clipboard.writeText(data.prompt).then(() => {
            alert('LLM prompt generated and copied to clipboard!');
        }).catch(err => {
            console.error('Failed to copy prompt:', err);
            alert('Prompt generated but failed to copy to clipboard.');
        });
    })
    .catch(error => {
        hideSearchLoading();
        console.error('[Client] ❌ Error generating prompt:', error);
        alert('An error occurred while generating the prompt. Please try again.');
    });
}

function handleShowInsertResponse() {
    if (elements.insertResponseContainer) {
        elements.insertResponseContainer.style.display = 'block';
        if (elements.llmResponseInput) {
            elements.llmResponseInput.focus();
        }
    }
}

function handleCancelInsertResponse() {
    if (elements.insertResponseContainer) {
        elements.insertResponseContainer.style.display = 'none';
        if (elements.llmResponseInput) {
            elements.llmResponseInput.value = '';
        }
    }
}

function handleProcessInsertedResponse() {
    if (!elements.llmResponseInput || !activeProject) {
        return;
    }
    
    const responseText = elements.llmResponseInput.value.trim();
    if (!responseText) {
        alert('Please paste the LLM response JSON.');
        return;
    }
    
    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        alert('Please select at least one video file.');
        return;
    }
    
    // Parse the JSON response
    let storyData;
    try {
        // Try to extract JSON from the response (in case it's wrapped in markdown or text)
        const jsonMatch = responseText.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            storyData = JSON.parse(jsonMatch[0]);
        } else {
            storyData = JSON.parse(responseText);
        }
    } catch (err) {
        alert('Invalid JSON format. Please check the response and try again.');
        console.error('JSON parse error:', err);
        return;
    }
    
    // Process the response similar to backend processing
    // Support both "sentences" and "storyline" keys (some LLMs use different formats)
    const explanation = storyData.explanation || '';
    const sentences = storyData.sentences || storyData.storyline || [];
    
    if (!sentences.length) {
        alert('No sentences found in the response. Expected "sentences" or "storyline" array.');
        return;
    }
    
    // Hide the insert response container
    handleCancelInsertResponse();
    
    // Show loading indicator
    showSearchLoading(`Processing ${sentences.length} sentences...`, null);
    
    // Process each sentence: extract source_segments for searching
    // Each sentence may have multiple source_segments that need to be searched separately
    const sentenceQueries = [];
    for (const sentenceObj of sentences) {
        if (typeof sentenceObj === 'string') {
            // Legacy format: just a string (use as both sentence and search query)
            sentenceQueries.push([sentenceObj, [sentenceObj]]);
        } else if (sentenceObj.sentence && sentenceObj.source_segments) {
            // New format: object with sentence and source_segments
            // source_segments are the actual corpus segments to search for
            const sourceSegments = Array.isArray(sentenceObj.source_segments) 
                ? sentenceObj.source_segments 
                : [sentenceObj.source_segments];
            // Store as [sentence_text, [source_segment1, source_segment2, ...]]
            sentenceQueries.push([sentenceObj.sentence, sourceSegments]);
        }
    }
    
    // Now search for each source_segment and add to results
    // Each source_segment will be searched separately and create a separate result card
    processStorySentences(sentenceQueries, selectedFiles, explanation);
}

function processStorySentences(sentenceQueries, selectedFiles, explanation) {
    // Send the response to backend to process (reuse the generate_story endpoint logic)
    // We'll create a simplified version that processes the response
    console.log(`Processing ${sentenceQueries.length} sentences from inserted response...`);
    
    // Convert sentenceQueries to the format expected by displayStorySegments
    // We need to search for each query and create segment results
    // For now, let's create a simplified version that calls the search for each query
    
    const allSegments = [];
    let processedCount = 0;
    
    function processNext() {
        if (processedCount >= sentenceQueries.length) {
            hideSearchLoading();
            if (allSegments.length > 0) {
                displayStorySegments(allSegments, explanation);
            } else {
                showNoResults('No matches found for the inserted response.');
            }
            return;
        }
        
        const [sentenceText, searchQueries] = sentenceQueries[processedCount];
        processedCount++;
        
        // Search for ALL queries in the source_segments (not just the first one)
        if (searchQueries.length > 0) {
            let queryIndex = 0;
            const searchId = `inserted-${processedCount}-${Date.now()}`;
            
            function searchNextQuery() {
                if (queryIndex >= searchQueries.length) {
                    // All queries for this sentence processed, move to next sentence
                    setTimeout(processNext, 50);
                    return;
                }
                
                const searchQuery = searchQueries[queryIndex];
                queryIndex++;
                
                fetch('/search_longest_segments_stream', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        sentence: searchQuery,
                        files: selectedFiles,
                        sentenceSearchCounter: processedCount,
                        includePartialMatches: false,
                        maxResultsPerSegment: 1,
                        exportClips: true,
                        clipGroup: `story-${searchId}`
                    })
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    
                    const reader = response.body.getReader();
                    const decoder = new TextDecoder();
                    let buffer = '';
                    let foundMatch = false;
                    
                    function readStream() {
                        return reader.read().then(({ done, value }) => {
                            if (done) {
                                // Move to next query (or next sentence if all queries done)
                                searchNextQuery();
                                return;
                            }
                            
                            buffer += decoder.decode(value, { stream: true });
                            const lines = buffer.split('\n');
                            buffer = lines.pop() || '';
                            
                            for (const line of lines) {
                                if (line.startsWith('data: ')) {
                                    try {
                                        const data = JSON.parse(line.slice(6));
                                        if (data.files && data.files.length > 0 && !foundMatch) {
                                            foundMatch = true;
                                            const segmentData = {
                                                phrase: searchQuery,
                                                files: data.files,
                                                created_sentence: sentenceText
                                            };
                                            allSegments.push(segmentData);
                                        }
                                    } catch (e) {
                                        // Ignore parse errors
                                    }
                                }
                            }
                            
                            return readStream();
                        });
                    }
                    
                    return readStream();
                })
                .catch(err => {
                    console.error('Error searching:', err);
                    // Continue to next query even if this one failed
                    searchNextQuery();
                });
            }
            
            // Start searching queries for this sentence
            searchNextQuery();
        } else {
            setTimeout(processNext, 50);
        }
    }
    
    processNext();
}

function displayDebugInfo(debugData) {
    if (!elements.resultsContainer || !debugData) {
        return;
    }
    
    const debugDiv = document.createElement('div');
    debugDiv.className = 'debug-info';
    debugDiv.style.cssText = 'padding: 16px; margin-bottom: 16px; background: rgba(255,200,0,0.1); border: 1px solid rgba(255,200,0,0.3); border-radius: 8px; font-family: monospace; font-size: 12px; max-height: 400px; overflow-y: auto;';
    
    let debugHTML = '<strong style="color: #ffc800;">🔍 DEBUG MODE</strong><br><br>';
    
    if (debugData.corpus_entry_count !== undefined) {
        debugHTML += `<strong>Corpus Entries:</strong> ${debugData.corpus_entry_count}<br><br>`;
    }
    
    if (debugData.corpus) {
        debugHTML += '<details><summary><strong>Corpus Data</strong></summary>';
        debugHTML += `<pre style="white-space: pre-wrap; word-wrap: break-word; margin-top: 8px;">${escapeHtml(debugData.corpus)}</pre>`;
        debugHTML += '</details><br>';
    }
    
    if (debugData.prompt) {
        debugHTML += '<details><summary><strong>Full Prompt Sent to LLM</strong></summary>';
        debugHTML += `<pre style="white-space: pre-wrap; word-wrap: break-word; margin-top: 8px;">${escapeHtml(debugData.prompt)}</pre>`;
        debugHTML += '</details><br>';
    }
    
    if (debugData.llm_response) {
        debugHTML += '<details><summary><strong>LLM Response</strong></summary>';
        debugHTML += `<pre style="white-space: pre-wrap; word-wrap: break-word; margin-top: 8px;">${escapeHtml(debugData.llm_response)}</pre>`;
        debugHTML += '</details><br>';
    }
    
    if (debugData.sentence_queries && Array.isArray(debugData.sentence_queries)) {
        debugHTML += '<details><summary><strong>LLM Created Sentences & Search Queries</strong></summary>';
        debugHTML += '<ol style="margin-top: 8px; padding-left: 20px;">';
        debugData.sentence_queries.forEach((item, idx) => {
            debugHTML += `<li style="margin-bottom: 12px;">`;
            debugHTML += `<strong>Created:</strong> ${escapeHtml(item.sentence)}<br>`;
            if (item.search_queries && Array.isArray(item.search_queries)) {
                debugHTML += `<strong>Search queries:</strong> `;
                debugHTML += item.search_queries.map(q => `"${escapeHtml(q)}"`).join(', ');
            }
            debugHTML += `</li>`;
        });
        debugHTML += '</ol></details><br>';
    } else if (debugData.llm_sentences && Array.isArray(debugData.llm_sentences)) {
        debugHTML += '<details><summary><strong>LLM Created Sentences (legacy format)</strong></summary>';
        debugHTML += '<ol style="margin-top: 8px; padding-left: 20px;">';
        debugData.llm_sentences.forEach((sentence, idx) => {
            debugHTML += `<li style="margin-bottom: 8px;">${escapeHtml(sentence)}</li>`;
        });
        debugHTML += '</ol></details><br>';
    }
    
    if (debugData.selected_indices) {
        debugHTML += `<strong>Selected Indices (legacy format):</strong> [${debugData.selected_indices.join(', ')}]<br>`;
    }
    
    debugDiv.innerHTML = debugHTML;
    
    // Insert at the beginning of results container
    if (elements.resultsContainer.firstChild) {
        elements.resultsContainer.insertBefore(debugDiv, elements.resultsContainer.firstChild);
    } else {
        elements.resultsContainer.appendChild(debugDiv);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function displayStorySegments(segments, storyExplanation) {
    // Clear existing results
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }

    // Add explanation header if provided
    if (storyExplanation) {
        const explanationDiv = document.createElement('div');
        explanationDiv.className = 'story-explanation';
        explanationDiv.style.cssText = 'padding: 16px; margin-bottom: 16px; background: rgba(255,255,255,0.05); border-radius: 8px; font-style: italic;';
        explanationDiv.innerHTML = `<strong>AI Story:</strong> ${storyExplanation}`;
        elements.resultsContainer.appendChild(explanationDiv);
    }

    // Display each segment
    segments.forEach((segmentData, index) => {
        addSegmentResult(segmentData, index + 1);
    });

    // Auto-add all segments to timeline
    console.log(`[Client] Auto-adding ${segments.length} story segments to timeline`);
    setTimeout(() => {
        const containers = document.querySelectorAll('.phrase-container');
        containers.forEach(container => {
            // Auto-select first match in each segment
            const listbox = container.querySelector('.listbox');
            if (listbox && listbox.options.length > 0) {
                listbox.selectedIndex = 0;
                listbox.dispatchEvent(new Event('change'));
            }
            // Add to timeline
            handleAddContainerToTimeline(container);
        });
    }, 500);
}

function handleStartOver() {
    searchCounter = 0;
    sentenceSearchCounter = 0;
    globalPhraseContainerIDCounter = 0;
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }
    if (elements.videoPlayer) {
        elements.videoPlayer.pause();
        elements.videoPlayer.removeAttribute('src');
        elements.videoPlayer.load();
    }
}

function collectVideoData() {
    const containers = document.querySelectorAll('.phrase-container');
    return Array.from(containers).map(container => ({
        video: container.querySelector('select').value,
        startTrim: parseInt(container.querySelector('.start-trim-slider').value, 10),
        endTrim: parseInt(container.querySelector('.end-trim-slider').value, 10)
    }));
}

function playVideo(filePath, shouldLoop = false, playbackRate = 1.0) {
    if (!elements.videoPlayer) {
        return;
    }

    clearTimeout(videoTimeoutId);

    // Remove any existing event listeners
    elements.videoPlayer.onloadedmetadata = null;

    try {
        elements.videoPlayer.src = getVideoUrl(filePath);
        elements.videoPlayer.loop = shouldLoop;
        elements.videoPlayer.load();
        
        // Set playback rate after metadata loads to ensure it's applied
        elements.videoPlayer.onloadedmetadata = () => {
            if (playbackRate !== 1.0) {
                elements.videoPlayer.playbackRate = playbackRate;
            }
            elements.videoPlayer.play().catch(error => {
                console.error('Error playing the video:', error);
            });
        };
        
        // Also set playback rate immediately (some browsers need this)
        elements.videoPlayer.playbackRate = playbackRate;
    } catch (error) {
        console.error('Exception while setting up video playback:', error);
    }

    elements.videoPlayer.onerror = event => {
        console.error('Error event on video element:', event);
    };
}

function playVideoWithTrim(filePath, startTrim, endTrim, shouldLoop = false, playbackRate = 1.0) {
    if (!elements.videoPlayer) {
        return;
    }

    clearTimeout(videoTimeoutId);

    elements.videoPlayer.src = getVideoUrl(filePath);
    elements.videoPlayer.loop = shouldLoop;
    elements.videoPlayer.playbackRate = playbackRate;
    elements.videoPlayer.load();

    // Remove any existing event listeners to prevent multiple handlers
    elements.videoPlayer.onloadedmetadata = null;
    elements.videoPlayer.onseeked = null;
    
    elements.videoPlayer.onloadedmetadata = () => {
        const duration = elements.videoPlayer.duration;
        const startSeconds = startTrim / 1000;
        const endSeconds = endTrim / 1000;
        
        // Validate that trim values don't exceed clip duration
        if (startSeconds + endSeconds >= duration) {
            console.error(`Trim values (start: ${startTrim}ms, end: ${endTrim}ms) exceed clip duration (${(duration * 1000).toFixed(0)}ms). Nothing to play.`);
            alert(`Cannot play: trim values exceed clip duration.\nClip: ${(duration * 1000).toFixed(0)}ms\nStart trim: ${startTrim}ms\nEnd trim: ${endTrim}ms\nRemaining: ${((duration * 1000) - startTrim - endTrim).toFixed(0)}ms`);
            return;
        }

        // Set playback rate before seeking
        elements.videoPlayer.playbackRate = playbackRate;
        
        // Set up seeked handler to ensure seek completes before playing
        let seekTimeoutId = null;
        let playbackStarted = false;
        
        const startPlayback = () => {
            if (playbackStarted) return;
            playbackStarted = true;
            
            // Clear seek timeout if it exists
            if (seekTimeoutId) {
                clearTimeout(seekTimeoutId);
                seekTimeoutId = null;
            }
            
            // Set up end trim timeout if needed
            if (endTrim > 0) {
                const endTime = duration - endSeconds;
                // Adjust timeout based on playback rate
                const playDuration = (endTime - startSeconds) / playbackRate;
                videoTimeoutId = setTimeout(() => {
                    if (shouldLoop) {
                        elements.videoPlayer.currentTime = startSeconds;
                        elements.videoPlayer.play();
                    } else {
                        elements.videoPlayer.pause();
                    }
                }, playDuration * 1000);
            }
            
            // Start playback only after seek completes
            elements.videoPlayer.play().catch(error => console.error('Error playing the video:', error));
        };
        
        elements.videoPlayer.onseeked = () => {
            // Clear the seeked handler to prevent it from firing multiple times
            elements.videoPlayer.onseeked = null;
            startPlayback();
        };
        
        // Set currentTime - this will trigger the seeked event
        elements.videoPlayer.currentTime = startSeconds;
        
        // Fallback: if seeked doesn't fire within 200ms, start playback anyway
        // This handles edge cases where seeked might not fire (e.g., if already at target time)
        seekTimeoutId = setTimeout(() => {
            if (!playbackStarted) {
                elements.videoPlayer.onseeked = null;
                startPlayback();
            }
        }, 200);
    };
}

async function rerenderClipWithNewTrims(phraseContainer, startTrimMs, endTrimMs) {
    if (!phraseContainer) {
        console.warn('rerenderClipWithNewTrims: no container provided');
        return null;
    }
    
    const selectElement = phraseContainer.querySelector('select');
    if (!selectElement) {
        console.error('rerenderClipWithNewTrims: missing select element');
        return null;
    }
    
    // Ensure an option is selected (select first if none selected)
    if (selectElement.selectedIndex < 0 && selectElement.options.length > 0) {
        selectElement.selectedIndex = 0;
    }
    
    const selectedOption = selectElement.options[selectElement.selectedIndex];
    if (!selectedOption) {
        console.error('rerenderClipWithNewTrims: no selected option');
        return null;
    }
    
    // Use original clip path for re-rendering to avoid filename accumulation
    const originalClipPath = selectedOption.dataset.originalClipPath || selectElement.value;
    const sourceVideo = selectedOption.title;
    const originalStart = parseFloat(selectedOption.dataset.originalStart);
    const originalEnd = parseFloat(selectedOption.dataset.originalEnd);
    
    if (!originalClipPath || !sourceVideo || isNaN(originalStart) || isNaN(originalEnd)) {
        console.warn('rerenderClipWithNewTrims: missing required data', {
            originalClipPath,
            sourceVideo,
            originalStart,
            originalEnd
        });
        return null;
    }
    
    try {
        console.log('Re-rendering clip with:', {
            originalClipPath,
            sourceVideo,
            originalStart,
            originalEnd,
            startTrimMs,
            endTrimMs
        });
        
        const response = await fetch('/rerender_clip', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                clip_path: originalClipPath,
                source_video: sourceVideo,
                original_start: originalStart,
                original_end: originalEnd,
                start_trim_ms: startTrimMs,
                end_trim_ms: endTrimMs
            })
        });
        
        if (!response.ok) {
            const error = await response.json();
            console.error('rerenderClipWithNewTrims: server error', error);
            return null;
        }
        
        const result = await response.json();
        return result.clip_path;
    } catch (error) {
        console.error('rerenderClipWithNewTrims: request failed', error);
        return null;
    }
}

async function playTrimmedVideo(phraseContainer) {
    if (!phraseContainer) {
        console.warn('playTrimmedVideo: no container provided');
        return;
    }
    
    const selectElement = phraseContainer.querySelector('select');
    const startSlider = phraseContainer.querySelector('.start-trim-slider');
    const endSlider = phraseContainer.querySelector('.end-trim-slider');
    
    if (!selectElement || !startSlider || !endSlider) {
        console.error('playTrimmedVideo: missing required elements', {
            hasSelect: !!selectElement,
            hasStartSlider: !!startSlider,
            hasEndSlider: !!endSlider
        });
        return;
    }
    
    const selectedVideo = selectElement.value;
    const startTrim = parseInt(startSlider.value, 10);
    const endTrim = parseInt(endSlider.value, 10);
    
    console.log('playTrimmedVideo:', { selectedVideo, startTrim, endTrim });
    
    // Check if this clip is already re-rendered
    const selectedOption = selectElement.options[selectElement.selectedIndex];
    const isRerendered = selectedOption?.dataset?.rerendered === 'true';
    
    if (!isRerendered) {
        // Re-render with current trim values before first playback
        const rerenderedPath = await rerenderClipWithNewTrims(phraseContainer, startTrim, endTrim);
        if (rerenderedPath && rerenderedPath.trim() !== '') {
            selectedOption.value = rerenderedPath;
            selectElement.value = rerenderedPath;
            selectedOption.dataset.rerendered = 'true';
            // Play without trimming (already trimmed)
            playVideoWithTrimInFloatingPreview(rerenderedPath, 0, 0, phraseContainer, selectElement);
            return;
        }
    }
    
    // If already re-rendered, play without trimming (clip is already trimmed)
    if (isRerendered) {
        playVideoWithTrimInFloatingPreview(selectedVideo, 0, 0, phraseContainer, selectElement);
    } else {
        // If re-render failed, fallback to JavaScript trimming
        playVideoWithTrimInFloatingPreview(selectedVideo, startTrim, endTrim, phraseContainer, selectElement);
    }
}

function playAllVideos() {
    const containers = document.querySelectorAll('.phrase-container');
    if (!containers.length || !elements.videoPlayer) {
        return;
    }

    let currentIndex = 0;

    function playNextVideo() {
        if (currentIndex < containers.length) {
            playTrimmedVideo(containers[currentIndex]);
            currentIndex += 1;
        } else {
            elements.videoPlayer.onpause = null;
            elements.videoPlayer.onended = null;
        }
    }

    elements.videoPlayer.onpause = playNextVideo;
    elements.videoPlayer.onended = playNextVideo;
    playNextVideo();
}

function addSegmentResult(segmentData) {
    /**
     * Add a single segment result to the results container incrementally.
     * Called for each segment as it's ready from the SSE stream.
     */
    if (!segmentData || !segmentData.phrase) {
        console.error('Invalid segment data:', segmentData);
        return;
    }

    if (!elements.resultsContainer) {
        console.error('Results container not found');
        return;
    }

    // Create a simple phrase container for incremental results
    // (This is a simplified version that can be enhanced later)
    const phraseContainer = document.createElement('div');
    const containerId = `phraseContainer${globalPhraseContainerIDCounter++}`;
    phraseContainer.id = containerId;
    phraseContainer.classList.add('phrase-container');
    phraseContainer.dataset.containerid = globalPhraseContainerIDCounter;
    // Store segmentData for access in floating preview
    phraseContainer.dataset.segmentData = JSON.stringify(segmentData);

    const title = document.createElement('h4');
    title.textContent = segmentData.phrase;
    title.classList.add('phrase-title');
    title.setAttribute('draggable', 'true');
    phraseContainer.appendChild(title);
    registerSearchContainerDrag(title, phraseContainer);

    const listbox = document.createElement('select');
    listbox.id = `phrase${containerId}`;
    listbox.size = 5;
    listbox.classList.add('phrase-video-listbox');

    // Store waveform reference for updating when match changes
    let waveformRef = null;

    segmentData.files.forEach((file, fileIndex) => {
        const option = document.createElement('option');
        if (typeof file === 'object' && file !== null) {
            // Debug: log file object to see what fields are present
            if (fileIndex === 0) {
                console.log('First file object in segment:', file);
            }
            
            option.value = file.file;
            const sourceVideo = file.source_video || 'Unknown';
            const videoName = sourceVideo.split('/').pop();
            // Get duration_ms from server - this is the exported clip duration (not full video)
            // file.file points to the exported clip in temp/, so duration_ms is the clip length
            // We need to subtract default trim values (450ms start + 450ms end) to get actual playable length
            let clipDurationMs = file.duration_ms;
            // Handle string numbers
            if (clipDurationMs != null && typeof clipDurationMs === 'string') {
                clipDurationMs = parseFloat(clipDurationMs);
            }
            // Fallback: calculate from silence times if available (for silence searches)
            // For silence searches, clips are exported with 0/0 trims, so use full duration
            const defaultStartTrim = (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') ? 0 : 450;
            const defaultEndTrim = (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') ? 0 : 450;
            
            if ((!clipDurationMs || isNaN(clipDurationMs) || clipDurationMs <= 0) && typeof file.silence_start === 'number' && typeof file.silence_end === 'number') {
                clipDurationMs = (file.silence_end - file.silence_start) * 1000;
            }
            const trimmedLength = formatTrimmedLength(clipDurationMs, defaultStartTrim, defaultEndTrim);
            // Always show the trimmed length, even if empty (format function returns empty string if invalid)
            option.text = `Match ${fileIndex + 1} (${trimmedLength}${videoName})`;
            option.title = sourceVideo;
            // Store the original exported clip path for waveform (before any re-rendering)
            option.dataset.originalClipPath = file.file;
            // Store original segment boundaries for re-rendering
            // Check for both null/undefined and ensure it's a valid number
            if (file.original_start != null && (typeof file.original_start === 'number' || !isNaN(parseFloat(file.original_start)))) {
                option.dataset.originalStart = String(file.original_start);
            } else {
                console.warn('Missing original_start in file data:', file);
            }
            if (file.original_end != null && (typeof file.original_end === 'number' || !isNaN(parseFloat(file.original_end)))) {
                option.dataset.originalEnd = String(file.original_end);
            } else {
                console.warn('Missing original_end in file data:', file);
            }
            // Store duration if provided (will be loaded later if not available)
            if (clipDurationMs != null && !isNaN(clipDurationMs) && clipDurationMs > 0) {
                option.dataset.durationMs = String(clipDurationMs);
            } else {
                // Try to load duration asynchronously from exported clip metadata
                // file.file points to the exported clip (temp/...), not the source video
                const clipPath = file.file;
                if (clipPath) {
                    getVideoDuration(clipPath).then(duration => {
                        if (duration && duration > 0) {
                            // This is the exported clip duration, subtract default trims for playable length
                            option.dataset.durationMs = String(duration);
                            const updatedTrimmedLength = formatTrimmedLength(duration, defaultStartTrim, defaultEndTrim);
                            option.text = `Match ${fileIndex + 1} (${updatedTrimmedLength}${videoName})`;
                        }
                    }).catch(() => {
                        // Ignore errors - duration will remain unknown
                    });
                }
            }
        } else {
            option.value = file;
            option.text = `Match ${fileIndex + 1}`;
        }
        listbox.appendChild(option);
    });

    phraseContainer.appendChild(listbox);
    
    // Ensure first option is selected by default
    if (listbox.options.length > 0) {
        listbox.selectedIndex = 0;
    }

    console.log(`Created listbox with ${segmentData.files.length} options for segment: ${segmentData.phrase}`);

    // Create sliders (hidden, but needed for trim logic)
    const startSlider = document.createElement('input');
    startSlider.type = 'range';
    startSlider.min = '0';
    startSlider.max = '5000';
    startSlider.value = '450';
    startSlider.step = '10';
    startSlider.classList.add('start-trim-slider');
    startSlider.style.display = 'none';

    const endSlider = document.createElement('input');
    endSlider.type = 'range';
    endSlider.min = '0';
    endSlider.max = '5000';
    endSlider.value = '450';
    endSlider.step = '10';
    endSlider.classList.add('end-trim-slider');
    endSlider.style.display = 'none';

    phraseContainer.appendChild(startSlider);
    phraseContainer.appendChild(endSlider);

    // Get original source video and segment boundaries for re-rendering
    // Use exported clip for waveform (faster), but track original segment for accurate trimming
    const firstFile = segmentData.files[0];
    let originalSourceVideo = null;
    let originalStart = null;
    let originalEnd = null;
    let originalDurationMs = null;
    const initialVideo = (typeof firstFile === 'object') ? firstFile.file : firstFile;
    
    if (typeof firstFile === 'object' && firstFile !== null) {
        originalSourceVideo = firstFile.source_video;
        originalStart = firstFile.original_start;
        originalEnd = firstFile.original_end;
        if (originalStart != null && originalEnd != null) {
            originalDurationMs = (originalEnd - originalStart) * 1000; // Convert to milliseconds
        }
    }
    
    // Create waveform visualization using exported clip (fast, already trimmed)
    // But use original segment duration for trim calculations (accurate)
    const waveform = createWaveformVisualization(
        phraseContainer,  // container - MUST be first parameter
        450,              // initialStartTrim
        450,              // initialEndTrim
        originalDurationMs || 5000,  // maxTrim - use original segment duration for accurate trimming
        (startTrim, endTrim) => {  // onTrimChange callback
            startSlider.value = startTrim;
            endSlider.value = endTrim;
            // Don't autoplay during dragging - only on release
        },
        initialVideo,     // videoPath - use exported clip for waveform (fast loading)
        1.0,              // initialSpeed
        async (startTrim, endTrim) => {  // onTrimRelease callback - re-render and play
            startSlider.value = startTrim;
            endSlider.value = endTrim;
            
            console.log('Trim release - re-rendering with:', { startTrim, endTrim });
            
            // Re-render clip with new trim values from original source
            const newClipPath = await rerenderClipWithNewTrims(phraseContainer, startTrim, endTrim);
            if (newClipPath && newClipPath.trim() !== '') {
                // Update the select element with the new clip path
                const selectedOption = listbox.options[listbox.selectedIndex];
                if (selectedOption) {
                    // Store the re-rendered clip path
                    selectedOption.value = newClipPath;
                    listbox.value = newClipPath;
                    // Mark this option as re-rendered so we know not to apply trimming
                    selectedOption.dataset.rerendered = 'true';
                }
                
                // Don't update waveform - it should stay on the original exported clip
                // The waveform represents the original segment, trims are just boundaries
                
                // Play the newly rendered clip (no trims needed, it's already trimmed)
                playVideoWithTrimInFloatingPreview(newClipPath, 0, 0, phraseContainer, listbox);
            } else {
                // Fallback: play with JavaScript trimming if re-render failed
                console.warn('Re-render failed or returned empty path, falling back to JavaScript trimming');
                playTrimmedVideo(phraseContainer);
            }
        }
    );
    
    // Set the clip duration to the original segment duration for accurate trim calculations
    if (originalDurationMs) {
        waveform.setClipDuration(originalDurationMs);
    }
    phraseContainer.appendChild(waveform);
    waveformRef = waveform;

    // Track the currently selected video to detect actual changes
    // Use original clip path for comparison, not re-rendered paths
    let previousOriginalClipPath = null;
    if (listbox.options.length > 0 && listbox.selectedIndex >= 0) {
        const currentOption = listbox.options[listbox.selectedIndex];
        previousOriginalClipPath = currentOption?.dataset?.originalClipPath || currentOption?.value || listbox.value;
    }

    // Add change listener for when user selects a different match
    listbox.addEventListener('change', async (event) => {
        const selectedOption = listbox.options[listbox.selectedIndex];
        const originalClipPath = selectedOption?.dataset?.originalClipPath || selectedOption?.value || listbox.value;
        
        console.log('Listbox change event fired:', listbox.value);
        console.log('Original clip path:', originalClipPath);
        console.log('Previous original clip path:', previousOriginalClipPath);
        
        // Update floating preview when selection changes
        updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
        
        // Only reset and reload if the original clip path actually changed
        if (originalClipPath !== previousOriginalClipPath) {
            console.log('Selection changed - resetting trim and loading new video');
            
            // Reset trim values to default when match changes
            startSlider.value = '450';
            endSlider.value = '450';
            
            if (waveformRef) {
                console.log('Updating waveform for new selection...');
                waveformRef.updateTrim(450, 450);
                
                // Get original source video and segment boundaries for trim calculations
                const originalStart = parseFloat(selectedOption?.dataset?.originalStart);
                const originalEnd = parseFloat(selectedOption?.dataset?.originalEnd);
                
                if (!isNaN(originalStart) && !isNaN(originalEnd)) {
                    // Calculate original segment duration for accurate trim calculations
                    const originalDurationMs = (originalEnd - originalStart) * 1000;
                    waveformRef.setClipDuration(originalDurationMs);
                }
                
                // Load waveform from original exported clip (fast), not re-rendered version
                try {
                    const duration = await getVideoDuration(originalClipPath);
                    if (duration && (isNaN(originalStart) || isNaN(originalEnd))) {
                        // Use exported clip duration if original data not available
                        waveformRef.setClipDuration(duration);
                    }
                    waveformRef.loadNewWaveform(originalClipPath);
                } catch (err) {
                    console.warn('Could not load video duration on match change:', err);
                }
            }
            
            // Update previous selection to track original clip path
            previousOriginalClipPath = originalClipPath;
            
            // Re-render and play the selected match on first selection
            // This ensures the clip is rendered with current trim values before first playback
            console.log('Re-rendering and playing selected match...');
            const currentStartTrim = parseInt(startSlider.value, 10);
            const currentEndTrim = parseInt(endSlider.value, 10);
            const rerenderedPath = await rerenderClipWithNewTrims(phraseContainer, currentStartTrim, currentEndTrim);
            if (rerenderedPath && rerenderedPath.trim() !== '') {
                const selectedOption = listbox.options[listbox.selectedIndex];
                if (selectedOption) {
                    selectedOption.value = rerenderedPath;
                    listbox.value = rerenderedPath;
                    selectedOption.dataset.rerendered = 'true';
                }
                // Play without trimming (already trimmed)
                playVideoWithTrimInFloatingPreview(rerenderedPath, 0, 0, phraseContainer, listbox);
            } else {
                // Fallback: play with JavaScript trimming
                playTrimmedVideo(phraseContainer);
            }
        } else {
            console.log('Same selection - skipping reset and re-render');
            // Don't re-render or play again if same selection - let click handler handle it
        }
    });
    
    // Also add click listener as fallback (but don't trigger change if already selected)
    listbox.addEventListener('click', async (event) => {
        if (event.target.tagName === 'OPTION') {
            const clickedOption = event.target;
            const clickedOriginalPath = clickedOption?.dataset?.originalClipPath || clickedOption?.value;
            console.log('Option clicked:', clickedOption.value);
            console.log('Clicked original path:', clickedOriginalPath);
            console.log('Previous original path:', previousOriginalClipPath);
            
            // Only trigger change event if the original clip path is actually different
            if (clickedOriginalPath !== previousOriginalClipPath) {
                const changeEvent = new Event('change', { bubbles: true });
                listbox.dispatchEvent(changeEvent);
            } else {
                // Just play the video without resetting
                // Check if it's already re-rendered
                const selectedOption = listbox.options[listbox.selectedIndex];
                const isRerendered = selectedOption?.dataset?.rerendered === 'true';
                if (isRerendered) {
                    // Play without trimming (already trimmed)
                    playVideoWithTrimInFloatingPreview(listbox.value, 0, 0, phraseContainer, listbox);
                } else {
                    // Re-render with current trim values before first playback
                    const currentStartTrim = parseInt(startSlider.value, 10);
                    const currentEndTrim = parseInt(endSlider.value, 10);
                    const rerenderedPath = await rerenderClipWithNewTrims(phraseContainer, currentStartTrim, currentEndTrim);
                    if (rerenderedPath && rerenderedPath.trim() !== '') {
                        selectedOption.value = rerenderedPath;
                        listbox.value = rerenderedPath;
                        selectedOption.dataset.rerendered = 'true';
                        // Play without trimming (already trimmed)
                        playVideoWithTrimInFloatingPreview(rerenderedPath, 0, 0, phraseContainer, listbox);
                    } else {
                        playTrimmedVideo(phraseContainer);
                    }
                }
            }
        }
        updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
    });

    // Load initial video duration
    if (segmentData.files[0]) {
        const firstFile = segmentData.files[0];
        const firstVideo = (typeof firstFile === 'object') ? firstFile.file : firstFile;
        
        // Use server-provided duration if available, otherwise fetch from video metadata
        if (typeof firstFile === 'object' && firstFile.duration_ms) {
            console.log(`Using server-provided duration: ${firstFile.duration_ms}ms`);
            if (waveformRef) {
                waveformRef.setClipDuration(firstFile.duration_ms);
            }
        } else {
            getVideoDuration(firstVideo).then(duration => {
                if (waveformRef) {
                    waveformRef.setClipDuration(duration);
                }
            }).catch(err => {
                console.warn('Could not load initial video duration:', err);
            });
        }
    }

    const controls = document.createElement('div');
    controls.classList.add('phrase-controls');

    const playButton = document.createElement('button');
    playButton.textContent = 'Play';
    playButton.classList.add('play-button');
    playButton.addEventListener('click', () => playTrimmedVideo(phraseContainer));
    controls.appendChild(playButton);

    const addButton = document.createElement('button');
    addButton.textContent = 'Add to Timeline';
    addButton.classList.add('add-button');
    addButton.addEventListener('click', () => handleAddContainerToTimeline(phraseContainer));
    controls.appendChild(addButton);

    const addAllButton = document.createElement('button');
    addAllButton.textContent = 'Add All Variants';
    addAllButton.classList.add('add-all-button');
    addAllButton.addEventListener('click', () => handleAddAllVariantsToTimeline(phraseContainer));
    controls.appendChild(addAllButton);

    phraseContainer.appendChild(controls);

    // Append to results container
    elements.resultsContainer.appendChild(phraseContainer);
    
    // Initialize floating preview if not already created
    initializeFloatingPreview();
}

function initializeFloatingPreview() {
    if (floatingPreviewContainer) {
        return; // Already initialized
    }
    
    // Create floating preview container
    floatingPreviewContainer = document.createElement('div');
    floatingPreviewContainer.id = 'floatingPreview';
    floatingPreviewContainer.classList.add('floating-preview');
    floatingPreviewContainer.style.display = 'none';
    
    // Create preview content container
    const previewContent = document.createElement('div');
    previewContent.classList.add('floating-preview-content');
    
    // Create video element for preview
    const previewVideo = document.createElement('video');
    previewVideo.classList.add('floating-preview-video');
    previewVideo.controls = true;
    previewVideo.muted = false;
    
    // Create title
    const previewTitle = document.createElement('h4');
    previewTitle.classList.add('floating-preview-title');
    
    previewContent.appendChild(previewTitle);
    previewContent.appendChild(previewVideo);
    floatingPreviewContainer.appendChild(previewContent);
    
    // Append to body
    document.body.appendChild(floatingPreviewContainer);
}

function updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider) {
    if (!floatingPreviewContainer) {
        initializeFloatingPreview();
    }
    
    if (!phraseContainer || !listbox) {
        return;
    }
    
    const selectedVideo = listbox.value;
    if (!selectedVideo) {
        floatingPreviewContainer.style.display = 'none';
        return;
    }
    
    // Check if mobile (using same breakpoint as CSS: 1024px)
    const isMobile = window.innerWidth <= 1024;
    
    if (isMobile) {
        // On mobile, position at top with padding
        floatingPreviewContainer.style.left = '16px';
        floatingPreviewContainer.style.right = '16px';
        floatingPreviewContainer.style.top = '16px';
        floatingPreviewContainer.style.width = 'calc(100% - 32px)';
    } else {
        // Desktop positioning - get card position
        const cardRect = phraseContainer.getBoundingClientRect();
        const containerRect = elements.resultsContainer.getBoundingClientRect();
        
        // Position preview to the right of the card
        const previewWidth = 400; // Fixed width for preview
        const gap = 20; // Gap between card and preview
        let left = cardRect.right + gap;
        let top = cardRect.top;
        
        // Check if preview would go off screen to the right
        if (left + previewWidth > window.innerWidth) {
            // Position to the left instead
            left = cardRect.left - previewWidth - gap;
        }
        
        // Check if preview would go off screen to the left
        if (left < 0) {
            left = gap;
        }
        
        // Check if preview would go off screen at the bottom
        const previewHeight = 500; // Estimated height
        if (top + previewHeight > window.innerHeight) {
            top = window.innerHeight - previewHeight - gap;
        }
        
        // Check if preview would go off screen at the top
        if (top < 0) {
            top = gap;
        }
        
        floatingPreviewContainer.style.left = `${left}px`;
        floatingPreviewContainer.style.top = `${top}px`;
        floatingPreviewContainer.style.right = 'auto';
        floatingPreviewContainer.style.width = '400px';
    }
    floatingPreviewContainer.style.display = 'block';
    floatingPreviewContainer.style.pointerEvents = 'auto'; // Ensure pointer events are enabled when showing
    
    // Update preview content
    const previewTitle = floatingPreviewContainer.querySelector('.floating-preview-title');
    const previewVideo = floatingPreviewContainer.querySelector('.floating-preview-video');
    
    // Update title with "match x: phrase" format and matched text
    const phraseTitle = phraseContainer.querySelector('.phrase-title');
    if (phraseTitle && previewTitle && listbox) {
        const selectedOption = listbox.options[listbox.selectedIndex];
        const matchNumber = listbox.selectedIndex + 1;
        const phraseText = phraseTitle.textContent;
        
        // Get the matched text from the selected file object
        let matchedText = '';
        const selectedVideo = listbox.value;
        const segmentData = phraseContainer.dataset.segmentData ? JSON.parse(phraseContainer.dataset.segmentData) : null;
        
        if (segmentData && segmentData.files) {
            const selectedFile = segmentData.files.find(f => {
                const filePath = (typeof f === 'object' && f !== null) ? f.file : f;
                return filePath === selectedVideo;
            });
            
            if (selectedFile && typeof selectedFile === 'object') {
                // Check for various possible fields that might contain the matched text
                matchedText = selectedFile.transcript || 
                             selectedFile.text || 
                             selectedFile.matched_text || 
                             selectedFile.matchedText ||
                             selectedFile.segment ||
                             '';
            }
        }
        
        // Build title with matched text in hover tooltip
        let titleHtml = '';
        if (matchedText) {
            // Wrap "Match N:" in a span with tooltip containing the matched text
            titleHtml = `<span class="match-number-tooltip" title="Matched: &quot;${escapeHtml(matchedText)}&quot;">Match ${matchNumber}:</span> ${escapeHtml(phraseText)}`;
        } else {
            titleHtml = `Match ${matchNumber}: ${escapeHtml(phraseText)}`;
        }
        previewTitle.innerHTML = titleHtml;
    }
    
    // Update video (but don't auto-play, let user control it)
    if (previewVideo) {
        const videoUrl = getVideoUrl(selectedVideo);
        if (previewVideo.src !== videoUrl) {
            previewVideo.src = videoUrl;
        }
    }
    
    // Store reference to current selected card
    currentSelectedCard = phraseContainer;
}

function playVideoWithTrimInFloatingPreview(filePath, startTrim, endTrim, phraseContainer, selectElement) {
    if (!floatingPreviewContainer) {
        initializeFloatingPreview();
    }
    
    const previewVideo = floatingPreviewContainer.querySelector('.floating-preview-video');
    if (!previewVideo) {
        console.error('Floating preview video element not found');
        return;
    }
    
    // Make sure preview is visible
    if (floatingPreviewContainer.style.display === 'none') {
        updateFloatingPreview(phraseContainer, selectElement, null, null, null);
    }
    
    clearTimeout(videoTimeoutId);
    
    const videoUrl = getVideoUrl(filePath);
    previewVideo.src = videoUrl;
    previewVideo.load();
    
    // Remove any existing event listeners to prevent multiple handlers
    previewVideo.onloadedmetadata = null;
    previewVideo.onseeked = null;
    
    previewVideo.onloadedmetadata = () => {
        const duration = previewVideo.duration;
        const startSeconds = startTrim / 1000;
        const endSeconds = endTrim / 1000;
        
        // Validate that trim values don't exceed clip duration
        if (startSeconds + endSeconds >= duration) {
            console.error(`Trim values (start: ${startTrim}ms, end: ${endTrim}ms) exceed clip duration (${(duration * 1000).toFixed(0)}ms). Nothing to play.`);
            alert(`Cannot play: trim values exceed clip duration.\nClip: ${(duration * 1000).toFixed(0)}ms\nStart trim: ${startTrim}ms\nEnd trim: ${endTrim}ms\nRemaining: ${((duration * 1000) - startTrim - endTrim).toFixed(0)}ms`);
            return;
        }
        
        // Set up seeked handler to ensure seek completes before playing
        let seekTimeoutId = null;
        let playbackStarted = false;
        
        const startPlayback = () => {
            if (playbackStarted) return;
            playbackStarted = true;
            
            // Clear seek timeout if it exists
            if (seekTimeoutId) {
                clearTimeout(seekTimeoutId);
                seekTimeoutId = null;
            }
            
            // Set up end trim timeout if needed
            if (endTrim > 0) {
                const endTime = duration - endSeconds;
                videoTimeoutId = setTimeout(() => {
                    previewVideo.pause();
                }, (endTime - startSeconds) * 1000);
            }
            
            // Start playback only after seek completes
            previewVideo.play().catch(error => console.error('Error playing the video:', error));
        };
        
        previewVideo.onseeked = () => {
            // Clear the seeked handler to prevent it from firing multiple times
            previewVideo.onseeked = null;
            startPlayback();
        };
        
        // Set currentTime - this will trigger the seeked event
        previewVideo.currentTime = startSeconds;
        
        // Fallback: if seeked doesn't fire within 200ms, start playback anyway
        // This handles edge cases where seeked might not fire (e.g., if already at target time)
        seekTimeoutId = setTimeout(() => {
            if (!playbackStarted) {
                previewVideo.onseeked = null;
                startPlayback();
            }
        }, 200);
    };
}

// Hide floating preview when clicking outside
document.addEventListener('click', (event) => {
    if (floatingPreviewContainer && floatingPreviewContainer.style.display !== 'none') {
        // Check if click is outside the preview and outside any phrase container
        const clickedElement = event.target;
        const isClickInsidePreview = floatingPreviewContainer.contains(clickedElement);
        const isClickInsideCard = clickedElement.closest('.phrase-container');
        
        if (!isClickInsidePreview && !isClickInsideCard) {
            floatingPreviewContainer.style.display = 'none';
            currentSelectedCard = null;
        }
    }
});

// Hide floating preview on scroll (optional - can be removed if too aggressive)
let scrollTimeout = null;
window.addEventListener('scroll', () => {
    if (floatingPreviewContainer && floatingPreviewContainer.style.display !== 'none') {
        // Hide preview on scroll, but only after a short delay to avoid flickering
        clearTimeout(scrollTimeout);
        scrollTimeout = setTimeout(() => {
            if (floatingPreviewContainer) {
                floatingPreviewContainer.style.display = 'none';
            }
        }, 150);
    }
}, { passive: true });

// Move floating preview away when mouse hovers over cards behind it
document.addEventListener('mousemove', (event) => {
    if (!floatingPreviewContainer || floatingPreviewContainer.style.display === 'none' || !elements.resultsContainer) {
        return;
    }
    
    const mouseX = event.clientX;
    const mouseY = event.clientY;
    
    // Check all phrase containers to see if mouse is over one
    const allCards = elements.resultsContainer.querySelectorAll('.phrase-container');
    const previewRect = floatingPreviewContainer.getBoundingClientRect();
    
    for (const card of allCards) {
        // Skip the currently selected card
        if (card === currentSelectedCard) {
            continue;
        }
        
        const cardRect = card.getBoundingClientRect();
        
        // Check if mouse is over this card
        const isMouseOverCard = (
            mouseX >= cardRect.left &&
            mouseX <= cardRect.right &&
            mouseY >= cardRect.top &&
            mouseY <= cardRect.bottom
        );
        
        // Check if preview is overlapping with this card
        const isOverlapping = !(
            previewRect.right < cardRect.left ||
            previewRect.left > cardRect.right ||
            previewRect.bottom < cardRect.top ||
            previewRect.top > cardRect.bottom
        );
        
        // If mouse is over a card and preview is blocking it, move preview away immediately
        if (isMouseOverCard && isOverlapping) {
            const previewWidth = 400;
            const gap = 20;
            const previewHeight = previewRect.height || 500;
            
            // Try positioning to the right first
            let newLeft = cardRect.right + gap;
            let newTop = cardRect.top;
            
            // If that would go off screen, try left
            if (newLeft + previewWidth > window.innerWidth) {
                newLeft = cardRect.left - previewWidth - gap;
            }
            
            // If that would go off screen, try below
            if (newLeft < 0) {
                newLeft = cardRect.left;
                newTop = cardRect.bottom + gap;
            }
            
            // If that would go off screen, try above
            if (newTop + previewHeight > window.innerHeight) {
                newTop = cardRect.top - previewHeight - gap;
            }
            
            // Ensure preview stays on screen
            if (newLeft < 0) newLeft = gap;
            if (newLeft + previewWidth > window.innerWidth) {
                newLeft = window.innerWidth - previewWidth - gap;
            }
            if (newTop < 0) newTop = gap;
            if (newTop + previewHeight > window.innerHeight) {
                newTop = window.innerHeight - previewHeight - gap;
            }
            
            // Temporarily allow pointer events to pass through while moving
            floatingPreviewContainer.style.pointerEvents = 'none';
            
            // Move immediately without transition
            floatingPreviewContainer.style.transition = 'none';
            floatingPreviewContainer.style.left = `${newLeft}px`;
            floatingPreviewContainer.style.top = `${newTop}px`;
            
            // Re-enable pointer events after a brief moment
            setTimeout(() => {
                if (floatingPreviewContainer) {
                    floatingPreviewContainer.style.pointerEvents = 'auto';
                }
            }, 50);
            
            break; // Only move once per check
        }
    }
}, { passive: true });

function updateDropdowns(data) {
    if (!Array.isArray(data)) {
        console.error('Received data is not an array:', data);
        return;
    }

    if (!elements.resultsContainer) {
        return;
    }

    globalPhraseContainerIDCounter += 1;

    data.forEach((phraseData, index) => {
        const phraseContainer = document.createElement('div');
        phraseContainer.id = `phraseContainer${globalPhraseContainerIDCounter}_${index}`;
        phraseContainer.classList.add('phrase-container');
        phraseContainer.dataset.counter = String(searchCounter);

        const title = document.createElement('h4');
        title.textContent = phraseData.phrase;
        title.classList.add('phrase-title');
        title.setAttribute('draggable', 'true');
        phraseContainer.appendChild(title);
        registerSearchContainerDrag(title, phraseContainer);

        const listbox = document.createElement('select');
        listbox.id = `phrase${index}`;
        listbox.size = 5;
        
        // Store waveform reference for updating when match changes
        let waveformRef = null;
        
        listbox.addEventListener('change', () => {
            const selectedVideo = listbox.value;
            const selectedOption = listbox.options[listbox.selectedIndex];

            // Default trims
            let startTrimMs = 450;
            let endTrimMs = 450;

            // If this option represents a silence entry, we exported a clip
            // that already spans exactly the silence region. In that case we
            // want trims relative to the exported clip itself (0 at the
            // beginning), so we default to 0/0 instead of trying to map the
            // original silence_start/silence_end against the full source
            // video duration.
            const silenceStart = selectedOption && selectedOption.dataset.silenceStart
                ? parseFloat(selectedOption.dataset.silenceStart)
                : null;
            const silenceEnd = selectedOption && selectedOption.dataset.silenceEnd
                ? parseFloat(selectedOption.dataset.silenceEnd)
                : null;

            if (waveformRef && selectedVideo) {
                // Load new video duration and waveform, then adjust trims
                // Use server-provided duration if available
                const serverDuration = selectedOption?.dataset?.durationMs ? parseInt(selectedOption.dataset.durationMs) : null;
                
                const durationPromise = serverDuration 
                    ? Promise.resolve(serverDuration)
                    : getVideoDuration(selectedVideo);
                
                durationPromise.then(durationMs => {
                    console.log(serverDuration ? 'Using server-provided duration:' : 'Using browser metadata duration:', durationMs, 'ms');
                    waveformRef.setClipDuration(durationMs);

                    // For silence clips we exported only the silence window
                    // itself, so treat the entire clip as the region of
                    // interest and leave trims at 0/0.
                    if (silenceStart != null && silenceEnd != null && durationMs > 0) {
                        startTrimMs = 0;
                        endTrimMs = 0;
                    }

                    startSlider.value = String(startTrimMs);
                    endSlider.value = String(endTrimMs);
                    waveformRef.updateTrim(startTrimMs, endTrimMs);
                    playTrimmedVideo(phraseContainer);
                    updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
                }).catch(err => {
                    console.warn('Could not load video duration on match change:', err);
                    // Fallback: use default trims
                    startSlider.value = String(startTrimMs);
                    endSlider.value = String(endTrimMs);
                    if (waveformRef) {
                        waveformRef.updateTrim(startTrimMs, endTrimMs);
                    }
                    playTrimmedVideo(phraseContainer);
                    updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
                });

                // Load real waveform for new clip
                waveformRef.loadNewWaveform(selectedVideo);
            } else {
                // No waveform available; still keep basic behavior
                startSlider.value = String(startTrimMs);
                endSlider.value = String(endTrimMs);
                playTrimmedVideo(phraseContainer);
                updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
            }
        });
        listbox.addEventListener('click', () => {
            playTrimmedVideo(phraseContainer);
            updateFloatingPreview(phraseContainer, listbox, waveformRef, startSlider, endSlider);
        });

        phraseData.files.forEach((file, fileIndex) => {
            const option = document.createElement('option');
            
            // Handle both new format (object with metadata) and old format (string)
            if (typeof file === 'object' && file !== null) {
                option.value = file.file;
                const sourceVideo = file.source_video || 'Unknown';
                const videoName = sourceVideo.split('/').pop(); // Get filename from path

                // Get duration_ms from server - this is the exported clip duration
                // We need to subtract default trim values to get actual playable length
                let clipDurationMs = file.duration_ms;
                // Handle string numbers
                if (clipDurationMs != null && typeof clipDurationMs === 'string') {
                    clipDurationMs = parseFloat(clipDurationMs);
                }
                // For silence searches, clips are exported with 0/0 trims, so use full duration
                // For regular searches, default trims are 450ms start + 450ms end
                const defaultStartTrim = (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') ? 0 : 450;
                const defaultEndTrim = (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') ? 0 : 450;
                
                // Fallback: calculate from silence times if available
                if ((!clipDurationMs || isNaN(clipDurationMs) || clipDurationMs <= 0) && typeof file.silence_start === 'number' && typeof file.silence_end === 'number') {
                    clipDurationMs = (file.silence_end - file.silence_start) * 1000;
                }
                const trimmedLength = formatTrimmedLength(clipDurationMs, defaultStartTrim, defaultEndTrim);
                
                // If this is a silence entry, show a more descriptive label
                if (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') {
                    const durationSec = Math.max(0, file.silence_end - file.silence_start);
                    const before = file.word_before || '';
                    const after = file.word_after || '';
                    const context = before || after
                        ? ` between "${before || '…'}" and "${after || '…'}"`
                        : '';
                    option.text = `Silence ${durationSec.toFixed(2)}s (${trimmedLength}${videoName})${context}`;

                    // Store metadata for potential future trim/playback logic
                    option.dataset.silenceStart = String(file.silence_start);
                    option.dataset.silenceEnd = String(file.silence_end);
                    if (before) option.dataset.wordBefore = before;
                    if (after) option.dataset.wordAfter = after;
                } else {
                    option.text = `Match ${fileIndex + 1} (${trimmedLength}${videoName})`;
                }
                
                // Store duration if provided, or try to load it asynchronously
                if (clipDurationMs != null && !isNaN(clipDurationMs) && clipDurationMs > 0) {
                    option.dataset.durationMs = String(clipDurationMs);
                } else {
                    // Try to load duration asynchronously from video metadata
                    const videoPath = file.file;
                    if (videoPath) {
                        getVideoDuration(videoPath).then(duration => {
                            if (duration && duration > 0) {
                                option.dataset.durationMs = String(duration);
                                const updatedTrimmedLength = formatTrimmedLength(duration, defaultStartTrim, defaultEndTrim);
                                // Update the option text with the loaded duration
                                if (typeof file.silence_start === 'number' && typeof file.silence_end === 'number') {
                                    const durationSec = Math.max(0, file.silence_end - file.silence_start);
                                    const before = file.word_before || '';
                                    const after = file.word_after || '';
                                    const context = before || after
                                        ? ` between "${before || '…'}" and "${after || '…'}"`
                                        : '';
                                    option.text = `Silence ${durationSec.toFixed(2)}s (${updatedTrimmedLength}${videoName})${context}`;
                                } else {
                                    option.text = `Match ${fileIndex + 1} (${updatedTrimmedLength}${videoName})`;
                                }
                            }
                        }).catch(() => {
                            // Ignore errors - duration will remain unknown
                        });
                    }
                }

                // Store duration if provided by server
                if (typeof file.duration_ms === 'number') {
                    option.dataset.durationMs = String(file.duration_ms);
                }

                option.title = sourceVideo; // Show full path on hover
                // Store the original exported clip path for waveform (before any re-rendering)
                if (file.file) {
                    option.dataset.originalClipPath = file.file;
                }
                // Store original segment boundaries for re-rendering
                if (file.original_start != null) {
                    option.dataset.originalStart = String(file.original_start);
                }
                if (file.original_end != null) {
                    option.dataset.originalEnd = String(file.original_end);
                }
            } else {
            option.value = file;
            option.text = `Match ${fileIndex + 1}`;
            }
            
            listbox.appendChild(option);
        });
        phraseContainer.appendChild(listbox);

        // Create sliders first (hidden, for data storage and precise control)
        const startSlider = document.createElement('input');
        const startSliderDisplay = document.createElement('div');
        startSlider.type = 'range';
        startSlider.min = '0';
        startSlider.max = '8000';
        startSlider.value = '450';
        startSlider.className = 'start-trim-slider';
        startSlider.style.display = 'none'; // Hidden by default
        startSliderDisplay.textContent = `start trim ${startSlider.value} ms`;
        startSliderDisplay.style.display = 'none';
        phraseContainer.appendChild(startSlider);
        phraseContainer.appendChild(startSliderDisplay);

        const endSlider = document.createElement('input');
        const endSliderDisplay = document.createElement('div');
        endSlider.type = 'range';
        endSlider.min = '0';
        endSlider.max = '8000';
        endSlider.value = '450';
        endSlider.className = 'end-trim-slider';
        endSlider.style.display = 'none'; // Hidden by default
        endSliderDisplay.textContent = `end trim ${endSlider.value} ms`;
        endSliderDisplay.style.display = 'none';
        phraseContainer.appendChild(endSlider);
        phraseContainer.appendChild(endSliderDisplay);

        // Get original source video and segment boundaries for re-rendering
        // Use exported clip for waveform (faster), but track original segment for accurate trimming
        const firstFile = phraseData.files && phraseData.files.length > 0 ? phraseData.files[0] : null;
        let originalSourceVideo = null;
        let originalStart = null;
        let originalEnd = null;
        let originalDurationMs = null;
        const initialVideo = firstFile && typeof firstFile === 'object' ? firstFile.file : (firstFile || null);
        
        if (firstFile && typeof firstFile === 'object' && firstFile !== null) {
            originalSourceVideo = firstFile.source_video;
            originalStart = firstFile.original_start;
            originalEnd = firstFile.original_end;
            if (originalStart != null && originalEnd != null) {
                originalDurationMs = (originalEnd - originalStart) * 1000; // Convert to milliseconds
            }
        }
        
        // Create waveform visualization using exported clip (fast, already trimmed)
        // But use original segment duration for trim calculations (accurate)
        const waveform = createWaveformVisualization(
            phraseContainer,
            450, // initial start trim
            450, // initial end trim
            originalDurationMs || 8000, // max trim - use original segment duration for accurate trimming
            (startTrim, endTrim) => {
                // Update sliders when waveform changes
                startSlider.value = startTrim;
                startSliderDisplay.textContent = `start trim ${startTrim} ms`;
                endSlider.value = endTrim;
                endSliderDisplay.textContent = `end trim ${endTrim} ms`;
                // Don't autoplay during dragging - only on release
            },
            initialVideo, // Use exported clip for waveform (fast loading)
            1.0, // initialSpeed
            async (startTrim, endTrim) => {  // onTrimRelease callback - re-render and play
                startSlider.value = startTrim;
                startSliderDisplay.textContent = `start trim ${startTrim} ms`;
                endSlider.value = endTrim;
                endSliderDisplay.textContent = `end trim ${endTrim} ms`;
                
                // Re-render clip with new trim values from original source
                const newClipPath = await rerenderClipWithNewTrims(phraseContainer, startTrim, endTrim);
                if (newClipPath && newClipPath.trim() !== '') {
                    // Update the select element with the new clip path
                    const selectedOption = listbox.options[listbox.selectedIndex];
                    if (selectedOption) {
                        selectedOption.value = newClipPath;
                        listbox.value = newClipPath;
                        selectedOption.dataset.rerendered = 'true';
                    }
                    
                    // Don't update waveform - it should stay on the original exported clip
                    // The waveform represents the original segment, trims are just boundaries
                    
                    // Play the newly rendered clip (no trims needed, it's already trimmed)
                    playVideoWithTrimInFloatingPreview(newClipPath, 0, 0, phraseContainer, listbox);
                } else {
                    // Fallback: play with JavaScript trimming if re-render failed
                    playTrimmedVideo(phraseContainer);
                }
            }
        );
        
        // Set the clip duration to the original segment duration for accurate trim calculations
        if (originalDurationMs) {
            waveform.setClipDuration(originalDurationMs);
        }
        waveformRef = waveform; // Store reference for match change updates
        phraseContainer.appendChild(waveform);

        // Connect sliders to waveform (for programmatic updates)
        const isMobile = window.innerWidth <= 1024;
        
        // On mobile, use 'change' event (fires on release), on desktop use 'input' (fires during drag)
        const sliderEvent = isMobile ? 'change' : 'input';
        
        startSlider.addEventListener(sliderEvent, function () {
            const value = parseInt(this.value, 10);
            startSliderDisplay.textContent = `start trim ${value} ms`;
            waveform.updateTrim(value, parseInt(endSlider.value, 10));
            playTrimmedVideo(phraseContainer);
        });

        endSlider.addEventListener(sliderEvent, function () {
            const value = parseInt(this.value, 10);
            endSliderDisplay.textContent = `end trim ${value} ms`;
            waveform.updateTrim(parseInt(startSlider.value, 10), value);
            playTrimmedVideo(phraseContainer);
        });
        
        // On mobile, also update display during input (but don't play)
        if (isMobile) {
            startSlider.addEventListener('input', function () {
                const value = parseInt(this.value, 10);
                startSliderDisplay.textContent = `start trim ${value} ms`;
                waveform.updateTrim(value, parseInt(endSlider.value, 10), true); // Skip callback
            });

            endSlider.addEventListener('input', function () {
                const value = parseInt(this.value, 10);
                endSliderDisplay.textContent = `end trim ${value} ms`;
                waveform.updateTrim(parseInt(startSlider.value, 10), value, true); // Skip callback
            });
        }

        const controls = document.createElement('div');
        controls.classList.add('phrase-controls');

        const playButton = document.createElement('button');
        playButton.type = 'button';
        playButton.textContent = 'Play';
        playButton.classList.add('play-button');
        playButton.addEventListener('click', () => playTrimmedVideo(phraseContainer));
        controls.appendChild(playButton);

        const addToTimelineButton = document.createElement('button');
        addToTimelineButton.type = 'button';
        addToTimelineButton.textContent = 'Add to Timeline';
        addToTimelineButton.classList.add('add-button');
        addToTimelineButton.addEventListener('click', () => handleAddContainerToTimeline(phraseContainer));
        controls.appendChild(addToTimelineButton);

        const addAllVariantsButton = document.createElement('button');
        addAllVariantsButton.type = 'button';
        addAllVariantsButton.textContent = 'Add All Variants';
        addAllVariantsButton.classList.add('add-all-button');
        addAllVariantsButton.addEventListener('click', () => handleAddAllVariantsToTimeline(phraseContainer));
        controls.appendChild(addAllVariantsButton);

        phraseContainer.appendChild(controls);

        elements.resultsContainer.appendChild(phraseContainer);

        // For grouped silence search results, auto-select the first option so
        // the user immediately sees a meaningful waveform/preview.
        if (listbox.options.length > 0) {
            listbox.selectedIndex = 0;
            listbox.dispatchEvent(new Event('change'));
        }
    });
}

function duplicateContainer(container) {
    if (!container) {
        return;
    }
    const phraseData = {
        phrase: container.querySelector('h4').textContent,
        files: Array.from(container.querySelector('select').options).map(option => {
            // Preserve the source_video info if available
            if (option.title) {
                return {
                    file: option.value,
                    source_video: option.title
                };
            }
            return option.value;
        })
    };
    updateDropdowns([phraseData]);
}

function moveContainer(container, direction) {
    const sibling = direction === 'left' ? container.previousElementSibling : container.nextElementSibling;
    if (!sibling) {
        return;
    }

    const containerWidth = container.offsetWidth;
    const siblingWidth = sibling.offsetWidth;

    if (direction === 'left') {
        container.parentNode.insertBefore(container, sibling);
    } else {
        container.parentNode.insertBefore(container, sibling.nextSibling);
    }

    container.style.transform = `translateX(${direction === 'left' ? -siblingWidth : siblingWidth}px)`;
    sibling.style.transform = `translateX(${direction === 'left' ? containerWidth : -containerWidth}px)`;

    setTimeout(() => {
        container.style.transform = '';
        sibling.style.transform = '';
    }, 300);
}

function removePhrase(phraseContainerId) {
    const phraseContainer = document.getElementById(phraseContainerId);
    if (phraseContainer && phraseContainer.parentNode) {
        phraseContainer.parentNode.removeChild(phraseContainer);
    }
}

async function handleLoadSentences() {
    try {
        const total = await loadSentences();
        if (elements.loadSentencesButton) {
            const label = total ? `${CORPUS_RELOAD_BUTTON_TEXT} (${total})` : `${CORPUS_RELOAD_BUTTON_TEXT} (empty)`;
            elements.loadSentencesButton.textContent = label;
            elements.loadSentencesButton.disabled = false;
        }
    } catch (error) {
        console.error('Failed to load sentences', error);
        if (elements.loadSentencesButton) {
            elements.loadSentencesButton.textContent = CORPUS_DEFAULT_BUTTON_TEXT;
            elements.loadSentencesButton.disabled = false;
        }
    }
}

function buildCombinedTranscriptions() {
    // Group sentences by source file and combine them into one long transcription
    combinedTranscriptions = {};
    
    const fileSegments = {}; // Map of file -> array of segments in order
    
    // Group segments by file
    sentences.forEach(entry => {
        if (!entry || !entry.source) return;
        const file = entry.source;
        if (!fileSegments[file]) {
            fileSegments[file] = [];
        }
        // Store the current segment
        if (entry.current) {
            fileSegments[file].push(entry.current);
        }
    });
    
    // Combine segments for each file into one long transcription
    for (const [file, segments] of Object.entries(fileSegments)) {
        // Join all segments with spaces to create one long transcription
        const combined = segments.join(' ');
        combinedTranscriptions[file] = {
            text: combined,
            normalized: combined.toLowerCase(),
            segments: segments // Keep original segments for reference
        };
    }
}

async function loadSentences() {
    const projectData = activeProject && typeof activeProject === 'object' ? activeProject.data : null;
    const selectedFiles = projectData && Array.isArray(projectData.selectedFiles) ? projectData.selectedFiles : [];
    const response = await fetch('/get_sentences', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ files: selectedFiles })
    });
    if (!response.ok) {
        throw new Error(`Failed to load sentences: ${response.status}`);
    }
    const data = await response.json();
    sentences = Array.isArray(data.sentences)
        ? data.sentences.map(normalizeCorpusEntry).filter(Boolean)
        : [];
    buildCombinedTranscriptions(); // Build combined transcriptions after loading
    updateAutocomplete();
    return sentences.length;
}

function updateAutocomplete() {
    if (!elements.inputText || !elements.suggestions) {
        return;
    }

    const inputValue = elements.inputText.value.trim();
    const normalizedInput = inputValue.toLowerCase();
    elements.suggestions.innerHTML = '';

    if (!normalizedInput) {
        return;
    }

    const MAX_SUGGESTIONS = 100;
    let count = 0;

    sentences.forEach(entry => {
        if (count >= MAX_SUGGESTIONS) {
            return;
        }
        if (!entry || !entry.currentNormalized) {
            return;
        }
        // Use includes for contains matching (match anywhere in sentence)
        if (!entry.currentNormalized.includes(normalizedInput)) {
            return;
        }
        const suggestion = document.createElement('div');
        suggestion.classList.add('suggestion-item');

        const prevHtml = entry.prev ? `<span class="context">${escapeHtml(entry.prev)}</span> ` : '';
        const nextHtml = entry.next ? ` <span class="context">${escapeHtml(entry.next)}</span>` : '';
        const highlightedCurrent = highlightPrefixMatch(entry.current, inputValue);
        
        // Add source file info if available
        let sourceHtml = '';
        if (entry.source) {
            const fileName = entry.source.split('/').pop() || entry.source;
            sourceHtml = `<span class="suggestion-source">${fileName}</span>`;
        }

        suggestion.innerHTML = `${prevHtml}${highlightedCurrent}${nextHtml}${sourceHtml}`;
        suggestion.addEventListener('click', () => selectSuggestion(entry.current));
        elements.suggestions.appendChild(suggestion);
        count++;
    });
}

function selectSuggestion(sentence) {
    if (!elements.inputText) {
        return;
    }
    elements.inputText.value = sentence;
    if (elements.suggestions) {
        elements.suggestions.innerHTML = '';
    }
    if (elements.sentenceInput) {
        elements.sentenceInput.value = sentence;
        updateProjectData(data => {
            data.currentSentence = sentence;
        });
    }
}

// Sentence autocomplete functions
function updateSentenceAutocomplete() {
    if (!elements.sentenceInput || !sentenceAutocompleteContainer || !sentences || sentences.length === 0) {
        hideSentenceAutocomplete();
        return;
    }

    // Only show autocomplete when cursor is at the end of the input
    // This allows users to edit in the middle without autocomplete interference
    const cursorPosition = elements.sentenceInput.selectionStart;
    const inputLength = elements.sentenceInput.value.length;
    if (cursorPosition !== inputLength) {
        hideSentenceAutocomplete();
        return;
    }

    // Snapshot of previous suggestions so we can reason about whether there
    // *used to be* valid continuations for the *current* prefix. This lets us
    // avoid auto-injecting a comma when the user is still on a valid path
    // (previous suggestions started with the current prefix), while still
    // allowing the helpful auto-comma once they move into a region with no
    // continuations at all.
    const previousSuggestions = Array.isArray(sentenceAutocompleteState.suggestions)
        ? sentenceAutocompleteState.suggestions
        : [];

    // Work on the active group (text after the last comma) so previous parts
    // of the sentence don't interfere with new completion cycles
    const fullInput = elements.sentenceInput.value;
    const lastSemicolonIndex = fullInput.lastIndexOf(';');
    const activeGroupRaw = lastSemicolonIndex === -1 ? fullInput : fullInput.slice(lastSemicolonIndex + 1);

    // Remember whether the user has finished a word with a trailing space.
    // Example: 'sicher ' (with space) should behave differently from 'sicher'.
    const hasTrailingSpace = /\s$/.test(activeGroupRaw);

    // We still match on the trimmed content itself, but keep hasTrailingSpace
    // to influence how aggressively we allow substring matches later.
    const inputValue = activeGroupRaw.trim();
    const normalizedInput = inputValue.toLowerCase();
    
    if (!normalizedInput) {
        hideSentenceAutocomplete();
        return;
    }

    // Get current accepted text (for Tab completion priority)
    const currentAccepted = sentenceAutocompleteState.currentAcceptedText.toLowerCase();

    // Determine if any of the *previous* suggestions actually started with
    // the current normalized input. If so, then there really are (or were)
    // continuations for this prefix, and auto-comma should not fire just
    // because the latest update produced zero suggestions (which might be a
    // transient or matching quirk).
    let previouslyHadPrefixMatch = false;
    if (previousSuggestions.length > 0 && normalizedInput) {
        for (const prev of previousSuggestions) {
            const text = (prev.currentNormalized || prev.current || '').toLowerCase();
            if (text.startsWith(normalizedInput)) {
                previouslyHadPrefixMatch = true;
                break;
            }
        }
    }
    
    // Check if current input matches a complete segment - if so, look for new segments starting with it
    const inputWords = normalizedInput.split(/\s+/);
    let isCompleteSegment = false;
    for (const entry of sentences) {
        if (entry && entry.currentNormalized === normalizedInput) {
            isCompleteSegment = true;
            break;
        }
    }
    
    // If input is a complete segment and differs from currentAccepted, reset to look for new segments
    if (isCompleteSegment && normalizedInput !== currentAccepted) {
        sentenceAutocompleteState.currentAcceptedText = inputValue;
    }
    
    // Get checkbox states
    const includePartialMatchesCheckbox = document.getElementById('includePartialMatches');
    const includePartialMatches = includePartialMatchesCheckbox ? includePartialMatchesCheckbox.checked : false;
    const allPartialMatchesCheckbox = document.getElementById('allPartialMatches');
    const allPartialMatches = allPartialMatchesCheckbox ? allPartialMatchesCheckbox.checked : false;
    
    // Filter and prioritize autocompletion suggestions
    // Use simple approach like updateAutocomplete - show all matches that contain input
    const MAX_SUGGESTIONS = 100;
    const suggestions = [];
    
    // Helper function to check if input matches at word boundaries
    // Always requires word boundary matching for the last word to avoid substring matches
    // (e.g., "euch" should match "euch" but not "beleuchten")
    function matchesAtWordBoundary(text, input, hasTrailingSpace) {
        const inputWords = input.split(/\s+/);
        const textWords = text.split(/\s+/);
        const lastInputWord = inputWords[inputWords.length - 1];
        
        // Check if input matches at word boundaries
        for (let i = 0; i <= textWords.length - inputWords.length; i++) {
            let matches = true;
            for (let j = 0; j < inputWords.length; j++) {
                const textWord = textWords[i + j];
                const inputWord = inputWords[j];
                
                if (j === inputWords.length - 1) {
                    // Last word: must start with input word (word boundary match)
                    // This prevents "euch" from matching "beleuchten"
                    if (!textWord.startsWith(inputWord)) {
                        matches = false;
                        break;
                    }
                    // If there's a trailing space, the word must be complete (exact match)
                    if (hasTrailingSpace && textWord !== inputWord) {
                        matches = false;
                        break;
                    }
                } else {
                    // Earlier words: must match exactly
                    if (textWord !== inputWord) {
                        matches = false;
                        break;
                    }
                }
            }
            if (matches) {
                return true;
            }
        }
        
        return false;
    }
    
    // Collect all matches that contain the input (like updateAutocomplete)
    const allMatches = [];
    const continuingSegments = [];
    
    sentences.forEach(entry => {
        if (!entry || !entry.current || !entry.currentNormalized) {
            return;
        }
        
        const segmentNormalized = entry.currentNormalized;
        
        // Priority: Segments that continue from current accepted text (for Tab chaining)
        if (currentAccepted && normalizedInput.startsWith(currentAccepted) && segmentNormalized.startsWith(currentAccepted)) {
            continuingSegments.push(entry);
        }
        // All other segments that match the input (with word boundary checking if trailing space)
        else if (matchesAtWordBoundary(segmentNormalized, normalizedInput, hasTrailingSpace)) {
            allMatches.push(entry);
        }
    });
    
    // Sort all matches: those starting with input first, then others
    // Within each group, sort by length (longest first)
    allMatches.sort((a, b) => {
        const aText = a.currentNormalized || a.current?.toLowerCase() || '';
        const bText = b.currentNormalized || b.current?.toLowerCase() || '';
        const aStarts = aText.startsWith(normalizedInput);
        const bStarts = bText.startsWith(normalizedInput);
        
        // Prioritize matches that start with input
        if (aStarts && !bStarts) return -1;
        if (!aStarts && bStarts) return 1;
        
        // Within same group, sort by length (longest first)
        return bText.length - aText.length;
    });
    
    // Deduplicate by continuation words for matches starting with input
    // This ensures "ich möchte", "ich kann", "ich bin" etc. are all shown as distinct options
    const seenNextWords = new Set();
    const deduplicatedMatches = [];
    
    for (const candidate of allMatches) {
        const text = candidate.currentNormalized || candidate.current?.toLowerCase() || '';
        const startsWithInput = text.startsWith(normalizedInput);
        
        if (startsWithInput) {
            // For matches starting with input, deduplicate by next words
            const words = text.split(/\s+/);
            const inputWordCount = normalizedInput.split(/\s+/).length;
            const continuationWords = words.slice(inputWordCount, inputWordCount + 3).join(' ');
            
            if (!seenNextWords.has(continuationWords) || continuationWords === '') {
                seenNextWords.add(continuationWords);
                deduplicatedMatches.push(candidate);
            }
        } else {
            // For matches containing input (but not starting with it), add all
            deduplicatedMatches.push(candidate);
        }
    }
    
    // Combine: continuing segments (Tab chaining) + all other matches
    const allCandidates = [...continuingSegments, ...deduplicatedMatches];
    
    // Limit to MAX_SUGGESTIONS
    for (let i = 0; i < Math.min(MAX_SUGGESTIONS, allCandidates.length); i++) {
        suggestions.push(allCandidates[i]);
    }

    // If no suggestions for a multi-word active group and we also did not
    // previously have any suggestions that start with the *current* prefix,
    // automatically close the group with a semicolon and restart autocomplete
    // from the last word/partial as a new group. This avoids injecting semicolons
    // while there are still valid completions available for the current
    // prefix, but preserves the helpful injection once the user really moves
    // into a dead-end like "sicher schwimmbäder und keine".
    // Skip if we just undid an auto-semicolon (to prevent immediate re-insertion)
    const shouldSkipAutoSemicolon = sentenceAutocompleteState.skipAutoSemicolon;
    sentenceAutocompleteState.skipAutoSemicolon = false; // Reset flag
    
    if (suggestions.length === 0 && inputWords.length > 1 && !previouslyHadPrefixMatch && !shouldSkipAutoSemicolon) {
        const lastWord = inputWords[inputWords.length - 1] || '';
        if (lastWord) {
            const fullInput = elements.sentenceInput.value;
            const lastSemicolonIndex = fullInput.lastIndexOf(';');
            const before = lastSemicolonIndex === -1 ? '' : fullInput.slice(0, lastSemicolonIndex + 1); // includes semicolon
            const currentGroup = lastSemicolonIndex === -1 ? fullInput : fullInput.slice(lastSemicolonIndex + 1);
            const groupTrimmed = currentGroup.trim();

            if (groupTrimmed) {
                // We want to freeze the current group and then start a new group
                // from the last word/partial, without duplicating that partial.
                // The frozen part is everything up to the last word of the current group.
                const groupWords = groupTrimmed.split(/\s+/);
                groupWords.pop(); // Remove the last word
                const frozenGroup = groupWords.join(' ');

                // Build the frozen prefix: previous groups + frozen group
                let frozenPrefix = '';
                if (before) {
                    const beforeClean = before.endsWith(';') ? before : before + ';';
                    frozenPrefix = beforeClean + (frozenGroup ? ' ' + frozenGroup : '');
                } else {
                    frozenPrefix = frozenGroup;
                }

                // The new group starts with just the last word
                const newGroup = lastWord;

                // Construct the new value with proper spacing
                let newValue = '';
                if (frozenPrefix) {
                    newValue = `${frozenPrefix}; ${newGroup}`.trimEnd();
                } else {
                    newValue = newGroup; // Edge case: no frozen prefix
                }

                // Store the undo target: the frozen prefix (without the problematic last word)
                // e.g., "mindestens e" → undo to "mindestens " (without "e")
                let undoTarget = '';
                if (frozenPrefix) {
                    undoTarget = frozenPrefix + ' '; // Add trailing space for continued typing
                }
                // If no frozen prefix, undo to empty (user typed a single word with no matches)
                
                sentenceAutocompleteState.lastAutoSemicolon = {
                    previousValue: undoTarget,
                    timestamp: Date.now()
                };

                // We are explicitly breaking out of the previous Tab chain
                // and starting a new group from the last word.
                sentenceAutocompleteState.isTabCompletion = false;
                elements.sentenceInput.value = newValue;
                sentenceAutocompleteState.currentAcceptedText = newGroup;

                // Restart autocomplete for the new group
                updateSentenceAutocomplete();
                return;
            }
        }
    }

    if (suggestions.length === 0) {
        hideSentenceAutocomplete();
        return;
    }
    
    // Update state
    sentenceAutocompleteState.suggestions = suggestions;
    sentenceAutocompleteState.selectedIndex = -1;
    
    // Render suggestions - match styling from Sentence Suggestions
    sentenceAutocompleteContainer.innerHTML = '';
    suggestions.forEach((entry, index) => {
        const suggestionItem = document.createElement('div');
        suggestionItem.className = 'suggestion-item';
        suggestionItem.style.cssText = 'padding: 8px 12px; cursor: pointer; border-bottom: 1px solid var(--border);';
        suggestionItem.addEventListener('mouseenter', () => {
            sentenceAutocompleteState.selectedIndex = index;
            updateSentenceAutocompleteSelection();
        });
        
        // Get the main text (without the prevContext prefix if it was included)
        let mainText = entry.current;
        if (entry.prevContext && mainText.toLowerCase().startsWith(entry.prevContext.toLowerCase())) {
            // Remove the context from the main text to avoid duplication
            mainText = mainText.substring(entry.prevContext.length).trimStart();
        }
        
        // When selecting, insert the main text (without context), not the full entry.current
        const textToInsert = mainText;
        suggestionItem.addEventListener('click', () => {
            selectSentenceSuggestion(textToInsert);
        });
        
        // Store for keyboard navigation
        suggestionItem.dataset.insertText = textToInsert;
        
        // Build display HTML matching Sentence Suggestions styling
        // Use prev/prevContext for preceding context
        const prevHtml = (entry.prev || entry.prevContext) ? `<span class="context">${escapeHtml(entry.prev || entry.prevContext)}</span> ` : '';
        // Use next for following context
        const nextHtml = entry.next ? ` <span class="context">${escapeHtml(entry.next)}</span>` : '';
        // Highlight the current text
        const highlightedCurrent = highlightPrefixMatch(mainText, inputValue);
        
        // Add source file info if available
        let sourceHtml = '';
        if (entry.source) {
            const fileName = entry.source.split('/').pop() || entry.source;
            sourceHtml = `<span class="suggestion-source">${fileName}</span>`;
        }
        
        suggestionItem.innerHTML = `${prevHtml}${highlightedCurrent}${nextHtml}${sourceHtml}`;
        
        sentenceAutocompleteContainer.appendChild(suggestionItem);
    });
    
    // Position and show (parent is position: relative, so use offsetTop/offsetLeft)
    const inputRect = elements.sentenceInput.getBoundingClientRect();
    const parentRect = elements.sentenceInput.parentElement.getBoundingClientRect();
    const offsetTop = inputRect.bottom - parentRect.top;
    const offsetLeft = inputRect.left - parentRect.left;
    
    sentenceAutocompleteContainer.style.top = `${offsetTop}px`;
    sentenceAutocompleteContainer.style.left = `${offsetLeft}px`;
    sentenceAutocompleteContainer.style.width = `${inputRect.width}px`;
    sentenceAutocompleteContainer.style.display = 'block';
    sentenceAutocompleteState.isVisible = true;
    updateSentenceAutocompleteSelection();
}

function updateSentenceAutocompleteSelection() {
    if (!sentenceAutocompleteContainer) return;
    
    const items = sentenceAutocompleteContainer.querySelectorAll('.suggestion-item');
    items.forEach((item, index) => {
        if (index === sentenceAutocompleteState.selectedIndex) {
            item.style.backgroundColor = 'var(--accent)';
            item.style.color = 'var(--bg-primary)';
            item.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        } else {
            item.style.backgroundColor = 'transparent';
            item.style.color = 'var(--text)';
        }
    });
}

function navigateSentenceAutocomplete(direction) {
    if (!sentenceAutocompleteState.isVisible || sentenceAutocompleteState.suggestions.length === 0) {
        return;
    }
    
    sentenceAutocompleteState.selectedIndex += direction;
    
    if (sentenceAutocompleteState.selectedIndex < -1) {
        sentenceAutocompleteState.selectedIndex = sentenceAutocompleteState.suggestions.length - 1;
    } else if (sentenceAutocompleteState.selectedIndex >= sentenceAutocompleteState.suggestions.length) {
        sentenceAutocompleteState.selectedIndex = -1;
    }
    
    updateSentenceAutocompleteSelection();
}

function acceptNextWord() {
    if (!elements.sentenceInput || sentenceAutocompleteState.suggestions.length === 0) {
        return;
    }
    
    // Work only on the active group (text after the last semicolon),
    // so previous groups don't interfere with Tab chaining.
    const fullInput = elements.sentenceInput.value;
    const lastSemicolonIndex = fullInput.lastIndexOf(';');
    const activeGroupRaw = lastSemicolonIndex === -1 ? fullInput : fullInput.slice(lastSemicolonIndex + 1);

    const currentInput = activeGroupRaw.trim();
    const normalizedInput = currentInput.toLowerCase();

    // Also interpret currentAccepted as the last group only
    const acceptedRaw = sentenceAutocompleteState.currentAcceptedText || '';
    const acceptedGroup = acceptedRaw.split(';').pop().trim();
    const currentAccepted = acceptedGroup.toLowerCase();
    
    // Try first to complete the *current* (possibly partial) last word from
    // the top suggestion. This handles cases like typing "hallo ro" when the
    // best suggestion is "hallo roger ..." – Tab in the middle of the word
    // should yield "hallo roger", not start a new group.
    const inputWords = currentInput.split(/\s+/);
    const lastInputWord = inputWords[inputWords.length - 1] || '';

    if (lastInputWord && sentenceAutocompleteState.suggestions.length > 0) {
        const topSuggestion = sentenceAutocompleteState.suggestions[0];
        const topText = (topSuggestion.current || '').toLowerCase();
        const topWords = topText.split(/\s+/);

        if (topWords.length >= inputWords.length) {
            let prefixMatches = true;
            // All words before the last must match exactly
            for (let i = 0; i < inputWords.length - 1; i++) {
                if (topWords[i] !== inputWords[i].toLowerCase()) {
                    prefixMatches = false;
                    break;
                }
            }

            if (prefixMatches) {
                const suggestionWord = topWords[inputWords.length - 1];
                const lastLower = lastInputWord.toLowerCase();

                // Only treat this as a mid-word completion if the suggestion
                // word starts with our partial but is strictly longer.
                if (suggestionWord.startsWith(lastLower) && suggestionWord.length > lastLower.length) {
                    const completedWords = [...inputWords];
                    completedWords[completedWords.length - 1] = suggestionWord;
                    const newGroupValue = completedWords.join(' ');

                    // Rebuild full input by replacing only the active group
                    let newFullValue;
                    if (lastSemicolonIndex === -1) {
                        newFullValue = newGroupValue;
                    } else {
                        const before = fullInput.slice(0, lastSemicolonIndex + 1); // includes semicolon
                        newFullValue = `${before} ${newGroupValue}`.replace(/\s+$/, '');
                    }

                    sentenceAutocompleteState.isTabCompletion = true;
                    elements.sentenceInput.value = newFullValue;
                    sentenceAutocompleteState.currentAcceptedText = newGroupValue;
                    updateSentenceAutocomplete();
                    return;
                }
            }
        }
    }

    // Find the best segment to continue from (next-word behavior)
    let bestSegment = null;
    let bestNextWord = null;
    
    // First, try to find a segment that continues from current accepted text (for Tab chain completion)
    if (currentAccepted) {
        for (const entry of sentenceAutocompleteState.suggestions) {
            const segmentNormalized = entry.currentNormalized;
            
            // Check if this segment continues from current accepted text
            if (segmentNormalized.startsWith(currentAccepted)) {
                // Get the next word after the current accepted text
                const remaining = entry.current.substring(currentAccepted.length).trim();
                if (remaining) {
                    const nextWord = remaining.split(/\s+/)[0];
                    if (nextWord) {
                        bestSegment = entry.current;
                        bestNextWord = nextWord;
                        break;
                    }
                }
            }
        }
    }
    
    // If no continuing segment found, find next word from segments matching current input
    if (!bestNextWord) {
        const inputWords = normalizedInput.split(/\s+/);
        
        // Look for segments that start with the current input (new segments)
        for (const entry of sentenceAutocompleteState.suggestions) {
            const segmentNormalized = entry.currentNormalized;
            const segmentWords = segmentNormalized.split(/\s+/);
            
            // Check if segment starts with current input words
            if (segmentWords.length >= inputWords.length) {
                let matches = true;
                for (let i = 0; i < inputWords.length; i++) {
                    if (segmentWords[i] !== inputWords[i]) {
                        matches = false;
                        break;
                    }
                }
                
                if (matches && segmentWords.length > inputWords.length) {
                    bestNextWord = segmentWords[inputWords.length];
                    bestSegment = entry.current;
                    break;
                }
            }
        }
    }
    
    // If still no word found, search within combined transcriptions directly
    if (!bestNextWord) {
        // Search combined transcriptions for matches
        for (const [file, transcription] of Object.entries(combinedTranscriptions)) {
            if (!transcription || !transcription.normalized) continue;
            
            const combinedNormalized = transcription.normalized;
            const combinedText = transcription.text;
            
            // Check if combined transcription contains the input
            if (combinedNormalized.includes(normalizedInput)) {
                // Find the position of the input
                const matchIndex = combinedNormalized.indexOf(normalizedInput);
                if (matchIndex !== -1) {
                    // Get the text after the match
                    const afterMatch = combinedText.substring(matchIndex + normalizedInput.length).trim();
                    if (afterMatch) {
                        const nextWord = afterMatch.split(/\s+/)[0];
                        if (nextWord) {
                            bestNextWord = nextWord;
                            // Extract a portion for display
                            const words = combinedText.split(/\s+/);
                            const normalizedWords = combinedNormalized.split(/\s+/);
                            const inputWords = normalizedInput.split(/\s+/);
                            
                            // Find word index
                            let wordIndex = 0;
                            let charCount = 0;
                            for (let i = 0; i < normalizedWords.length; i++) {
                                if (charCount + normalizedWords[i].length >= matchIndex) {
                                    wordIndex = i;
                                    break;
                                }
                                charCount += normalizedWords[i].length + 1;
                            }
                            
                            const startWord = Math.max(0, wordIndex);
                            const endWord = Math.min(words.length, wordIndex + inputWords.length + 15);
                            bestSegment = words.slice(startWord, endWord).join(' ');
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // If still no word found, check if current input is a complete segment
    // If so, look for segments that might continue with a space (new segment starting)
    if (!bestNextWord) {
        // Check if any segment exactly matches current input - if so, we've completed it
        let isComplete = false;
        for (const entry of sentenceAutocompleteState.suggestions) {
            if (entry.currentNormalized === normalizedInput) {
                isComplete = true;
                break;
            }
        }
        
        // If complete, look for segments that start with current input + space (next segment)
        if (isComplete) {
            for (const entry of sentenceAutocompleteState.suggestions) {
                const segmentNormalized = entry.currentNormalized;
                // Look for segments that start with current input followed by more words
                if (segmentNormalized.startsWith(normalizedInput + ' ')) {
                    const remaining = segmentNormalized.substring(normalizedInput.length + 1);
                    const nextWord = remaining.split(/\s+/)[0];
                    if (nextWord) {
                        bestNextWord = nextWord;
                        bestSegment = entry.current;
                        break;
                    }
                }
            }
        }
    }
    
    if (bestNextWord) {
        // Add the next word to the active group
        const newGroupValue = currentInput ? `${currentInput} ${bestNextWord}` : bestNextWord;

        // Rebuild full input by replacing only the active group
        let newFullValue;
        if (lastSemicolonIndex === -1) {
            newFullValue = newGroupValue;
        } else {
            const before = fullInput.slice(0, lastSemicolonIndex + 1); // includes semicolon
            newFullValue = `${before} ${newGroupValue}`.replace(/\s+$/, '');
        }

        // Set flag to indicate this is Tab completion
        sentenceAutocompleteState.isTabCompletion = true;

        elements.sentenceInput.value = newFullValue;

        // Update accepted text for next Tab press (track only the active group)
        sentenceAutocompleteState.currentAcceptedText = newGroupValue;

        // Update suggestions for the new input
        updateSentenceAutocomplete();
    } else if (currentInput) {
        // No more completions found for current sequence
        // Close current group (if any) with a semicolon and start a new group
        const fullInput = elements.sentenceInput.value;
        const lastSemicolonIndex = fullInput.lastIndexOf(';');
        const before = lastSemicolonIndex === -1 ? '' : fullInput.slice(0, lastSemicolonIndex + 1); // includes semicolon
        const currentGroup = lastSemicolonIndex === -1 ? fullInput : fullInput.slice(lastSemicolonIndex + 1);
        const groupTrimmed = currentGroup.trim();

        if (groupTrimmed) {
            const groupWords = groupTrimmed.split(/\s+/);
            const lastWord = groupWords[groupWords.length - 1] || '';

            // Build prefix with the finished group and a semicolon
            let prefix = '';
            if (before) {
                // There were previous groups, append this finished group after a space if needed
                const beforeClean = before.endsWith(';') ? before : before + ';';
                prefix = `${beforeClean} ${groupTrimmed}; `;
            } else {
                // This is the first group
                prefix = `${groupTrimmed}; `;
            }

            // Start a new group from the last word/partial
            const newGroup = lastWord;
            const newValue = `${prefix}${newGroup}`.trimEnd();

            sentenceAutocompleteState.isTabCompletion = true;
            elements.sentenceInput.value = newValue;

            // Accepted text for the new cycle is only the new group
            sentenceAutocompleteState.currentAcceptedText = newGroup;

            // Refresh suggestions for the new group (text after last comma)
            updateSentenceAutocomplete();
        }
    }
}

function selectSentenceSuggestion(sentence) {
    if (!elements.sentenceInput) {
        return;
    }
    
    elements.sentenceInput.value = sentence;
    sentenceAutocompleteState.currentAcceptedText = sentence;
    updateProjectData(data => {
        data.currentSentence = sentence;
    });
    hideSentenceAutocomplete();
}

function undoAutoSemicolon() {
    // Check if there's a recent auto-semicolon to undo (within 5 seconds)
    const autoSemicolon = sentenceAutocompleteState.lastAutoSemicolon;
    if (!autoSemicolon || !elements.sentenceInput) {
        return false;
    }
    
    const timeSinceAutoSemicolon = Date.now() - autoSemicolon.timestamp;
    const UNDO_WINDOW_MS = 5000; // 5 seconds to undo
    
    if (timeSinceAutoSemicolon > UNDO_WINDOW_MS) {
        // Too old, clear it and let normal backspace happen
        sentenceAutocompleteState.lastAutoSemicolon = null;
        return false;
    }
    
    // Set flag to prevent auto-semicolon from re-triggering
    sentenceAutocompleteState.skipAutoSemicolon = true;
    
    // Restore the previous value
    elements.sentenceInput.value = autoSemicolon.previousValue;
    sentenceAutocompleteState.lastAutoSemicolon = null;
    
    // Reset accepted text to match restored state
    const lastSemicolonIndex = autoSemicolon.previousValue.lastIndexOf(';');
    const activeGroup = lastSemicolonIndex === -1 
        ? autoSemicolon.previousValue 
        : autoSemicolon.previousValue.slice(lastSemicolonIndex + 1);
    sentenceAutocompleteState.currentAcceptedText = activeGroup.trim();
    
    // Refresh autocomplete (skipAutoSemicolon flag will prevent re-insertion)
    updateSentenceAutocomplete();
    
    return true; // Indicate we handled the backspace
}

function hideSentenceAutocomplete() {
    if (sentenceAutocompleteContainer) {
        sentenceAutocompleteContainer.style.display = 'none';
    }
    sentenceAutocompleteState.isVisible = false;
    sentenceAutocompleteState.selectedIndex = -1;
}

function resetDragState() {
    dragState.type = null;
    dragState.data = null;
    dragState.previewTargetIndex = undefined;
    dragState.originalItemPositions = null;
    if (elements.timelineList) {
        elements.timelineList.classList.remove('drag-active');
    }
    document.querySelectorAll('.timeline-item.drag-over').forEach(item => item.classList.remove('drag-over'));
    document.querySelectorAll('.timeline-item.drag-source').forEach(item => item.classList.remove('drag-source'));
    document.querySelectorAll('.phrase-container.drag-source').forEach(item => item.classList.remove('drag-source'));
}

function buildTimelineEntryFromContainer(container) {
    if (!container) {
        return null;
    }
    const select = container.querySelector('select');
    if (!select || !select.options.length) {
        return null;
    }
    const selectedOption = select.options[select.selectedIndex >= 0 ? select.selectedIndex : 0];
    const fileValue = selectedOption ? selectedOption.value : '';
    if (!fileValue) {
        return null;
    }
    const phrase = container.querySelector('h4') ? container.querySelector('h4').textContent : '';
    const startSlider = container.querySelector('.start-trim-slider');
    const endSlider = container.querySelector('.end-trim-slider');
    
    // Get original clip path and segment boundaries for re-rendering
    const originalClipPath = selectedOption?.dataset?.originalClipPath || fileValue;
    const originalStart = selectedOption?.dataset?.originalStart ? parseFloat(selectedOption.dataset.originalStart) : null;
    const originalEnd = selectedOption?.dataset?.originalEnd ? parseFloat(selectedOption.dataset.originalEnd) : null;
    const sourceVideo = selectedOption?.title || '';
    
    return {
        phrase: phrase || 'Clip',
        file: fileValue,
        matchLabel: selectedOption ? selectedOption.text : '',
        startTrim: startSlider ? parseInt(startSlider.value, 10) || 0 : 0,
        endTrim: endSlider ? parseInt(endSlider.value, 10) || 0 : 0,
        originalClipPath: originalClipPath,
        originalStart: originalStart,
        originalEnd: originalEnd,
        sourceVideo: sourceVideo
    };
}

function createTimelineEntry(baseEntry) {
    if (!baseEntry || !baseEntry.file) {
        return null;
    }
    return {
        id: generateId('timeline'),
        phrase: baseEntry.phrase || 'Clip',
        file: baseEntry.file,
        matchLabel: baseEntry.matchLabel || '',
        startTrim: parseInt(baseEntry.startTrim, 10) || 0,
        endTrim: parseInt(baseEntry.endTrim, 10) || 0,
        enabled: baseEntry.enabled !== undefined ? baseEntry.enabled : true,
        addedAt: new Date().toISOString(),
        // Store original clip path and segment boundaries for re-rendering
        originalClipPath: baseEntry.originalClipPath || baseEntry.file,
        originalStart: baseEntry.originalStart,
        originalEnd: baseEntry.originalEnd,
        sourceVideo: baseEntry.sourceVideo || ''
    };
}

function registerSearchContainerDrag(handleElement, container) {
    if (!handleElement || !container) {
        return;
    }
    handleElement.addEventListener('dragstart', event => {
        const baseEntry = buildTimelineEntryFromContainer(container);
        if (!baseEntry) {
            event.preventDefault();
            return;
        }
        dragState.type = 'searchClip';
        dragState.data = baseEntry;
        event.dataTransfer.effectAllowed = 'copy';
        event.dataTransfer.setData('text/plain', JSON.stringify({ type: 'searchClip' }));
        container.classList.add('drag-source');
        if (elements.timelineList) {
            elements.timelineList.classList.add('drag-active');
        }
    });
    handleElement.addEventListener('dragend', () => {
        container.classList.remove('drag-source');
        resetDragState();
    });
}

function registerTimelineItemDrag(item, entry) {
    if (!item || !entry) {
        return;
    }
    item.setAttribute('draggable', 'true');
    
    // Find waveform container and prevent drag initiation from it
    const waveformContainer = item.querySelector('.waveform-container');
    if (waveformContainer) {
        waveformContainer.addEventListener('mousedown', event => {
            event.stopPropagation();
            item.setAttribute('draggable', 'false');
        });
        waveformContainer.addEventListener('mouseup', () => {
            item.setAttribute('draggable', 'true');
        });
    }
    
    item.addEventListener('dragstart', event => {
        // Check if we're trying to drag from a waveform element
        const isWaveform = event.target.closest('.waveform-container');
        if (isWaveform) {
            event.preventDefault();
            return false;
        }
        
        dragState.type = 'timelineItem';
        dragState.data = { id: entry.id };
        item.classList.add('drag-source');
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', entry.id);
        if (elements.timelineList) {
            elements.timelineList.classList.add('drag-active');
            
            // Store original positions of ALL items BEFORE any transforms are applied
            // This is critical because getBoundingClientRect() includes transforms
            // Store positions RELATIVE TO CONTENT (not viewport) so they're scroll-independent
            const allItems = elements.timelineList.querySelectorAll('.timeline-item');
            dragState.originalItemPositions = [];
            
            // Store initial scroll position - this is our "expected" scroll
            dragStartScrollLeft = elements.timelineList.scrollLeft;
            expectedScrollLeft = elements.timelineList.scrollLeft;
            
            // Clear any existing transforms first to get true original positions
            allItems.forEach((itm) => {
                itm.style.transform = '';
            });
            // Force reflow to ensure transforms are cleared
            void elements.timelineList.offsetHeight;
            
            // Capture positions relative to CONTENT START (scroll-independent)
            const containerRect = elements.timelineList.getBoundingClientRect();
            const scrollLeft = elements.timelineList.scrollLeft;
            
            allItems.forEach((itm) => {
                const rect = itm.getBoundingClientRect();
                // Convert to content-relative positions (independent of scroll)
                const contentLeft = rect.left - containerRect.left + scrollLeft;
                dragState.originalItemPositions.push({
                    contentLeft: contentLeft,
                    contentRight: contentLeft + rect.width,
                    width: rect.width,
                    contentMidpoint: contentLeft + rect.width / 2
                });
            });
        }
    });
    item.addEventListener('dragend', () => {
        item.classList.remove('drag-source');
        clearTimelineDragPreview();
        resetDragState();
    });
    item.addEventListener('dragenter', event => handleTimelineItemDragEnter(event, item));
    item.addEventListener('dragleave', event => handleTimelineItemDragLeave(event, item));
}

function handleTimelineItemDragEnter(event, item) {
    // No longer used for preview - handled by dragover with position detection
    if (!dragState.type || !activeProject) {
        return;
    }
    event.preventDefault();
}

function handleTimelineItemDragLeave(event, item) {
    // No longer used for preview - handled by dragover with position detection
    if (!dragState.type) {
        return;
    }
}

// Track preview state
let previewUpdatePending = false;
let currentDragSourceIndex = -1;
let lastComputedTarget = -1;
let dragStartScrollLeft = 0; // Store scroll position at drag start
let expectedScrollLeft = 0; // The scroll position we expect (ignoring auto-scroll)

function updateTimelineDragPreviewByPosition(mouseX) {
    if (!elements.timelineList || dragState.type !== 'timelineItem') {
        return;
    }
    
    const allItems = Array.from(elements.timelineList.querySelectorAll('.timeline-item'));
    const sourceItem = allItems.find(item => item.dataset.id === dragState.data?.id);
    
    if (!sourceItem || allItems.length === 0) {
        return;
    }
    
    const originalPositions = dragState.originalItemPositions;
    if (!originalPositions || originalPositions.length !== allItems.length) {
        return;
    }
    
    const sourceIndex = allItems.indexOf(sourceItem);
    currentDragSourceIndex = sourceIndex;
    
    // Use the EXPECTED scroll position for calculations
    // This ignores any browser auto-scroll caused by transforms
    const containerRect = elements.timelineList.getBoundingClientRect();
    const mouseContentPosition = mouseX - containerRect.left + expectedScrollLeft;
    
    const itemWidth = originalPositions[sourceIndex].width;
    const gap = 12;
    const shiftAmount = itemWidth + gap;
    
    // SIMPLE APPROACH: Compare mouse against VISUAL midpoints of each item
    // Visual midpoint = original midpoint + any transform offset
    // The transform offset depends on the CURRENT target (from previous frame)
    
    const currentTarget = lastComputedTarget >= 0 ? lastComputedTarget : sourceIndex;
    
    // Calculate the VISUAL midpoint for each non-source item based on current transforms
    // An item shifts LEFT if: sourceIndex < currentTarget AND item is between source and target
    // An item shifts RIGHT if: sourceIndex > currentTarget AND item is between target and source
    
    function getVisualMidpoint(itemIndex) {
        const originalMidpoint = originalPositions[itemIndex].contentMidpoint;
        
        if (itemIndex === sourceIndex) {
            return originalMidpoint; // Source doesn't shift
        }
        
        let offset = 0;
        if (sourceIndex < currentTarget) {
            // Items between source+1 and target-1 have shifted LEFT
            if (itemIndex > sourceIndex && itemIndex < currentTarget) {
                offset = -shiftAmount;
            }
        } else if (sourceIndex > currentTarget) {
            // Items between target and source-1 have shifted RIGHT
            if (itemIndex >= currentTarget && itemIndex < sourceIndex) {
                offset = shiftAmount;
            }
        }
        
        return originalMidpoint + offset;
    }
    
    // Find target by checking which item's visual midpoint we've crossed
    // The rule: if mouse is LEFT of an item's visual midpoint, insert BEFORE that item
    let newTarget = allItems.length; // Default: insert at end
    
    for (let i = 0; i < allItems.length; i++) {
        if (i === sourceIndex) continue; // Skip source
        
        const visualMidpoint = getVisualMidpoint(i);
        
        if (mouseContentPosition < visualMidpoint) {
            newTarget = i;
            break;
        }
    }
    
    // Update the target
    lastComputedTarget = newTarget;
    
    // Apply transforms based on the NEW target
    for (let i = 0; i < allItems.length; i++) {
        const item = allItems[i];
        if (i === sourceIndex) continue;
        
        item.classList.remove('drag-over');
        
        if (sourceIndex < newTarget) {
            // Dragging RIGHT: items between source+1 and target-1 shift LEFT
            if (i > sourceIndex && i < newTarget) {
                item.style.transform = `translateX(-${shiftAmount}px)`;
            } else {
                item.style.transform = '';
            }
        } else if (sourceIndex > newTarget) {
            // Dragging LEFT: items between target and source-1 shift RIGHT
            if (i >= newTarget && i < sourceIndex) {
                item.style.transform = `translateX(${shiftAmount}px)`;
            } else {
                item.style.transform = '';
            }
        } else {
            item.style.transform = '';
        }
    }
    
    // Highlight the drop zone
    if (newTarget !== sourceIndex) {
        if (sourceIndex < newTarget && newTarget > 0) {
            const highlightIndex = newTarget - 1;
            if (allItems[highlightIndex] && highlightIndex !== sourceIndex) {
                allItems[highlightIndex].classList.add('drag-over');
            }
        } else if (sourceIndex > newTarget) {
            if (allItems[newTarget] && newTarget !== sourceIndex) {
                allItems[newTarget].classList.add('drag-over');
            }
        }
    }
    
    dragState.previewTargetIndex = newTarget;
}

function clearTimelineDragPreview() {
    lastDragMouseX = 0;
    previewUpdatePending = false;
    currentDragSourceIndex = -1;
    lastComputedTarget = -1;
    dragStartScrollLeft = 0;
    expectedScrollLeft = 0;
    
    if (!elements.timelineList) {
        return;
    }
    const allItems = elements.timelineList.querySelectorAll('.timeline-item');
    allItems.forEach(item => {
        item.style.transform = '';
        item.classList.remove('drag-over');
    });
}

function handleTimelineDragEnter(event) {
    if (!dragState.type || !activeProject) {
        return;
    }
    event.preventDefault();
    if (elements.timelineList) {
        elements.timelineList.classList.add('drag-active');
    }
}

// Store last mouse X for scroll-during-drag updates
let lastDragMouseX = 0;

function handleTimelineDragOver(event) {
    if (!dragState.type || !activeProject) {
        return;
    }
    event.preventDefault();
    if (event.dataTransfer) {
        event.dataTransfer.dropEffect = dragState.type === 'timelineItem' ? 'move' : 'copy';
    }
    
    // Store mouse position for scroll updates
    lastDragMouseX = event.clientX;
    
    // Update preview based on mouse position (throttled via requestAnimationFrame)
    if (dragState.type === 'timelineItem' && !previewUpdatePending) {
        previewUpdatePending = true;
        requestAnimationFrame(() => {
            updateTimelineDragPreviewByPosition(event.clientX);
            previewUpdatePending = false;
        });
    }
}

// Handle scroll during drag - PREVENT scrolling to avoid calculation issues
function handleTimelineScrollDuringDrag() {
    if (dragState.type === 'timelineItem') {
        // Force scroll back to expected position immediately
        // This prevents browser auto-scroll from affecting calculations
        if (elements.timelineList.scrollLeft !== expectedScrollLeft) {
            elements.timelineList.scrollLeft = expectedScrollLeft;
        }
    }
}

function handleTimelineDragLeave(event) {
    if (!dragState.type || !elements.timelineList) {
        return;
    }
    const related = event.relatedTarget;
    if (!related || !elements.timelineList.contains(related)) {
        elements.timelineList.classList.remove('drag-active');
        document.querySelectorAll('.timeline-item.drag-over').forEach(item => item.classList.remove('drag-over'));
        clearTimelineDragPreview();
    }
}

function handleTimelineDrop(event) {
    if (!dragState.type || !activeProject) {
        clearTimelineDragPreview();
        resetDragState();
        return;
    }
    event.preventDefault();
    
    // Use preview target index if available (more accurate), otherwise calculate from event
    const dropIndex = dragState.previewTargetIndex !== undefined 
        ? dragState.previewTargetIndex 
        : getTimelineDropIndex(event);
    
    console.log('Drop index:', dropIndex, 'Drag type:', dragState.type, 'Preview index:', dragState.previewTargetIndex);

    if (dragState.type === 'searchClip' && dragState.data) {
        const entry = createTimelineEntry(dragState.data);
        if (entry) {
            updateTimeline(entries => {
                const index = Math.max(0, Math.min(dropIndex, entries.length));
                entries.splice(index, 0, entry);
            });
        }
    } else if (dragState.type === 'timelineItem' && dragState.data) {
        const movingId = dragState.data.id;
        if (movingId) {
            updateTimeline(entries => {
                const fromIndex = entries.findIndex(item => item.id === movingId);
                console.log('Moving item from index:', fromIndex, 'to index:', dropIndex);
                
                if (fromIndex === -1) {
                    console.warn('Could not find item to move');
                    return;
                }
                
                // Clamp target index to valid range
                let targetIndex = Math.max(0, Math.min(dropIndex, entries.length));
                
                // If dropping at the same position, do nothing
                if (targetIndex === fromIndex) {
                    console.log('No move needed - same position');
                    return;
                }
                
                // Remove the item from its current position
                const [item] = entries.splice(fromIndex, 1);
                
                // Adjust target index if we removed an item before the target
                if (fromIndex < targetIndex) {
                    targetIndex -= 1;
                }
                
                console.log('Inserting at adjusted index:', targetIndex);
                
                // Insert at the target position
                entries.splice(targetIndex, 0, item);
            });
        }
    }

    clearTimelineDragPreview();
    resetDragState();
}

function getTimelineDropIndex(event) {
    const entries = getTimelineEntries();
    
    if (!elements.timelineList) {
        console.log('getTimelineDropIndex: No timeline list');
        return entries.length;
    }
    
    // Get all timeline items in DOM order
    const allItems = Array.from(elements.timelineList.querySelectorAll('.timeline-item'));
    
    if (allItems.length === 0) {
        console.log('getTimelineDropIndex: No items');
        return 0;
    }
    
    const mouseX = event.clientX;
    let targetItem = null;
    
    // First try to find target via closest (works when dropping directly on an item)
    targetItem = event.target.closest('.timeline-item');
    
    // If not found, find which item the cursor is visually over
    if (!targetItem) {
        for (const item of allItems) {
            const rect = item.getBoundingClientRect();
            // getBoundingClientRect accounts for CSS transforms, so this gives visual position
            if (mouseX >= rect.left && mouseX <= rect.right) {
                targetItem = item;
                break;
            }
        }
    }
    
    // If still not found, check if we're before first or after last
    if (!targetItem) {
        const firstRect = allItems[0].getBoundingClientRect();
        const lastRect = allItems[allItems.length - 1].getBoundingClientRect();
        
        if (mouseX < firstRect.left) {
            console.log('getTimelineDropIndex: Before first item, returning 0');
            return 0;
        } else if (mouseX > lastRect.right) {
            console.log('getTimelineDropIndex: After last item, returning', entries.length);
        return entries.length;
    }
        // If we're in the timeline area but not over any item, try to find closest item
        console.log('getTimelineDropIndex: Not over any item, finding closest');
        let closestItem = null;
        let closestDistance = Infinity;
        for (const item of allItems) {
            const rect = item.getBoundingClientRect();
            const centerX = rect.left + rect.width / 2;
            const distance = Math.abs(mouseX - centerX);
            if (distance < closestDistance) {
                closestDistance = distance;
                closestItem = item;
            }
        }
        if (closestItem) {
            targetItem = closestItem;
        } else {
            console.log('getTimelineDropIndex: No closest item found, defaulting to end');
        return entries.length;
    }
    }
    
    // Get the array index from the target item's dataset
    // This is the index in the entries array, not DOM position
    const targetArrayIndex = parseInt(targetItem.dataset.index || '-1', 10);
    
    console.log('getTimelineDropIndex: Target item dataset.index:', targetItem.dataset.index, 'parsed:', targetArrayIndex);
    
    if (Number.isNaN(targetArrayIndex) || targetArrayIndex < 0) {
        console.warn('getTimelineDropIndex: Invalid targetArrayIndex, returning end');
        return entries.length;
    }
    
    // Determine if we're dropping before or after this item
    const rect = targetItem.getBoundingClientRect();
    const isAfter = mouseX > rect.left + rect.width / 2;
    
    const result = isAfter ? targetArrayIndex + 1 : targetArrayIndex;
    console.log('getTimelineDropIndex: mouseX:', mouseX, 'rect center:', rect.left + rect.width / 2, 'isAfter:', isAfter, 'result:', result);
    
    return result;
}

function getTimelineEntries() {
    return Array.isArray(activeProject?.data?.timeline) ? activeProject.data.timeline : [];
}

function stopTimelineItemDragFromInput(event) {
    event.stopPropagation();
}

function setTimelineTrimValue(entryId, field, value) {
    if (!activeProject || !entryId) {
        return;
    }
    // Handle speed as float, file/rerendered as-is, other values as int
    let safeValue;
    if (field === 'speed') {
        const parsedValue = Number.parseFloat(value);
        safeValue = Number.isFinite(parsedValue) && parsedValue > 0 ? parsedValue : 1.0;
    } else if (field === 'file' || field === 'rerendered') {
        // For file paths and boolean flags, use the value as-is
        safeValue = value;
    } else {
        const parsedValue = Number.parseInt(value, 10);
        safeValue = Number.isFinite(parsedValue) && parsedValue >= 0 ? parsedValue : 0;
    }
    updateProjectData(data => {
        if (!Array.isArray(data.timeline)) {
            return;
        }
        const target = data.timeline.find(item => item && item.id === entryId);
        if (target) {
            target[field] = safeValue;
        }
    });
}

let isRenderingTimeline = false;

function renderTimeline(entriesParam) {
    if (!elements.timelineList) {
        return;
    }
    isRenderingTimeline = true;
    const entries = Array.isArray(entriesParam) ? entriesParam : getTimelineEntries();
    elements.timelineList.innerHTML = '';

    if (!entries.length) {
        isRenderingTimeline = false;
        return;
    }

    const fragment = document.createDocumentFragment();

    entries.forEach((entry, index) => {
        if (!entry || typeof entry !== 'object') {
            return;
        }

        if (!entry.id) {
            entry.id = generateId('timeline');
        }
        
        // Ensure enabled property exists (default to true for existing entries)
        if (entry.enabled === undefined) {
            entry.enabled = true;
        }

        const item = document.createElement('li');
        item.className = 'timeline-item';
        if (!entry.enabled) {
            item.classList.add('timeline-item-disabled');
        }
        item.dataset.id = entry.id;

        const indexBadge = document.createElement('div');
        indexBadge.className = 'timeline-index';
        indexBadge.textContent = String(index + 1);

        const body = document.createElement('div');
        body.className = 'timeline-body';

        const meta = document.createElement('div');
        meta.className = 'timeline-meta';

        const title = document.createElement('strong');
        title.textContent = entry.phrase || `Clip ${index + 1}`;
        meta.appendChild(title);

        body.appendChild(meta);

        const trimControls = document.createElement('div');
        trimControls.className = 'timeline-trim-controls';

        const startValue = Number.isFinite(entry.startTrim) && entry.startTrim >= 0 ? entry.startTrim : 450;
        const endValue = Number.isFinite(entry.endTrim) && entry.endTrim >= 0 ? entry.endTrim : 450;

        // Get current speed for waveform display
        const currentSpeed = entry.speed !== undefined ? entry.speed : 1.0;
        
        // Get original clip path and segment boundaries for re-rendering
        const originalClipPath = entry.originalClipPath || entry.file;
        const originalStart = entry.originalStart;
        const originalEnd = entry.originalEnd;
        let originalDurationMs = null;
        if (originalStart != null && originalEnd != null) {
            originalDurationMs = (originalEnd - originalStart) * 1000; // Convert to milliseconds
        }
        
        // Create waveform visualization for timeline
        // Use original clip path for waveform (fast), but track original segment duration for accurate trimming
        const timelineWaveform = createWaveformVisualization(
            trimControls,
            startValue,
            endValue,
            originalDurationMs || TIMELINE_TRIM_MAX, // Use original segment duration for accurate trimming
            (startTrim, endTrim) => {
                entry.startTrim = startTrim;
                entry.endTrim = endTrim;
                setTimelineTrimValue(entry.id, 'startTrim', startTrim);
                setTimelineTrimValue(entry.id, 'endTrim', endTrim);
                // Don't autoplay during dragging - only on release
            },
            originalClipPath, // Use original clip path for waveform (fast loading)
            currentSpeed, // Pass initial speed for display
            async (startTrim, endTrim) => {  // onTrimRelease callback - re-render and play
                // Store the trim values from the callback (these are the actual current values)
                const currentStartTrim = startTrim;
                const currentEndTrim = endTrim;
                
                console.log('Timeline trim release - re-rendering with:', { 
                    startTrim: currentStartTrim, 
                    endTrim: currentEndTrim, 
                    entryId: entry.id,
                    entry_startTrim: entry.startTrim,  // Debug: check entry value
                    entry_endTrim: entry.endTrim       // Debug: check entry value
                });
                
                // Update entry and project data AFTER we've captured the values
                entry.startTrim = currentStartTrim;
                entry.endTrim = currentEndTrim;
                setTimelineTrimValue(entry.id, 'startTrim', currentStartTrim);
                setTimelineTrimValue(entry.id, 'endTrim', currentEndTrim);
                
                // Re-render clip with new trim values from original source
                // Use the captured values, not entry values (in case entry was updated elsewhere)
                if (originalStart != null && originalEnd != null && entry.sourceVideo) {
                    const rerenderedPath = await rerenderTimelineClip(entry, currentStartTrim, currentEndTrim);
                    if (rerenderedPath && rerenderedPath.trim() !== '') {
                        // Update entry with re-rendered path (both in-memory and project data)
                        entry.file = rerenderedPath;
                        entry.rerendered = true;
                        // Also update project data so getTimelineEntries() returns the correct file
                        setTimelineTrimValue(entry.id, 'file', rerenderedPath);
                        setTimelineTrimValue(entry.id, 'rerendered', true);
                        // Don't update waveform - it should stay on the original clip
                        // Play the newly rendered clip (no trims needed, it's already trimmed)
                        playTimelineItem(entry.id);
                    } else {
                        // Fallback: play with JavaScript trimming
                        playTimelineItem(entry.id);
                    }
                } else {
                    // No original data available, just play with current trims
                    playTimelineItem(entry.id);
                }
            }
        );
        
        // Set the clip duration to the original segment duration for accurate trim calculations
        if (originalDurationMs) {
            timelineWaveform.setClipDuration(originalDurationMs);
        }
        trimControls.appendChild(timelineWaveform);
        body.appendChild(trimControls);

        // Speed slider (100% to 50%)
        const speedControls = document.createElement('div');
        speedControls.className = 'timeline-speed-controls';

        const speedLabel = document.createElement('label');
        speedLabel.className = 'speed-label';
        speedLabel.textContent = 'Speed: ';

        const speedValue = document.createElement('span');
        speedValue.className = 'speed-value';
        speedValue.textContent = `${Math.round(currentSpeed * 100)}%`;
        speedLabel.appendChild(speedValue);

        const speedSlider = document.createElement('input');
        speedSlider.type = 'range';
        speedSlider.className = 'speed-slider';
        speedSlider.min = '50';
        speedSlider.max = '100';
        speedSlider.value = String(Math.round(currentSpeed * 100));
        speedSlider.step = '5';
        speedSlider.draggable = false;
        
        // Prevent drag events from bubbling to parent (card drag)
        speedSlider.addEventListener('mousedown', (e) => {
            e.stopPropagation();
            item.setAttribute('draggable', 'false');
        });
        speedSlider.addEventListener('mouseup', () => {
            item.setAttribute('draggable', 'true');
        });
        speedSlider.addEventListener('dragstart', (e) => {
            e.preventDefault();
            e.stopPropagation();
        });

        speedSlider.addEventListener('input', () => {
            const speedPercent = parseInt(speedSlider.value, 10);
            const speed = speedPercent / 100;
            speedValue.textContent = `${speedPercent}%`;
            entry.speed = speed;
            setTimelineTrimValue(entry.id, 'speed', speed);
            // Update waveform display to show effective times
            if (timelineWaveform.setSpeed) {
                timelineWaveform.setSpeed(speed);
            }
        });

        speedSlider.addEventListener('change', () => {
            // Auto-play when speed changes (user interaction only, not during render)
            if (!isRenderingTimeline) {
                playTimelineItem(entry.id);
            }
        });
        
        speedControls.appendChild(speedLabel);
        speedControls.appendChild(speedSlider);
        body.appendChild(speedControls);

        const actions = document.createElement('div');
        actions.className = 'timeline-actions';

        // Enable/Disable toggle checkbox
        const toggleContainer = document.createElement('label');
        toggleContainer.className = 'timeline-toggle';
        toggleContainer.style.cssText = 'display: flex; align-items: center; gap: 6px; cursor: pointer; user-select: none;';
        
        const toggleCheckbox = document.createElement('input');
        toggleCheckbox.type = 'checkbox';
        toggleCheckbox.checked = entry.enabled;
        toggleCheckbox.dataset.id = entry.id;
        toggleCheckbox.style.cssText = 'cursor: pointer;';
        
        const toggleLabel = document.createElement('span');
        toggleLabel.textContent = entry.enabled ? 'Enabled' : 'Disabled';
        toggleLabel.style.cssText = 'font-size: 12px; color: var(--text-secondary, #999);';
        
        toggleCheckbox.addEventListener('change', (e) => {
            const enabled = e.target.checked;
            toggleLabel.textContent = enabled ? 'Enabled' : 'Disabled';
            entry.enabled = enabled;
            
            // Update visual state
            if (enabled) {
                item.classList.remove('timeline-item-disabled');
            } else {
                item.classList.add('timeline-item-disabled');
            }
            
            // Save to project data
            updateTimeline(mutator => {
                const target = mutator.find(item => item.id === entry.id);
                if (target) {
                    target.enabled = enabled;
                }
            });
        });
        
        toggleContainer.appendChild(toggleCheckbox);
        toggleContainer.appendChild(toggleLabel);
        actions.appendChild(toggleContainer);

        const buttonRow = document.createElement('div');
        buttonRow.className = 'button-row';

        const previewBtn = document.createElement('button');
        previewBtn.type = 'button';
        previewBtn.className = 'ghost-button';
        previewBtn.dataset.action = 'play';
        previewBtn.dataset.id = entry.id;
        previewBtn.textContent = 'Preview';
        buttonRow.appendChild(previewBtn);

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'ghost-button danger';
        removeBtn.dataset.action = 'remove';
        removeBtn.dataset.id = entry.id;
        removeBtn.textContent = 'Remove';
        buttonRow.appendChild(removeBtn);
        
        actions.appendChild(buttonRow);

        item.appendChild(indexBadge);
        item.appendChild(body);
        item.appendChild(actions);
        item.dataset.index = String(index);
        registerTimelineItemDrag(item, entry);
        fragment.appendChild(item);
    });

    elements.timelineList.appendChild(fragment);
    isRenderingTimeline = false;
}

function updateTimeline(mutator, options) {
    if (!activeProject || typeof mutator !== 'function') {
        return;
    }
    updateProjectData(data => {
        if (!Array.isArray(data.timeline)) {
            data.timeline = [];
        }
        mutator(data.timeline);
    }, options);
    renderTimeline();
}

function handleAddContainerToTimeline(container) {
    if (!activeProject || !container) {
        return;
    }
    const baseEntry = buildTimelineEntryFromContainer(container);
    if (!baseEntry) {
        return;
    }
    const timelineEntry = createTimelineEntry(baseEntry);
    if (!timelineEntry) {
        return;
    }
    updateTimeline(entries => {
        entries.push(timelineEntry);
    });
}

function handleAddAllVariantsToTimeline(container) {
    if (!activeProject || !container) {
        return;
    }
    const select = container.querySelector('select');
    if (!select || !select.options.length) {
        return;
    }
    
    const phrase = container.querySelector('h4') ? container.querySelector('h4').textContent : '';
    const startSlider = container.querySelector('.start-trim-slider');
    const endSlider = container.querySelector('.end-trim-slider');
    const startTrim = startSlider ? parseInt(startSlider.value, 10) || 0 : 0;
    const endTrim = endSlider ? parseInt(endSlider.value, 10) || 0 : 0;
    
    const additions = [];
    const originalSelectedIndex = select.selectedIndex;
    
    // Iterate through all options and create timeline entries
    for (let i = 0; i < select.options.length; i++) {
        const option = select.options[i];
        const fileValue = option.value;
        if (!fileValue) {
            continue;
        }
        
        const baseEntry = {
            phrase: phrase || 'Clip',
            file: fileValue,
            matchLabel: option.text,
            startTrim: startTrim,
            endTrim: endTrim
        };
        
        const timelineEntry = createTimelineEntry(baseEntry);
        if (timelineEntry) {
            additions.push(timelineEntry);
        }
    }
    
    // Restore original selection
    select.selectedIndex = originalSelectedIndex;
    
    if (additions.length > 0) {
        updateTimeline(entries => {
            additions.forEach(entry => entries.push(entry));
        });
        console.log(`Added ${additions.length} variants to timeline`);
    }
}

function addAllMatchesToTimeline() {
    if (!activeProject) {
        return;
    }
    const containers = document.querySelectorAll('.phrase-container');
    const additions = [];
    containers.forEach(container => {
        const baseEntry = buildTimelineEntryFromContainer(container);
        if (!baseEntry) {
            return;
        }
        const entry = createTimelineEntry(baseEntry);
        if (entry) {
            additions.push(entry);
        }
    });
    if (!additions.length) {
        return;
    }
    updateTimeline(entries => {
        additions.forEach(entry => entries.push(entry));
    });
}

function handleTimelineListClick(event) {
    const button = event.target.closest('button');
    if (!button) {
        return;
    }
    const action = button.dataset.action;
    const itemId = button.dataset.id;
    if (!action || !itemId) {
        return;
    }

    switch (action) {
        case 'play':
            playTimelineItem(itemId);
            break;
        case 'remove':
            removeTimelineItem(itemId);
            break;
        default:
            break;
    }
}

async function rerenderTimelineClip(entry, startTrimMs, endTrimMs) {
    if (!entry || !entry.originalClipPath || !entry.sourceVideo || 
        entry.originalStart == null || entry.originalEnd == null) {
        console.warn('rerenderTimelineClip: missing required data', {
            hasEntry: !!entry,
            originalClipPath: entry?.originalClipPath,
            sourceVideo: entry?.sourceVideo,
            originalStart: entry?.originalStart,
            originalEnd: entry?.originalEnd
        });
        return null;
    }
    
    try {
        console.log('Re-rendering timeline clip with:', {
            originalClipPath: entry.originalClipPath,
            sourceVideo: entry.sourceVideo,
            originalStart: entry.originalStart,
            originalEnd: entry.originalEnd,
            startTrimMs,
            endTrimMs
        });
        
        const response = await fetch('/rerender_clip', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                clip_path: entry.originalClipPath,
                source_video: entry.sourceVideo,
                original_start: entry.originalStart,
                original_end: entry.originalEnd,
                start_trim_ms: startTrimMs,
                end_trim_ms: endTrimMs
            })
        });
        
        console.log('Request body sent to server:', {
            start_trim_ms: startTrimMs,
            end_trim_ms: endTrimMs,
            entry_endTrim: entry.endTrim  // Debug: check if entry has different value
        });
        
        if (!response.ok) {
            const error = await response.json();
            console.error('rerenderTimelineClip: server error', error);
            return null;
        }
        
        const result = await response.json();
        return result.clip_path;
    } catch (error) {
        console.error('rerenderTimelineClip: request failed', error);
        return null;
    }
}

function playTimelineItem(itemId) {
    const entry = getTimelineEntries().find(item => item.id === itemId);
    if (!entry || !entry.file) {
        return;
    }
    // Update video player title
    updateVideoPlayerTitle(entry.segment || 'Timeline item');
    const speed = entry.speed !== undefined ? entry.speed : 1.0;
    
    // Check if this clip is already re-rendered
    if (entry.rerendered) {
        // Play re-rendered video directly without any trimming or pause logic
        // The video is already trimmed at the file level
        playVideo(entry.file, false, speed);
    } else {
        // Play with trimming logic
        playVideoWithTrim(entry.file, entry.startTrim || 0, entry.endTrim || 0, false, speed);
    }
}

function updateVideoPlayerTitle(title) {
    const videoCard = document.querySelector('.video-card');
    if (videoCard) {
        const titleElement = videoCard.querySelector('h2');
        if (titleElement) {
            titleElement.textContent = title || 'Preview';
        }
    }
}

function removeTimelineItem(itemId) {
    updateTimeline(entries => {
        const index = entries.findIndex(item => item.id === itemId);
        if (index !== -1) {
            entries.splice(index, 1);
        }
    });
}

function playTimeline() {
    const entries = getTimelineEntries().filter(entry => 
        entry && entry.file && entry.enabled !== false
    );
    if (!entries.length || !elements.videoPlayer) {
        return;
    }

    let currentIndex = 0;

    const playNext = () => {
        if (currentIndex >= entries.length) {
            elements.videoPlayer.onpause = null;
            elements.videoPlayer.onended = null;
            updateVideoPlayerTitle('Preview');
            return;
        }
        const entry = entries[currentIndex];
        currentIndex += 1;
        // Update video player title for timeline playback
        updateVideoPlayerTitle(entry.segment || `Timeline item ${currentIndex}`);
        const speed = entry.speed !== undefined ? entry.speed : 1.0;
        
        // Check if this clip is already re-rendered
        if (entry.rerendered) {
            // Play re-rendered video directly without any trimming or pause logic
            // The video is already trimmed at the file level
            playVideo(entry.file, false, speed);
        } else {
            // Play with trimming logic
            playVideoWithTrim(entry.file, entry.startTrim || 0, entry.endTrim || 0, false, speed);
        }
    };

    elements.videoPlayer.onpause = playNext;
    elements.videoPlayer.onended = playNext;
    playNext();
}

function mergeTimeline() {
    const allEntries = getTimelineEntries();
    console.log(`[Merge] Total timeline entries: ${allEntries.length}`);
    
    // Filter out invalid entries and disabled entries, log any that are removed
    const entries = allEntries.filter((entry, index) => {
        if (!entry) {
            console.warn(`[Merge] Entry at index ${index} is null/undefined`);
            return false;
        }
        if (!entry.file) {
            console.warn(`[Merge] Entry at index ${index} (phrase: "${entry.phrase}") has no file property`);
            return false;
        }
        if (entry.enabled === false) {
            console.log(`[Merge] Entry at index ${index} (phrase: "${entry.phrase}") is disabled, skipping`);
            return false;
        }
        return true;
    });
    
    if (!entries.length) {
        console.warn('[Merge] No valid entries to merge');
        return;
    }

    console.log(`[Merge] Preparing to merge ${entries.length} valid timeline entries:`, entries.map((e, i) => ({
        index: i,
        phrase: e.phrase,
        file: e.file,
        startTrim: e.startTrim,
        endTrim: e.endTrim,
        id: e.id
    })));

    // Check for duplicate files
    const fileCounts = {};
    entries.forEach((entry, index) => {
        const fileKey = `${entry.file}_${entry.startTrim}_${entry.endTrim}`;
        if (!fileCounts[fileKey]) {
            fileCounts[fileKey] = [];
        }
        fileCounts[fileKey].push(index + 1);
    });
    
    Object.entries(fileCounts).forEach(([fileKey, indices]) => {
        if (indices.length > 1) {
            console.warn(`[Merge] Duplicate entry detected at positions: ${indices.join(', ')} (file: ${fileKey})`);
        }
    });

    const videosToMerge = entries.map((entry, index) => {
        const videoData = {
        title: (entry.phrase || 'Clip').replace(/\s+/g, '_'),
        video: entry.file,
        startTrim: parseInt(entry.startTrim, 10) || 0,
            endTrim: parseInt(entry.endTrim, 10) || 0,
            speed: entry.speed !== undefined ? entry.speed : 1.0
        };
        console.log(`[Merge] Entry ${index + 1}:`, videoData);
        return videoData;
    });

    // Show progress indicator
    showMergeProgress('Preparing merge...', 0, videosToMerge.length);

    // Disable merge button during merge
    if (elements.timelineMergeButton) {
        elements.timelineMergeButton.disabled = true;
    }

    let abortController = new AbortController();
    let reader = null;

    // Use SSE endpoint for progress updates
    fetch('/merge_videos_stream', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ videos: videosToMerge }),
        signal: abortController.signal
    })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            function processStream() {
                return reader.read().then(({ done, value }) => {
                    if (done) {
                        hideMergeProgress();
                        if (elements.timelineMergeButton) {
                            elements.timelineMergeButton.disabled = false;
                        }
                        return;
                    }

                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    for (const line of lines) {
                        if (line.startsWith('data: ')) {
                            try {
                                const jsonStr = line.substring(6);
                                const data = JSON.parse(jsonStr);

                                if (data.done) {
                                    hideMergeProgress();
                                    if (elements.timelineMergeButton) {
                                        elements.timelineMergeButton.disabled = false;
                                    }
                                    
                                    if (data.merged_video) {
                                        // Get loop preference from checkbox (defaults to false if not found)
                                        const shouldLoop = elements.loopMergedVideo ? elements.loopMergedVideo.checked : false;
                                        playVideoWithTrim(data.merged_video, 0, 0, shouldLoop);

                                        // Switch to viewer on mobile after merge completes
                                        if (isMobileLayout()) {
                                            setTimeout(() => {
                                                switchMobileSection('viewer');
                                            }, 500);
                                        }

                                        const downloadId = 'mergedDownloadLink';
                                        const existingDownload = document.getElementById(downloadId);
                                        if (existingDownload) {
                                            existingDownload.remove();
                                        }

                                        const downloadButton = document.createElement('a');
                                        downloadButton.id = downloadId;
                                        downloadButton.className = 'button ghost-button';
                                        downloadButton.href = getVideoUrl(data.merged_video);
                                        downloadButton.download = '';
                                        downloadButton.textContent = 'Download merged video';

                                        const videoCard = elements.videoPlayer ? elements.videoPlayer.closest('.video-card') : null;
                                        if (videoCard) {
                                            videoCard.appendChild(downloadButton);
                                        } else {
                                            document.body.appendChild(downloadButton);
                                        }
                                    }
                                    return;
                                }

                                if (data.error) {
                                    hideMergeProgress();
                                    if (elements.timelineMergeButton) {
                                        elements.timelineMergeButton.disabled = false;
                                    }
                                    console.error('[Merge] Error:', data.error);
                                    alert(`Error merging videos: ${data.error}`);
                                    return;
                                }

                                // Update progress
                                if (data.message && data.progress !== undefined && data.total !== undefined) {
                                    updateMergeProgress(data.message, data.progress, data.total);
                                }
                            } catch (e) {
                                console.error('[Merge] Error parsing SSE data:', e, line);
                            }
                        }
                    }

                    return processStream();
                });
            }

            return processStream();
        })
        .catch(error => {
            if (error.name === 'AbortError') {
                console.log('[Merge] Merge was cancelled');
                return;
            }
            hideMergeProgress();
            if (elements.timelineMergeButton) {
                elements.timelineMergeButton.disabled = false;
            }
            console.error('Error merging timeline videos:', error);
            alert(`Error merging videos: ${error.message}`);
        });
}

function clearTimeline() {
    updateTimeline(entries => {
        entries.length = 0;
    });
}

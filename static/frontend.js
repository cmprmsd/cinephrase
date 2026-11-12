const AUTOSAVE_DELAY = 800;
const MAX_SAVED_SENTENCES = 50;

const DEFAULT_TRIM_MS = 450;   // the search export pads every match by this much on both sides
const MIN_PLAYABLE_MS = 20;    // a clip is never trimmed below this

// Sample-accurate clip preview (static/clip-player.js)
let clipPlayer = null;
let mainStage = null;
let floatingStage = null;
const resultCards = new WeakMap();   // .phrase-container element -> card controller (see createResultCard)
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
let autosaveRevision = 0;      // bumped by every change of the active project
let autosaveRequest = null;     // the save that is on the wire, if any
let autosaveFailed = false;     // the last save attempt did not reach the server (or was refused)
let isApplyingProject = false;

const elements = {};
const dragState = { type: null, data: null };

// Sentence autocomplete state
let sentenceAutocompleteContainer = null;
let sentenceAutocompleteState = {
    selectedIndex: -1,
    suggestions: [],
    currentAcceptedText: '',
    isVisible: false,
    isTabCompletion: false,  // Flag to track if change came from Tab
    keyboardSelection: false, // the highlighted suggestion was chosen with the arrow keys (not just hovered)
    pasted: false,            // the last edit was a paste: never rewrite pasted text
    timer: null,              // pending refresh after typing
    // Auto-semicolon undo support
    lastAutoSemicolon: null,  // { previousValue: string, timestamp: number } or null
    skipAutoSemicolon: false, // Flag to skip auto-semicolon after undo
    noSplitPrefix: ''         // group the user restored with Backspace: not split again while it is being extended
};

// Floating preview state
let floatingPreviewContainer = null;
let currentSelectedCard = null;

// localStorage / sessionStorage throw when site data is blocked or the quota is exceeded: the app must still work.
function readStored(storage, key) {
    try {
        return storage.getItem(key);
    } catch (error) {
        return null;
    }
}

function writeStored(storage, key, value) {
    try {
        storage.setItem(key, value);
    } catch (error) {
        /* not essential */
    }
}

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

/**
 * Copy text to the clipboard; resolves to whether it worked. The Clipboard API only exists on https and
 * localhost, so a device that reaches the app over the LAN needs the selection-based way.
 */
async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (error) { /* refused (no focus, permission): try the other way */ }
    }
    const area = document.createElement('textarea');
    area.value = text;
    area.setAttribute('readonly', '');
    area.style.cssText = 'position: fixed; top: 0; left: 0; width: 1px; height: 1px; opacity: 0;';
    document.body.appendChild(area);
    area.select();
    let copied = false;
    try {
        copied = document.execCommand('copy');
    } catch (error) { /* not supported */ }
    area.remove();
    return copied;
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
        writeStored(window.sessionStorage, 'mobileActiveSection', sectionName);
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
                    const savedSection = readStored(window.sessionStorage, 'mobileActiveSection') || 'search';
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
        const savedSection = readStored(window.sessionStorage, 'mobileActiveSection') || 'search';
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
    corpusRequestToken += 1;        // answers of requests that are still on the way are for the old file list
    sentences = [];
    combinedTranscriptions = {}; // Clear combined transcriptions
    sentenceAutocompleteState.suggestions = [];     // they belong to the corpus that is gone
    hideSentenceAutocomplete();
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
/** Escape a value for use in HTML text and in quoted attribute values. */
function escapeHtml(value) {
    return String(value ?? '')
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
    elements.sentenceInput = document.getElementById('sentenceInput');
    elements.storyPromptInput = document.getElementById('storyPromptInput');
    elements.generateStoryButton = document.getElementById('generateStoryButton');
    elements.copyLLMPromptButton = document.getElementById('copyLLMPromptButton');
    elements.insertResponseButton = document.getElementById('insertResponseButton');
    elements.insertResponseContainer = document.getElementById('insertResponseContainer');
    elements.llmResponseInput = document.getElementById('llmResponseInput');
    elements.processResponseButton = document.getElementById('processResponseButton');
    elements.cancelInsertResponseButton = document.getElementById('cancelInsertResponseButton');
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
    elements.createCollectionButton = document.getElementById('createCollectionButton');
    elements.collectionsList = document.getElementById('collectionsList');
    elements.collectionsEmpty = document.getElementById('collectionsEmpty');
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
    elements.timelineUndoButton = document.getElementById('timelineUndoButton');
    elements.timelineRedoButton = document.getElementById('timelineRedoButton');
    elements.timelineRestoreButton = document.getElementById('timelineRestoreButton');
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
                // Pure CSS maths (9px track padding on each side): correct even while the slider is hidden
                const minPercent = (minMs - minRange) / (maxRange - minRange);
                const maxPercent = (maxMs - minRange) / (maxRange - minRange);
                silenceRangeFill.style.left = `calc(9px + (100% - 18px) * ${minPercent})`;
                silenceRangeFill.style.width = `calc((100% - 18px) * ${maxPercent - minPercent})`;
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

    // Initialize autocomplete container for sentence input
    if (elements.sentenceInput) {
        // Create autocomplete container
        sentenceAutocompleteContainer = document.createElement('div');
        sentenceAutocompleteContainer.id = 'sentenceAutocomplete';
        sentenceAutocompleteContainer.className = 'autocomplete-suggestions';
        sentenceAutocompleteContainer.style.cssText = 'position: absolute; z-index: 1000; max-height: 300px; overflow-y: auto; background: var(--bg-secondary, #1a1a1a); border: 1px solid var(--border, #333); border-radius: 4px; display: none; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);';
        elements.sentenceInput.parentElement.style.position = 'relative';
        elements.sentenceInput.parentElement.appendChild(sentenceAutocompleteContainer);

        const onSentenceInput = event => {
            if (event && event.isComposing) {
                return;     // an IME composition is in progress: handled on compositionend
            }
            const currentValue = elements.sentenceInput.value.trim();
            // Reset accepted text if user manually edited (not from Tab completion)
            if (!sentenceAutocompleteState.isTabCompletion) {
                sentenceAutocompleteState.currentAcceptedText = currentValue;
            }
            sentenceAutocompleteState.isTabCompletion = false;  // Reset flag
            sentenceAutocompleteState.pasted = Boolean(event && event.inputType === 'insertFromPaste');

            // Clear auto-semicolon undo state on manual input (but not if it just happened)
            // Check if this input is NOT from the auto-semicolon we just set (within 50ms)
            const autoSemicolon = sentenceAutocompleteState.lastAutoSemicolon;
            if (autoSemicolon && (Date.now() - autoSemicolon.timestamp > 50)) {
                sentenceAutocompleteState.lastAutoSemicolon = null;
            }

            updateProjectData(data => {
                data.currentSentence = currentValue;
            });
            scheduleSentenceAutocomplete();
        };
        elements.sentenceInput.addEventListener('input', onSentenceInput);
        elements.sentenceInput.addEventListener('compositionend', onSentenceInput);

        elements.sentenceInput.addEventListener('keydown', event => {
            if (event.isComposing || event.keyCode === 229) {
                return;     // keys that belong to an IME composition (Enter confirms a candidate)
            }
            if (event.key === 'Enter') {
                event.preventDefault();
                cancelScheduledSentenceAutocomplete();
                // A suggestion chosen with the arrow keys is taken over; a merely hovered one is not.
                const selected = sentenceAutocompleteState.isVisible && sentenceAutocompleteState.keyboardSelection
                    ? sentenceAutocompleteState.suggestions[sentenceAutocompleteState.selectedIndex]
                    : null;
                if (selected) {
                    // Get the text without context prefix (main text only)
                    let textToInsert = selected.current;
                    if (selected.prevContext && textToInsert.toLowerCase().startsWith(selected.prevContext.toLowerCase())) {
                        textToInsert = textToInsert.substring(selected.prevContext.length).trimStart();
                    }
                    selectSentenceSuggestion(textToInsert);
                }
                handleSentenceSearch();
                hideSentenceAutocomplete();
            } else if (event.key === 'Tab') {
                if (event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) {
                    return;                             // leave the field as usual
                }
                cancelScheduledSentenceAutocomplete();
                updateSentenceAutocomplete();           // suggestions for the text as it is right now
                if (!sentenceAutocompleteState.isVisible) {
                    return;                             // nothing to complete: Tab moves to the next control
                }
                event.preventDefault();
                acceptNextWord();
            } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                if (sentenceAutocompleteState.isVisible) {
                    event.preventDefault();             // otherwise the caret moves between the lines
                    navigateSentenceAutocomplete(event.key === 'ArrowDown' ? 1 : -1);
                }
            } else if (event.key === 'Escape') {
                cancelScheduledSentenceAutocomplete();
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
    
    if (elements.createCollectionButton) {
        elements.createCollectionButton.addEventListener('click', handleCreateCollection);
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
    if (elements.timelineUndoButton) {
        elements.timelineUndoButton.addEventListener('click', undoTimeline);
    }
    if (elements.timelineRedoButton) {
        elements.timelineRedoButton.addEventListener('click', redoTimeline);
    }

    if (elements.timelineRestoreButton) {
        elements.timelineRestoreButton.addEventListener('click', restoreMissingClips);
    }

    if (elements.timelineList) {
        elements.timelineList.addEventListener('click', handleTimelineListClick);
        elements.timelineList.addEventListener('keydown', handleTimelineListKeydown);
        elements.timelineList.addEventListener('dragenter', handleTimelineDragEnter);
        elements.timelineList.addEventListener('dragover', handleTimelineDragOver);
        elements.timelineList.addEventListener('dragleave', handleTimelineDragLeave);
        elements.timelineList.addEventListener('drop', handleTimelineDrop);
    }
    if (elements.loopMergedVideo && elements.videoPlayer) {
        // Also takes effect on a merged video that is already playing
        elements.loopMergedVideo.addEventListener('change', () => {
            elements.videoPlayer.loop = elements.loopMergedVideo.checked;
        });
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
        'searchSentenceButton',
        'startOverButton',
        'playAllButton',
        'addAllMatchesButton',
        'timelinePlayButton',
        'timelineMergeButton',
        'timelineClearButton',
        'sentenceInput'
    ];

    toggledControls.forEach(key => {
        if (elements[key]) {
            elements[key].disabled = !hasProject;
        }
    });
    updateTimelineActionButtons();      // Play/Merge also need an enabled clip (and no merge in progress)

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

let libraryLoadFailed = false;

async function fetchLibraryFiles() {
    const button = elements.refreshFilesButton;
    if (button) {
        button.disabled = true;
    }
    try {
        const response = await fetch('/get_files');
        if (!response.ok) {
            throw new Error(`Failed to fetch files: ${response.status}`);
        }
        const data = await response.json();
        libraryFiles = Array.isArray(data) ? data : [];
        libraryLoadFailed = false;
    } catch (error) {
        console.error('Error fetching files:', error);
        libraryLoadFailed = true;
    } finally {
        if (button) {
            button.disabled = false;
        }
        renderFilePickers();
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
        
        // Load collections
        await fetchCollections();

        if (projectList.length) {
            // Try to restore the last selected project from localStorage
            const savedProjectId = readStored(window.localStorage, 'lastSelectedProjectId');
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
    const previous = activeProject;
    try {
        // Leaving a project: its pending changes have to be on the server first.
        if (previous && !(await flushAutosave())) {
            const discard = confirm(`The latest changes to "${previous.name || 'Untitled Project'}" could not be saved.\n\nSwitch anyway and lose them?`);
            if (!discard) {
                renderProjectPicker();
                return;
            }
        }
        // Nothing of the previous project may arrive in the next one (search results, a story, a preview)
        cancelActiveSearch();
        stopClipPlayback();
        clearMergedOutput();
        hideFloatingPreview();
        setAutosaveStatus('Loading project…', 'saving');
        const response = await fetch(`/api/projects/${projectId}`);
        if (response.status === 404) {
            projectList = projectList.filter(item => item.id !== projectId);
        }
        if (!response.ok) {
            throw new Error(`Failed to load project: ${response.status}`);
        }
        const project = await response.json();
        // Edits made while the request was on the wire still belong to the previous project
        if (previous && activeProject === previous) {
            await flushAutosave();
        }
        cancelAutosave();
        autosaveFailed = false;
        activeProject = project;
        try {
            applyProjectToUI(project);
        } catch (error) {
            // Do not stay half-way in a project that cannot be shown
            activeProject = previous;
            if (previous) {
                applyProjectToUI(previous);
            }
            throw error;
        }
        writeStored(window.localStorage, 'lastSelectedProjectId', projectId);
        updateProjectListEntry(project);
        renderProjectPicker();
        // Autoload corpus after project is loaded
        handleLoadSentences();
    } catch (error) {
        console.error('Failed to load project', error);
        renderProjectPicker();      // the picker must show the project that is really open
        setAutosaveStatus(
            activeProject ? `Could not open the project - still in "${activeProject.name || 'Untitled Project'}"` : 'Failed to load project',
            'error'
        );
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
        cancelAutosave();
        if (autosaveRequest) {
            await autosaveRequest;
        }
        cancelAutosave();
        cancelActiveSearch();
        stopClipPlayback();
        clearMergedOutput();
        hideFloatingPreview();
        const response = await fetch(`/api/projects/${activeProject.id}`, { method: 'DELETE' });
        // 404: already deleted elsewhere, which is what was asked for
        if (!response.ok && response.status !== 404) {
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
    try {
        applyProjectData(project);
    } finally {
        // A render error must not leave autosave switched off for good
        isApplyingProject = false;
    }
}

function applyProjectData(project) {
    const data = project.data || {};

    if (elements.projectSelect) {
        elements.projectSelect.value = project.id;
    }

    if (elements.projectNameInput) {
        elements.projectNameInput.value = project.name || '';
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
    if (storyPromptInput) {
        // Always assign: the previous project's prompt must not stay in the field
        storyPromptInput.value = typeof data.storyPrompt === 'string' ? data.storyPrompt : '';
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
    resetTimelineHistory();
    renderTimeline(data.timeline);
    updateProjectControlsState();
    setAutosaveStatus('All changes saved', 'ready');
}

function clearWorkspaceInputs() {
    if (elements.projectNameInput) {
        elements.projectNameInput.value = '';
    }
    if (elements.sentenceInput) {
        elements.sentenceInput.value = '';
    }
    if (elements.storyPromptInput) {
        elements.storyPromptInput.value = '';
    }
    if (elements.llmResponseInput) {
        elements.llmResponseInput.value = '';
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
    autosaveRevision += 1;
    clearTimeout(autosaveTimer);
    setAutosaveStatus('Unsaved changes', 'dirty');
    autosaveTimer = setTimeout(runAutosave, immediate ? 0 : AUTOSAVE_DELAY);
}

// Browsers only let a request outlive the page (keepalive) while its body is below 64 KiB.
const KEEPALIVE_BYTES = 60000;

function projectSaveBody(project, force = false) {
    const payload = {
        name: normalizeProjectName(project.name),
        data: project.data || {}
    };
    // The server refuses the save if the project has changed since this revision (another tab or window).
    if (!force && Number.isInteger(project.revision)) {
        payload.baseRevision = project.revision;
    }
    return JSON.stringify(payload);
}

function putProject(project, { keepalive = false, force = false } = {}) {
    const body = projectSaveBody(project, force);
    return fetch(`/api/projects/${project.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body,
        keepalive: keepalive && new Blob([body]).size < KEEPALIVE_BYTES
    });
}

/**
 * Save the active project. The local project object stays the source of truth (the server's echo is not
 * swapped in), so edits made while a save is on the wire are kept and trigger a follow-up save.
 */
async function runAutosave(keepalive = false) {
    clearTimeout(autosaveTimer);
    autosaveTimer = null;
    if (!activeProject || isApplyingProject) {
        return;
    }
    if (autosaveRequest) {
        // One save at a time; the running one re-runs when it sees a newer revision.
        return autosaveRequest;
    }
    const project = activeProject;
    const revision = autosaveRevision;
    setAutosaveStatus('Saving…', 'saving');
    autosaveRequest = (async () => {
        let outcome = 'saved';
        let serverProject = null;
        try {
            const response = await putProject(project, { keepalive: keepalive === true });
            if (response.status === 404) {
                outcome = 'gone';
            } else if (response.status === 409) {
                outcome = 'conflict';
                serverProject = (await response.json().catch(() => ({}))).project || null;
            } else if (!response.ok) {
                throw new Error(`Autosave failed: ${response.status}`);
            } else {
                const updated = await response.json();
                project.updatedAt = updated.updatedAt;
                project.revision = updated.revision;
                updateProjectListEntry(project);
                refreshActiveProjectOption();
            }
        } catch (error) {
            outcome = 'failed';
            console.error('Autosave failed', error);
        }
        autosaveRequest = null;
        if (project !== activeProject) {
            return;
        }
        autosaveFailed = outcome !== 'saved';
        if (outcome === 'gone') {
            handleProjectDeletedElsewhere(project);
        } else if (outcome === 'conflict') {
            handleProjectChangedElsewhere(project, serverProject);
        } else if (outcome === 'failed') {
            setAutosaveStatus('Autosave failed, retrying…', 'error');
            autosaveTimer = setTimeout(runAutosave, 5000);
        } else if (autosaveRevision !== revision) {
            setAutosaveStatus('Unsaved changes', 'dirty');
            if (!autosaveTimer) {
                autosaveTimer = setTimeout(runAutosave, 0);
            }
        } else {
            setAutosaveStatus('All changes saved', 'ready');
        }
    })();
    return autosaveRequest;
}

/** The same project was saved from another tab or window after this one loaded it. */
function handleProjectChangedElsewhere(project, serverProject) {
    setAutosaveStatus('Changed in another tab or window', 'error');
    if (!serverProject) {
        return;
    }
    const takeTheirs = confirm(
        `"${project.name || 'Untitled Project'}" was changed in another tab or window after this one loaded it.\n\n` +
        'OK: load that newer version here (changes made in this tab since then are dropped).\n' +
        'Cancel: keep this tab\'s version and overwrite the other one.'
    );
    if (project !== activeProject) {
        return;
    }
    autosaveFailed = false;
    if (takeTheirs) {
        cancelAutosave();
        stopClipPlayback();
        activeProject = serverProject;
        applyProjectToUI(serverProject);
        updateProjectListEntry(serverProject);
        refreshActiveProjectOption();
    } else {
        project.revision = serverProject.revision;
        scheduleAutosave(true);
    }
}

/** The project no longer exists on the server (deleted in another tab): keep the work or move on. */
async function handleProjectDeletedElsewhere(project) {
    setAutosaveStatus('This project was deleted elsewhere', 'error');
    projectList = projectList.filter(item => item.id !== project.id);
    const keep = confirm(`"${project.name || 'Untitled Project'}" was deleted in another tab or window.\n\nOK: keep this tab's copy as a new project.\nCancel: discard it.`);
    if (project !== activeProject) {
        return;
    }
    try {
        cancelAutosave();
        autosaveFailed = false;
        activeProject = null;
        if (keep) {
            const created = await createProjectOnServer(project.name, project.data);
            projectList.push(buildProjectSummary(created));
            await loadProject(created.id);
        } else if (projectList.length) {
            await loadProject(projectList[0].id);
        } else {
            await handleCreateProject();
        }
    } catch (error) {
        console.error('Could not recover from the deleted project', error);
        setAutosaveStatus('Failed to load projects', 'error');
    }
}

/**
 * Write pending changes now (before switching, duplicating or leaving the project).
 * Resolves to false if they could not be saved.
 */
async function flushAutosave() {
    for (let attempt = 0; attempt < 3; attempt++) {
        if (autosaveRequest) {
            await autosaveRequest;
        }
        if (!autosaveTimer) {
            break;
        }
        await runAutosave();
    }
    return !autosaveTimer && !autosaveRequest && !autosaveFailed;
}

function cancelAutosave() {
    clearTimeout(autosaveTimer);
    autosaveTimer = null;
}

function hasUnsavedChanges() {
    return Boolean(activeProject) && (autosaveTimer !== null || autosaveRequest !== null);
}

// Hiding the tab: save now rather than after the autosave delay.
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && activeProject && autosaveTimer) {
        runAutosave(true);
    }
});

// Leaving the page within the autosave delay must not lose the last edits.
window.addEventListener('pagehide', () => {
    if (!hasUnsavedChanges()) {
        return;
    }
    if (!autosaveRequest) {
        runAutosave(true);
    } else if (autosaveTimer) {
        // A save is on the wire but does not contain the latest edits: send the current state as well. Both
        // are based on the same revision, so this one skips the revision check.
        cancelAutosave();
        putProject(activeProject, { keepalive: true, force: true }).catch(() => { /* the page is going away */ });
    }
});

// A project too large for a keepalive request cannot be saved while the page unloads: ask before leaving.
window.addEventListener('beforeunload', event => {
    if (hasUnsavedChanges() && new Blob([projectSaveBody(activeProject)]).size >= KEEPALIVE_BYTES) {
        event.preventDefault();
        event.returnValue = '';
    }
});

// Back from the back/forward cache: the project may have been saved since (see pagehide), so reload it.
window.addEventListener('pageshow', event => {
    if (event.persisted && activeProject) {
        loadProject(activeProject.id);
    }
});

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

    const librarySet = new Set(libraryFiles);
    chosenFiles.forEach(file => {
        const option = new Option(file, file);
        if (!libraryLoadFailed && !librarySet.has(file)) {
            option.textContent = `${file} (missing)`;
            option.dataset.missing = 'true';
        }
        chosenFragment.appendChild(option);
    });

    if (libraryLoadFailed) {
        const notice = new Option('Could not load the library - click Refresh', '');
        notice.disabled = true;
        availableFragment.appendChild(notice);
    } else if (!libraryFiles.length) {
        const notice = new Option('No videos found in the library folder(s)', '');
        notice.disabled = true;
        availableFragment.appendChild(notice);
    }
    libraryFiles.forEach(file => {
        if (!chosenSet.has(file)) {
            availableFragment.appendChild(new Option(file, file));
        }
    });

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

// Collections management
let collections = [];

async function fetchCollections() {
    try {
        const response = await fetch('/api/collections');
        if (!response.ok) {
            throw new Error(`Failed to fetch collections: ${response.status}`);
        }
        const data = await response.json();
        collections = Array.isArray(data.collections) ? data.collections : [];
        renderCollections();
    } catch (error) {
        console.error('Error fetching collections:', error);
    }
}

function renderCollections() {
    if (!elements.collectionsList || !elements.collectionsEmpty) {
        return;
    }
    
    // The hint lives inside the list: keep it attached, replace only the items
    elements.collectionsList.replaceChildren(elements.collectionsEmpty);
    
    if (collections.length === 0) {
        elements.collectionsEmpty.style.display = 'block';
        return;
    }
    
    elements.collectionsEmpty.style.display = 'none';
    
    collections.forEach(collection => {
        const collectionItem = document.createElement('div');
        collectionItem.className = 'collection-item';
        collectionItem.dataset.collectionId = collection.id;
        
        const fileCount = Array.isArray(collection.files) ? collection.files.length : 0;
        const createdDate = collection.createdAt ? new Date(collection.createdAt).toLocaleDateString() : '';
        
        collectionItem.innerHTML = `
            <div class="collection-item-header">
                <div class="collection-item-info">
                    <div class="collection-item-name">${escapeHtml(collection.name)}</div>
                    <div class="collection-item-meta">${fileCount} file${fileCount !== 1 ? 's' : ''} • ${escapeHtml(createdDate)}</div>
                </div>
                <div class="collection-item-actions">
                    <button class="collection-load-button collection-add-button ghost-button" data-collection-id="${escapeHtml(collection.id)}" title="Add this collection's files to the project">
                        Add
                    </button>
                    <button class="collection-load-button collection-replace-button ghost-button" data-collection-id="${escapeHtml(collection.id)}" title="Replace the project's files with this collection">
                        Replace
                    </button>
                    <button class="collection-delete-button ghost-button danger" data-collection-id="${escapeHtml(collection.id)}" title="Delete collection">
                        Delete
                    </button>
                </div>
            </div>
        `;
        
        const addButton = collectionItem.querySelector('.collection-add-button');
        const replaceButton = collectionItem.querySelector('.collection-replace-button');
        const deleteButton = collectionItem.querySelector('.collection-delete-button');
        
        addButton.addEventListener('click', () => handleLoadCollection(collection.id, false));
        replaceButton.addEventListener('click', () => handleLoadCollection(collection.id, true));
        deleteButton.addEventListener('click', () => handleDeleteCollection(collection.id));
        
        elements.collectionsList.appendChild(collectionItem);
    });
}

async function handleCreateCollection() {
    if (!activeProject) {
        alert('Please create or select a project first');
        return;
    }
    
    const projectFiles = Array.isArray(activeProject?.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    
    if (projectFiles.length === 0) {
        alert('No files in current project. Add files to create a collection.');
        return;
    }
    
    const name = prompt('Enter collection name:');
    if (!name || !name.trim()) {
        return;
    }
    
    const collectionName = name.trim();
    const existing = collections.find(collection => collection.name === collectionName);
    if (existing && !confirm(`A collection named "${collectionName}" already exists. Update it with the current ${projectFiles.length} file(s)?`)) {
        return;
    }
    
    try {
        const response = await fetch('/api/collections', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name: collectionName,
                files: projectFiles,
                overwrite: Boolean(existing)
            })
        });
        
        if (!response.ok) {
            const error = await response.json().catch(() => ({}));
            alert(error.error || 'Failed to create collection');
            return;
        }
        
        await fetchCollections();
    } catch (error) {
        console.error('Error creating collection:', error);
        alert('Failed to create collection');
    }
}

async function handleLoadCollection(collectionId, replace = false) {
    if (!activeProject) {
        alert('Please create or select a project first');
        return;
    }
    
    const collection = collections.find(c => c.id === collectionId);
    if (!collection) {
        alert('Collection not found');
        return;
    }
    
    const collectionFiles = Array.isArray(collection.files) ? collection.files : [];
    if (collectionFiles.length === 0) {
        alert('Collection is empty');
        return;
    }
    
    const currentFiles = Array.isArray(activeProject?.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    // Replacing drops the current file list: ask, and treat Cancel/Escape as "do nothing".
    if (replace && currentFiles.length > 0 &&
        !confirm(`Replace the ${currentFiles.length} file(s) of this project with the ${collectionFiles.length} file(s) of "${collection.name}"?`)) {
        return;
    }
    
    updateProjectData(data => {
        if (replace) {
            data.selectedFiles = [...collectionFiles];
        } else {
            // Add the files that are not in the project yet
            const current = Array.isArray(data.selectedFiles) ? data.selectedFiles : [];
            const currentSet = new Set(current);
            collectionFiles.forEach(file => {
                if (!currentSet.has(file)) {
                    current.push(file);
                }
            });
            data.selectedFiles = current;
        }
    });
    
    renderFilePickers();
    resetCorpusState();
    // Automatically reload corpus with new file list
    handleLoadSentences();
}

async function handleDeleteCollection(collectionId) {
    const collection = collections.find(c => c.id === collectionId);
    if (!collection) {
        return;
    }
    
    if (!confirm(`Delete collection "${collection.name}"?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/collections/${collectionId}`, {
            method: 'DELETE'
        });
        
        if (!response.ok) {
            const error = await response.json();
            alert(error.error || 'Failed to delete collection');
            return;
        }
        
        await fetchCollections();
    } catch (error) {
        console.error('Error deleting collection:', error);
        alert('Failed to delete collection');
    }
}

function getSilencePreferences() {
    let minSilence = elements.minSilenceInput ? parseFloat(elements.minSilenceInput.value) : 0;
    if (!Number.isFinite(minSilence) || minSilence < 0) {
        minSilence = 0;
    }

    // An emptied field means "default" (10 s / 2 words), not 0
    let maxSilence = elements.maxSilenceInput ? parseFloat(elements.maxSilenceInput.value) : 10;
    if (!Number.isFinite(maxSilence)) {
        maxSilence = 10;
    }
    if (maxSilence < minSilence) {
        maxSilence = minSilence;
    }

    let threshold = elements.silenceWordThresholdInput ? parseInt(elements.silenceWordThresholdInput.value, 10) : 2;
    if (!Number.isInteger(threshold)) {
        threshold = 2;
    }
    if (threshold < 1) {
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
    hideSearchLoading();
    revealResultsSection();

    const loadingDiv = document.createElement('div');
    loadingDiv.id = 'searchLoadingIndicator';
    loadingDiv.className = 'search-loading';
    loadingDiv.setAttribute('role', 'status');
    const spinner = document.createElement('div');
    spinner.className = 'loading-spinner';
    const messageContainer = document.createElement('div');
    messageContainer.style.cssText = 'display: flex; align-items: center; gap: 10px;';
    const text = document.createElement('p');
    text.style.cssText = 'margin: 0; flex: 1;';
    text.textContent = message;
    messageContainer.appendChild(text);

    if (onCancel) {
        const cancelBtn = document.createElement('button');
        cancelBtn.type = 'button';
        cancelBtn.className = 'button ghost-button';
        cancelBtn.textContent = 'Cancel Search';
        cancelBtn.style.cssText = 'padding: 4px 12px; font-size: 12px; min-width: auto;';
        cancelBtn.onclick = onCancel;
        messageContainer.appendChild(cancelBtn);
    }

    loadingDiv.append(spinner, messageContainer);
    elements.resultsContainer.appendChild(loadingDiv);
}

function hideSearchLoading() {
    const loadingDiv = document.getElementById('searchLoadingIndicator');
    if (loadingDiv) {
        loadingDiv.remove();
    }
}

/** On the narrow layout only one section is visible: bring the one with the search feedback to the front. */
function revealResultsSection() {
    if (isMobileLayout()) {
        switchMobileSection('results');
        return;
    }
    // Desktop: the Matches card is often below the fold, where a spinner or "No matches" would go unseen
    const section = elements.resultsContainer ? elements.resultsContainer.closest('.card') : null;
    if (section && section.getBoundingClientRect().top > window.innerHeight - 200) {
        section.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
}

/** Mark the search buttons while a search runs (the spinner itself may be off screen). */
function setSearchBusy(busy) {
    document.querySelectorAll('#searchLongestSegmentsButton, #searchSilenceButton, #generateStoryButton').forEach(button => {
        button.classList.toggle('is-busy', busy);
        button.setAttribute('aria-busy', busy ? 'true' : 'false');
    });
}

/** Refuse a search with a reason, keeping the results that are on screen. */
function rejectSearch(message) {
    document.querySelectorAll('.result-notice.search-rejected').forEach(notice => notice.remove());
    const notice = document.createElement('div');
    notice.className = 'result-notice search-rejected';
    notice.setAttribute('role', 'alert');
    notice.textContent = message;
    elements.resultsContainer.prepend(notice);
    revealResultsSection();
}

/** Floating progress box of a running merge: what the server is doing, a bar and a cancel button. */
function showMergeProgress(message, progress, total, onCancel = null) {
    hideMergeProgress();

    const progressDiv = document.createElement('div');
    progressDiv.id = 'mergeProgressIndicator';
    progressDiv.className = 'merge-progress';
    progressDiv.setAttribute('role', 'status');

    const header = document.createElement('div');
    header.className = 'merge-progress-header';
    const spinner = document.createElement('div');
    spinner.className = 'loading-spinner';
    const texts = document.createElement('div');
    texts.className = 'merge-progress-texts';
    const title = document.createElement('p');
    title.className = 'merge-progress-title';
    title.textContent = 'Merging timeline';
    const detail = document.createElement('p');
    detail.className = 'merge-progress-detail';
    texts.append(title, detail);
    header.append(spinner, texts);

    const track = document.createElement('div');
    track.className = 'merge-progress-track';
    const bar = document.createElement('div');
    bar.className = 'merge-progress-bar';
    track.appendChild(bar);

    progressDiv.append(header, track);
    if (onCancel) {
        const cancelButton = document.createElement('button');
        cancelButton.type = 'button';
        cancelButton.className = 'ghost-button merge-progress-cancel';
        cancelButton.textContent = 'Cancel merge';
        cancelButton.addEventListener('click', onCancel);
        progressDiv.appendChild(cancelButton);
    }
    document.body.appendChild(progressDiv);
    updateMergeProgress(message, progress, total);
}

function updateMergeProgress(message, progress, total) {
    const progressDiv = document.getElementById('mergeProgressIndicator');
    if (!progressDiv) {
        return;
    }
    const percentage = total > 0 ? Math.max(0, Math.min(100, Math.round((progress / total) * 100))) : 0;
    progressDiv.querySelector('.merge-progress-detail').textContent = message ? `${message} (${percentage}%)` : `${percentage}%`;
    progressDiv.querySelector('.merge-progress-bar').style.width = `${percentage}%`;
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

        messageEl.textContent = displayMessage;
    } else {
        console.warn('[updateSearchLoadingMessage] Message element not found');
    }
    
    // One skip button per segment: progress events arrive several times a second, and rebuilding the button
    // on each of them swallowed clicks and reset its "Skipping..." state.
    const segmentKey = segmentData && segmentData.segment_index
        ? `${segmentData.search_id || ''}|${segmentData.segment_phrase || segmentData.segment_index}`
        : '';
    const existingBtn = messageContainer.querySelector('.skip-segment-btn');
    if (existingBtn && segmentKey && existingBtn.dataset.segment === segmentKey) {
        return;
    }
    if (existingBtn) {
        existingBtn.remove();
    }
    
    // Add skip button if we have segment data
    if (segmentKey) {
        const skipBtn = document.createElement('button');
        skipBtn.dataset.segment = segmentKey;
        skipBtn.className = 'button ghost-button skip-segment-btn';
        skipBtn.textContent = 'Skip Segment';
        skipBtn.title = 'Skip this segment immediately (no clips will be rendered)';
        skipBtn.style.cssText = 'padding: 4px 12px; font-size: 12px; min-width: auto;';
        skipBtn.onclick = () => {
            // The server sends the phrase with the progress event. (Cutting it out of the message between
            // quotes breaks on apostrophes: "don't stop" became "don".)
            const quoted = message.match(/'(.+)'/);
            const segmentPhrase = segmentData.segment_phrase || (quoted ? quoted[1] : '');
            if (!segmentPhrase || !window.currentSearchId) {
                console.error('[Client] Cannot skip: missing segment text or search ID');
                return;
            }

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
    
    revealResultsSection();

    // Remove any existing no results message
    const existingMessage = document.getElementById('noResultsMessage');
    if (existingMessage) {
        existingMessage.remove();
    }
    
    // Create no results message
    const messageDiv = document.createElement('div');
    messageDiv.id = 'noResultsMessage';
    messageDiv.className = 'no-results-message';
    const text = document.createElement('p');
    text.textContent = message;
    messageDiv.appendChild(text);
    
    elements.resultsContainer.appendChild(messageDiv);
}

// Helper function to get video duration
// ---------------------------------------------------------------------------------------------------------
// Clip model
//
// A clip is identified by a "clip reference": the exported preview file in temp/ plus, when known, the library
// source and the bounds it was cut from. Trims are whole milliseconds removed from the start / end of that clip.
// Preview (clip-player.js), waveform and merge all resolve the same reference on the server, so a trim value
// means the same sample everywhere.
// ---------------------------------------------------------------------------------------------------------

function makeClipRef(source) {
    const start = Number.parseFloat(source.originalStart);
    const end = Number.parseFloat(source.originalEnd);
    return {
        clip: source.clip || '',
        source: source.source || '',
        originalStart: Number.isFinite(start) ? start : null,
        originalEnd: Number.isFinite(end) ? end : null
    };
}

function clipRefKey(ref) {
    return [ref.clip, ref.source, ref.originalStart, ref.originalEnd].join('|');
}

/**
 * Keep trims non-negative and leave at least MIN_PLAYABLE_MS of the clip.
 * `moved` names the handle the user is dragging ('start' | 'end'): only that one gives way. Without it (values
 * loaded from a project, defaults on a very short clip) both are scaled down proportionally.
 */
function clampTrims(startTrim, endTrim, durationMs, moved = null) {
    let start = Math.max(0, Math.round(Number(startTrim) || 0));
    let end = Math.max(0, Math.round(Number(endTrim) || 0));
    if (!Number.isFinite(durationMs) || durationMs <= 0) {
        return { start, end };
    }
    const budget = Math.max(0, Math.floor(durationMs - MIN_PLAYABLE_MS));
    if (start + end <= budget) {
        return { start, end };
    }
    if (moved === 'start') {
        end = Math.min(end, budget);
        start = budget - end;
    } else if (moved === 'end') {
        start = Math.min(start, budget);
        end = budget - start;
    } else {
        const total = start + end;
        start = Math.floor(budget * (start / total));
        end = Math.min(end, budget - start);
    }
    return { start, end };
}

// ---------------------------------------------------------------------------------------------------------
// Waveform with trim handles
// ---------------------------------------------------------------------------------------------------------

const WAVEFORM_POINTS = 800;
const WAVEFORM_CACHE_LIMIT = 2000;
const waveformCache = new Map();   // clip reference key -> Promise<{ waveform, durationMs }>

function fetchWaveformData(ref) {
    const key = clipRefKey(ref);
    if (waveformCache.has(key)) {
        return waveformCache.get(key);
    }
    const request = fetch('/get_waveform', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...ref, width: WAVEFORM_POINTS })
    })
        .then(async response => {
            const data = await response.json().catch(() => ({}));
            if (!response.ok || data.error || !Array.isArray(data.waveform)) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }
            return { waveform: data.waveform, durationMs: Number(data.duration_ms) || 0 };
        })
        .catch(error => {
            waveformCache.delete(key);
            throw error;
        });
    if (waveformCache.size >= WAVEFORM_CACHE_LIMIT) {
        waveformCache.delete(waveformCache.keys().next().value);
    }
    waveformCache.set(key, request);
    return request;
}

/**
 * Waveform of one clip with a start and an end trim handle.
 *
 * The clip length comes from the server together with the waveform (it is the length of the audio the preview
 * plays), so the handles, the picture and the sound share one time base.
 *
 * Callbacks: onTrimChange(start, end, byUser) on every change, onTrimRelease(start, end) when the user lets go
 * (pointer up, or shortly after the last key press) - that is the moment to preview.
 * Keys on a focused handle: arrows 10 ms, Shift+arrows 1 ms, PageUp/PageDown 100 ms.
 */
function createWaveformVisualization({
    clipRef = null,
    startTrim = 0,
    endTrim = 0,
    speed = 1,
    onTrimChange = null,
    onTrimRelease = null
} = {}) {
    const container = document.createElement('div');
    container.className = 'waveform-container';

    const canvas = document.createElement('canvas');
    canvas.className = 'waveform-canvas';
    canvas.width = WAVEFORM_POINTS;
    canvas.height = 120;

    const selection = document.createElement('div');
    selection.className = 'waveform-selection';

    const playhead = document.createElement('div');
    playhead.className = 'waveform-playhead';
    playhead.hidden = true;

    const makeHandle = (side, label) => {
        const handle = document.createElement('div');
        handle.className = `waveform-handle waveform-handle--${side}`;
        handle.setAttribute('role', 'slider');
        handle.setAttribute('aria-label', label);
        handle.setAttribute('tabindex', '0');
        handle.setAttribute('draggable', 'false');
        handle.title = `${label}: drag, or arrow keys (10 ms, Shift 1 ms, PageUp/PageDown 100 ms)`;
        return handle;
    };
    const startHandle = makeHandle('start', 'Start trim');
    const endHandle = makeHandle('end', 'End trim');

    const startLabel = document.createElement('div');
    startLabel.className = 'waveform-label waveform-label--start';
    const endLabel = document.createElement('div');
    endLabel.className = 'waveform-label waveform-label--end';
    const durationInfo = document.createElement('div');
    durationInfo.className = 'waveform-duration-info';

    container.append(canvas, selection, playhead, startHandle, endHandle, startLabel, endLabel, durationInfo);

    const state = {
        ref: null,
        requestId: 0,
        data: null,
        status: 'empty',       // 'empty' | 'loading' | 'ready' | 'error'
        durationMs: 0,
        start: Math.max(0, Math.round(startTrim) || 0),
        end: Math.max(0, Math.round(endTrim) || 0),
        speed,
        dragging: null,        // 'start' | 'end'
        dragOffsetPx: 0,
        keyTimer: null
    };
    const context = canvas.getContext('2d');

    function draw() {
        const { width, height } = canvas;
        context.clearRect(0, 0, width, height);
        context.strokeStyle = '#6b5ce6';
        const centerY = height / 2;
        if (!state.data || !state.data.length) {
            // No fake waveform while loading or after an error: a flat line is honest.
            context.globalAlpha = 0.35;
            context.lineWidth = 1;
            context.beginPath();
            context.moveTo(0, centerY);
            context.lineTo(width, centerY);
            context.stroke();
            context.globalAlpha = 1;
            return;
        }
        const scale = height * 0.4;
        const columns = Math.max(1, Math.floor(width));
        for (let x = 0; x < columns; x++) {
            // Peak of the source points covered by this pixel column, so short bursts stay visible when shrunk.
            const from = Math.floor((x / columns) * state.data.length);
            const to = Math.max(from + 1, Math.floor(((x + 1) / columns) * state.data.length));
            let value = 0;
            for (let index = from; index < to && index < state.data.length; index++) {
                if (state.data[index] > value) value = state.data[index];
            }
            context.lineWidth = 0.5 + value;
            context.beginPath();
            context.moveTo(x + 0.5, centerY - value * scale);
            context.lineTo(x + 0.5, centerY + value * scale);
            context.stroke();
        }
    }

    function resizeCanvas() {
        const rect = canvas.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0) {
            canvas.width = Math.round(rect.width);
            canvas.height = Math.round(rect.height);
            draw();
        }
    }
    new ResizeObserver(resizeCanvas).observe(container);

    function formatMs(value) {
        const shown = state.speed !== 1 ? Math.round(value / state.speed) : value;
        return `${shown}ms`;
    }

    function updateSelection() {
        const duration = state.durationMs;
        const known = duration > 0;
        const startPercent = known ? (state.start / duration) * 100 : 0;
        const endPercent = known ? 100 - (state.end / duration) * 100 : 100;

        selection.style.left = `${startPercent}%`;
        selection.style.width = `${Math.max(0, endPercent - startPercent)}%`;
        startHandle.style.left = `${startPercent}%`;
        endHandle.style.left = `${endPercent}%`;
        startLabel.style.left = `${startPercent}%`;
        endLabel.style.left = `${endPercent}%`;
        startLabel.textContent = formatMs(state.start);
        endLabel.textContent = formatMs(state.end);
        container.classList.toggle('is-loading', state.status === 'loading');
        container.classList.toggle('has-error', state.status === 'error');

        startHandle.setAttribute('aria-valuemin', '0');
        endHandle.setAttribute('aria-valuemin', '0');
        startHandle.setAttribute('aria-valuemax', String(Math.max(0, Math.round(duration - state.end))));
        endHandle.setAttribute('aria-valuemax', String(Math.max(0, Math.round(duration - state.start))));
        startHandle.setAttribute('aria-valuenow', String(state.start));
        endHandle.setAttribute('aria-valuenow', String(state.end));
        startHandle.setAttribute('aria-valuetext', `${state.start} ms cut from the start`);
        endHandle.setAttribute('aria-valuetext', `${state.end} ms cut from the end`);

        if (state.status === 'error') {
            durationInfo.textContent = 'Clip audio unavailable';
        } else if (!known) {
            durationInfo.textContent = state.status === 'loading' ? 'Loading…' : '';
        } else {
            const trimmed = Math.max(0, duration - state.start - state.end);
            durationInfo.textContent = state.speed !== 1
                ? `Original: ${trimmed.toFixed(0)}ms | Playback: ${Math.round(trimmed / state.speed)}ms (${Math.round(state.speed * 100)}%)`
                : `Full: ${duration.toFixed(0)}ms | Trimmed: ${trimmed.toFixed(0)}ms`;
        }
    }

    function updateTrim(newStart, newEnd, silent = false, moved = null) {
        const clamped = clampTrims(newStart, newEnd, state.durationMs, moved);
        const changed = clamped.start !== state.start || clamped.end !== state.end;
        state.start = clamped.start;
        state.end = clamped.end;
        updateSelection();
        if (changed && onTrimChange && !silent) {
            // Third argument: true when the user moved a handle, false when the trims were only corrected
            // to fit the clip.
            onTrimChange(state.start, state.end, moved !== null);
        }
        return changed;
    }

    function release() {
        if (onTrimRelease) {
            onTrimRelease(state.start, state.end);
        }
    }

    function load(ref) {
        const requestId = ++state.requestId;
        state.ref = ref;
        state.data = null;
        state.durationMs = 0;
        if (!ref || !ref.clip) {
            state.status = 'empty';
            draw();
            updateSelection();
            return;
        }
        state.status = 'loading';
        draw();
        updateSelection();
        fetchWaveformData(ref).then(
            result => {
                if (requestId !== state.requestId) return;
                state.status = 'ready';
                state.data = result.waveform;
                state.durationMs = result.durationMs;
                draw();
                // Trims that do not fit this clip (defaults on a very short clip) are corrected and reported.
                updateTrim(state.start, state.end);
            },
            error => {
                if (requestId !== state.requestId) return;
                console.warn('Waveform unavailable:', error.message);
                state.status = 'error';
                draw();
                updateSelection();
            }
        );
    }

    // --- pointer interaction (pointer capture: no document-level listeners to leak) -------------------------

    function handlePositionPx(side, rect) {
        if (!(state.durationMs > 0)) return side === 'start' ? 0 : rect.width;
        return side === 'start'
            ? (state.start / state.durationMs) * rect.width
            : (1 - state.end / state.durationMs) * rect.width;
    }

    function trimAt(side, clientX) {
        const rect = canvas.getBoundingClientRect();
        const fraction = Math.max(0, Math.min(1, (clientX - rect.left - state.dragOffsetPx) / rect.width));
        const value = Math.round((side === 'start' ? fraction : 1 - fraction) * state.durationMs);
        if (side === 'start') {
            updateTrim(value, state.end, false, 'start');
        } else {
            updateTrim(state.start, value, false, 'end');
        }
    }

    container.addEventListener('pointerdown', event => {
        if (event.button !== 0 || !(state.durationMs > 0)) return;
        const rect = canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        let side;
        if (event.target === startHandle) {
            side = 'start';
        } else if (event.target === endHandle) {
            side = 'end';
        } else {
            side = Math.abs(x - handlePositionPx('start', rect)) <= Math.abs(x - handlePositionPx('end', rect)) ? 'start' : 'end';
        }
        // Grabbing a handle keeps the grab offset; clicking the waveform moves the nearest handle to the pointer.
        // A finger on the waveform may be the start of a page scroll, so it never makes a handle jump: it drags
        // the nearest handle from where it is, and a gesture the browser takes over is undone (see endDrag).
        const onHandle = event.target === startHandle || event.target === endHandle;
        const jump = !onHandle && event.pointerType !== 'touch';
        state.dragOffsetPx = jump ? 0 : x - handlePositionPx(side, rect);
        state.dragging = side;
        state.gestureStart = { start: state.start, end: state.end };
        event.preventDefault();
        event.stopPropagation();
        container.setPointerCapture(event.pointerId);
        (side === 'start' ? startHandle : endHandle).focus({ preventScroll: true });
        if (jump) trimAt(side, event.clientX);
    });

    container.addEventListener('pointermove', event => {
        if (state.dragging) trimAt(state.dragging, event.clientX);
    });

    const endDrag = event => {
        if (!state.dragging) return;
        state.dragging = null;
        if (container.hasPointerCapture(event.pointerId)) container.releasePointerCapture(event.pointerId);
        const before = state.gestureStart;
        if (event.type === 'pointercancel') {
            updateTrim(before.start, before.end);       // it became a scroll: nothing was meant to change
            return;
        }
        if (event.pointerType !== 'touch' || state.start !== before.start || state.end !== before.end) {
            release();
        }
    };
    container.addEventListener('pointerup', endDrag);
    container.addEventListener('pointercancel', endDrag);
    // The waveform lives inside draggable cards: never let a trim gesture start a card drag.
    container.addEventListener('dragstart', event => {
        event.preventDefault();
        event.stopPropagation();
    });

    // --- keyboard ---------------------------------------------------------------------------------------

    const onKey = side => event => {
        let step = 0;
        if (event.key === 'ArrowLeft') step = -1;
        else if (event.key === 'ArrowRight') step = 1;
        else if (event.key === 'PageDown') step = -10;
        else if (event.key === 'PageUp') step = 10;
        else return;
        event.preventDefault();
        event.stopPropagation();
        const delta = step * (event.shiftKey ? 1 : 10);
        // Right always moves the handle right: more start trim, less end trim.
        if (side === 'start') {
            updateTrim(state.start + delta, state.end, false, 'start');
        } else {
            updateTrim(state.start, state.end - delta, false, 'end');
        }
        clearTimeout(state.keyTimer);
        state.keyTimer = setTimeout(release, 350);
    };
    startHandle.addEventListener('keydown', onKey('start'));
    endHandle.addEventListener('keydown', onKey('end'));

    // --- public API -------------------------------------------------------------------------------------

    container.getTrim = () => ({ start: state.start, end: state.end });
    container.getDuration = () => state.durationMs;
    /** Show another clip (new match selected) with its own trims. */
    container.setClip = (ref, trims = {}) => {
        state.start = Math.max(0, Math.round(trims.start) || 0);
        state.end = Math.max(0, Math.round(trims.end) || 0);
        load(ref);
    };
    container.setSpeed = newSpeed => {
        state.speed = newSpeed;
        updateSelection();
    };
    /** Position of the play head as a fraction of the trimmed range, or null to hide it. */
    container.setPlayhead = fraction => {
        if (fraction === null || !(state.durationMs > 0)) {
            playhead.hidden = true;
            return;
        }
        const from = state.start / state.durationMs;
        const to = 1 - state.end / state.durationMs;
        playhead.hidden = false;
        playhead.style.left = `${(from + (to - from) * Math.max(0, Math.min(1, fraction))) * 100}%`;
    };

    load(clipRef);
    return container;
}

// ---------------------------------------------------------------------------------------------------------
// Searching
// ---------------------------------------------------------------------------------------------------------

/**
 * Read a text/event-stream response and call onEvent(data) for every `data: {json}` line.
 * Reading stops when the stream ends or onEvent returns false.
 */
async function readEventStream(response, onEvent) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
        for (;;) {
            const { done, value } = await reader.read();
            buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
            const lines = buffer.split('\n');
            buffer = done ? '' : lines.pop();
            for (const line of lines) {
                if (!line.startsWith('data: ')) {
                    continue;
                }
                let data;
                try {
                    data = JSON.parse(line.slice(6));
                } catch (error) {
                    console.error('Malformed event from the server:', line);
                    continue;
                }
                if (onEvent(data) === false) {
                    return;
                }
            }
            if (done) {
                return;
            }
        }
    } finally {
        reader.cancel().catch(() => { /* already closed */ });
    }
}

// Only one search runs at a time: starting a new one (or "Clear results") ends the previous one, so results of
// an older search can never arrive in the list of a newer one.
let activeSearch = null;

function beginSearch() {
    cancelActiveSearch();
    const search = {
        controller: new AbortController(),
        serverId: null,          // id the server reports with its progress events (needed for skip / cancel)
        cancelled: false,
        get isCurrent() {
            return activeSearch === search && !search.cancelled;
        }
    };
    activeSearch = search;
    window.currentSearchId = null;
    setSearchBusy(true);
    revealResultsSection();
    return search;
}

function cancelActiveSearch() {
    const search = activeSearch;
    if (!search) {
        return;
    }
    activeSearch = null;
    search.cancelled = true;
    setSearchBusy(false);
    if (search.serverId) {
        // Tell the server to stop rendering clips right away (closing the connection is only noticed later).
        fetch('/cancel_search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ search_id: search.serverId })
        }).catch(() => { /* the search may already be over */ });
    }
    search.controller.abort();
    hideSearchLoading();
}

function finishSearch(search) {
    if (activeSearch === search) {
        activeSearch = null;
        setSearchBusy(false);
        hideSearchLoading();
    }
}

function getMaxResultsPerSegment() {
    const input = document.getElementById('maxResultsPerSegment');
    const parsed = input ? parseInt(input.value, 10) : NaN;
    const value = Number.isInteger(parsed) && parsed > 0 ? Math.min(parsed, 1000) : 25;
    if (input && String(value) !== input.value) {
        input.value = String(value);
    }
    return value;
}

const WORD_CHARACTER = '[\\p{L}\\p{N}]';
const WILDCARD_EXPANSION_LIMIT = 50;

/**
 * Expand one search term with wildcards against the loaded corpus.
 *   ball*    words that start with "ball" (a * stands for letters inside one word; "der ball*" works too,
 *            and a * on its own stands for one whole word: "der * ist")
 *   ball**   from a word starting with "ball" to the end of its sentence
 *   **ball   from the start of a sentence up to a word ending in "ball"
 *   **ball** whole sentences containing "ball"
 * Every expansion starts and ends on a word boundary, because the server matches whole words.
 * Returns the unique expansions (at most `limit`), or null if the pattern is not supported.
 */
function expandWildcardTerm(term, limit) {
    const results = [];
    const seen = new Set();
    const add = text => {
        const cleaned = text.trim().replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, '');
        const key = searchTokens(cleaned).join(' ');     // what the search would compare
        if (key && !seen.has(key)) {
            seen.add(key);
            results.push(cleaned);
        }
    };
    const corpus = sentences.map(entry => entry && entry.current).filter(Boolean);
    // A match must start at a word boundary. (Written without lookbehind, which older Safari cannot parse:
    // the character before the match is captured and skipped instead.)
    const boundary = `(^|[^\\p{L}\\p{N}])`;

    if (term.includes('**')) {
        const leading = term.startsWith('**');
        const trailing = term.endsWith('**');
        const core = term.replace(/^\*\*/, '').replace(/\*\*$/, '').trim();
        if (!core || core.includes('*') || (!leading && !trailing)) {
            return null;
        }
        const escaped = escapeRegExp(core);
        const pattern = leading && trailing
            ? `()(${escaped})`
            : leading
                ? `()(${escaped})(?!${WORD_CHARACTER})`
                : `${boundary}(${escaped})`;
        const regex = new RegExp(pattern, 'iu');
        for (const text of corpus) {
            if (results.length >= limit) break;
            const match = regex.exec(text);
            if (!match) continue;
            const from = match.index + match[1].length;
            if (leading && trailing) {
                add(text);
            } else if (leading) {
                add(text.slice(0, from + match[2].length));
            } else {
                add(text.slice(from));
            }
        }
        return results;
    }

    // Single *: letters/digits (and inner apostrophes or hyphens) inside one word; alone it is a whole word
    const inWord = `(?:${WORD_CHARACTER}|['’-])*`;
    const pattern = term.split(/\s+/).filter(Boolean)
        .map(word => (word === '*' ? `${WORD_CHARACTER}${inWord}` : word.split('*').map(escapeRegExp).join(inWord)))
        .join(`[^\\p{L}\\p{N}]+`);
    const regex = new RegExp(`${boundary}(${pattern})(?!${WORD_CHARACTER})`, 'giu');
    for (const text of corpus) {
        if (results.length >= limit) break;
        for (const match of text.matchAll(regex)) {
            add(match[2]);
            if (results.length >= limit) break;
        }
    }
    return results;
}

/** Show a notice above/between the result cards (failed renders, a failed group) without ending the search. */
function showResultNotice(message) {
    if (!elements.resultsContainer) {
        return;
    }
    const notice = document.createElement('div');
    notice.className = 'result-notice';
    notice.setAttribute('role', 'status');
    notice.textContent = message;
    const loading = document.getElementById('searchLoadingIndicator');
    elements.resultsContainer.insertBefore(notice, loading);
}

function handleSentenceSearch() {
    if (!activeProject || !elements.sentenceInput) {
        return;
    }
    const rawSentence = elements.sentenceInput.value.trim();
    if (!rawSentence) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        rejectSearch('Add at least one video to the project (Media Library) before searching.');
        return;
    }

    // Groups are separated by semicolons; wildcards are expanded against the corpus
    const maxResultsPerSegment = getMaxResultsPerSegment();
    const sentenceGroups = [];
    const seenGroups = new Set();
    const notes = [];       // things to tell the user about single terms without giving up the other terms
    for (const term of rawSentence.split(';').map(part => part.trim()).filter(Boolean)) {
        let expansions = [term];
        if (term.includes('*')) {
            if (!sentences.length) {
                rejectSearch('Wildcards (*) are looked up in the corpus: click "Load corpus" in the Media Library first.');
                return;
            }
            expansions = expandWildcardTerm(term, WILDCARD_EXPANSION_LIMIT + 1);
            if (expansions === null) {
                rejectSearch(`"${term}" is not a supported wildcard pattern. Use ball*, ball**, **ball or **ball**.`);
                return;
            }
            if (!expansions.length) {
                notes.push(`Nothing in the corpus matches "${term}".`);
            } else if (expansions.length > WILDCARD_EXPANSION_LIMIT) {
                expansions.length = WILDCARD_EXPANSION_LIMIT;
                notes.push(`"${term}" matches more than ${WILDCARD_EXPANSION_LIMIT} phrases: only the first ${WILDCARD_EXPANSION_LIMIT} are searched.`);
            }
        }
        expansions.forEach(group => {
            const key = group.toLowerCase();
            if (!seenGroups.has(key)) {
                seenGroups.add(key);
                sentenceGroups.push(group);
            }
        });
    }
    if (!sentenceGroups.length) {
        if (notes.length) {
            rejectSearch(notes.join(' '));
        }
        return;
    }

    // The search is valid: only now replace what is on screen
    cancelActiveSearch();
    stopClipPlayback();
    hideFloatingPreview();
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }

    if (isMobileLayout()) {
        switchMobileSection('results');
    }

    const includePartialMatchesCheckbox = document.getElementById('includePartialMatches');
    const includePartialMatches = includePartialMatchesCheckbox ? includePartialMatchesCheckbox.checked : false;
    const allPartialMatchesCheckbox = document.getElementById('allPartialMatches');
    const allPartialMatches = allPartialMatchesCheckbox ? allPartialMatchesCheckbox.checked : false;
    const silencePreferences = getSilencePreferences();

    updateProjectData(data => {
        data.currentSentence = rawSentence;
        data.silencePreferences = silencePreferences;
        data.includePartialMatches = includePartialMatches;
        data.allPartialMatches = allPartialMatches;
        data.maxResultsPerSegment = maxResultsPerSegment;
    });

    sentenceSearchCounter += 1;
    window.currentSentenceGroups = sentenceGroups;
    const search = beginSearch();
    const displaySentence = sentenceGroups.length > 3
        ? `${sentenceGroups.slice(0, 3).join('; ')}; … (${sentenceGroups.length} phrases)`
        : sentenceGroups.join('; ');
    showSearchLoading(`Searching for "${displaySentence}"...`, cancelActiveSearch);
    notes.forEach(showResultNotice);

    let resultCount = 0;
    const problems = [...notes];
    const unmatched = [];
    const onEvent = data => {
        if (!search.isCurrent) {
            return false;
        }
        if (data.done) {
            if (data.cancelled) {
                problems.push('cancelled');     // ended from elsewhere: not "No matches found"
                showResultNotice('The search was cancelled.');
            }
            return false;
        }
        if (data.warning) {
            // e.g. project videos that are no longer in the library
            problems.push(data.warning);
            showResultNotice(data.warning);
            return true;
        }
        if (data.error) {
            // One group failed; the server continues with the next one.
            console.error('Search error:', data.error);
            problems.push(data.error);
            showResultNotice(data.error);
            return true;
        }
        if (data.progress) {
            if (data.search_id) {
                search.serverId = data.search_id;
                window.currentSearchId = data.search_id;
            }
            updateSearchLoadingMessage(data.progress, data);
            return true;
        }
        if (data.no_match) {
            unmatched.push(data.phrase);
            return true;
        }
        if (data.phrase) {
            if (data.skipped) {
                updateSearchLoadingMessage(`⏭️ Skipped: '${data.phrase}' (no clips rendered)`);
                return true;
            }
            const files = Array.isArray(data.files) ? data.files : [];
            if (data.failed) {
                const notice = `${data.failed} of ${data.failed + files.length} clips for "${data.phrase}" could not be rendered: ${data.first_error || 'unknown error'}`;
                problems.push(notice);
                showResultNotice(notice);
            }
            if (files.length) {
                resultCount += 1;
                addSegmentResult(data);
            }
        }
        return true;
    };

    fetch('/search_longest_segments_stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            files: selectedFiles,
            sentence_groups: sentenceGroups,
            sentenceSearchCounter,
            minSilence: silencePreferences.minSilence,
            maxSilence: silencePreferences.maxSilence,
            silenceWordThreshold: silencePreferences.silenceWordThreshold,
            includePartialMatches,
            allPartialMatches,
            maxResultsPerSegment
        }),
        signal: search.controller.signal
    })
        .then(async response => {
            if (!response.ok) {
                const failure = await response.json().catch(() => ({}));
                throw new Error(failure.error || `the server answered with HTTP ${response.status}`);
            }
            await readEventStream(response, onEvent);
            if (!search.isCurrent) {
                return;
            }
            finishSearch(search);
            if (!resultCount && !problems.length) {
                showNoResults(`No matches found for "${displaySentence}".`);
            } else if (unmatched.length) {
                // Some phrases of a multi-phrase search found nothing: say which, the cards do not show it.
                showResultNotice(`No matches for ${unmatched.map(phrase => `"${phrase}"`).join(', ')}.`);
            }
        })
        .catch(error => {
            if (!search.isCurrent) {
                return;     // cancelled, or replaced by a newer search
            }
            finishSearch(search);
            console.error('[Client] ❌ Error searching sentence:', error);
            showNoResults(`The search failed: ${error.message}`);
        });
}

// Search for pauses between words whose length lies in the range of the "Search silence" slider
function handleSearchSilence() {
    if (!activeProject || !elements.silenceRangeMinInput || !elements.silenceRangeMaxInput) {
        return;
    }

    const selectedFiles = Array.isArray(activeProject.data?.selectedFiles) ? activeProject.data.selectedFiles : [];
    if (!selectedFiles.length) {
        rejectSearch('Add at least one video to the project (Media Library) before searching.');
        return;
    }
    cancelActiveSearch();
    stopClipPlayback();
    hideFloatingPreview();
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }

    let minMs = Math.max(0, Math.min(10000, parseInt(elements.silenceRangeMinInput.value, 10) || 0));
    let maxMs = Math.max(0, Math.min(10000, parseInt(elements.silenceRangeMaxInput.value, 10) || 0));
    if (minMs > maxMs) {
        maxMs = minMs;
        elements.silenceRangeMaxInput.value = String(maxMs);
    }
    const minSilence = minMs / 1000;
    const maxSilence = maxMs / 1000;
    const range = `between ${minSilence.toFixed(1)}s and ${maxSilence.toFixed(1)}s`;

    if (isMobileLayout()) {
        switchMobileSection('results');
    }
    const search = beginSearch();
    showSearchLoading(`Searching silences ${range}...`, cancelActiveSearch);

    let clipCount = 0;
    let failure = null;
    let warned = false;
    const onEvent = data => {
        if (!search.isCurrent || data.done) {
            return false;
        }
        if (data.warning) {
            warned = true;
            showResultNotice(data.warning);
            return true;
        }
        if (data.error) {
            failure = data.error;
            return false;
        }
        if (data.phrase && Array.isArray(data.files)) {
            clipCount += data.files.length;
            // Buckets ("1-2s silences") grow while the search is streaming
            const existing = Array.from(elements.resultsContainer.querySelectorAll('.phrase-container'))
                .map(container => resultCards.get(container))
                .find(card => card && card.phrase === data.phrase);
            if (existing) {
                existing.addFiles(data.files, data.total_matches);
            } else {
                createResultCard(data);
            }
        }
        return true;
    };

    fetch('/search_silences_stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            files: selectedFiles,
            minSilence,
            maxSilence,
            maxResultsPerSegment: getMaxResultsPerSegment()
        }),
        signal: search.controller.signal
    })
        .then(async response => {
            if (!response.ok) {
                const body = await response.json().catch(() => ({}));
                throw new Error(body.error || `the server answered with HTTP ${response.status}`);
            }
            await readEventStream(response, onEvent);
            if (!search.isCurrent) {
                return;
            }
            finishSearch(search);
            if (failure) {
                showNoResults(`Error: ${failure}`);
            } else if (!clipCount && !warned) {
                showNoResults(`No silences found ${range}.`);
            }
        })
        .catch(error => {
            if (!search.isCurrent) {
                return;
            }
            finishSearch(search);
            console.error('[Client] Error searching silences:', error);
            showNoResults(`The silence search failed: ${error.message}`);
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
    cancelActiveSearch();
    stopClipPlayback();
    hideFloatingPreview();
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

    // The LLM can take minutes: the request can be cancelled, and a project switch discards its answer
    const search = beginSearch();
    // Named by the client so Cancel can reach the request (it has no progress events to learn an id from)
    search.serverId = generateId('story');
    showSearchLoading(`Generating story with AI...`, cancelActiveSearch);

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
            debugMode: debugMode,
            search_id: search.serverId
        }),
        signal: search.controller.signal
    })
        .then(async response => ({ ok: response.ok, status: response.status, data: await response.json().catch(() => ({})) }))
        .then(({ ok, status, data }) => {
        if (!search.isCurrent) {
            return;
        }
        finishSearch(search);

        // The server explains what went wrong (LLM unreachable, timeout, model missing, invalid answer).
        if (!ok || data.error) {
            console.error('[Client] ❌ Error generating story:', data.error || status);
            showNoResults(`Error: ${data.error || `the server answered with HTTP ${status}`}`);
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
        
        displayStorySegments(data.segments, data.story_explanation);
        if (data.debug) {
            displayDebugInfo(data.debug);       // after the cards: displayStorySegments empties the list first
        }
        if (Array.isArray(data.unmatched) && data.unmatched.length) {
            showResultNotice(`${data.unmatched.length} segment(s) of the story were not found in the selected videos: ${data.unmatched.map(item => `"${item}"`).join(', ')}`);
        }
        })
        .catch(error => {
        if (!search.isCurrent) {
            return;     // cancelled, or the project was switched
        }
        finishSearch(search);
        console.error('[Client] ❌ Error generating story:', error);
        showNoResults(`The story could not be generated: ${error.message}`);
    });
}

// Store prompt when generating (before API call)

function handleCopyLLMPrompt() {
    // Always build the prompt from the current story prompt, files and options. (A prompt cached from an
    // earlier run was copied even after any of them had changed.)
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
    const preferLongSegmentsCheckbox = document.getElementById('preferLongSegments');
    const preferLongSegments = preferLongSegmentsCheckbox ? preferLongSegmentsCheckbox.checked : true;
    
    // Feedback on the button itself: the results list has nothing to do with copying a prompt
    const button = elements.copyLLMPromptButton;
    const label = button ? button.textContent : '';
    const setButton = (text, disabled) => {
        if (button) {
            button.textContent = text;
            button.disabled = disabled;
        }
    };
    setButton('Building prompt…', true);

    fetch('/generate_story_prompt', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            prompt: prompt,
            files: selectedFiles,
            maxSegments: maxStorySegments,
            preferLongSegments: preferLongSegments
        })
    })
    .then(response => response.json().catch(() => ({ error: `the server answered with HTTP ${response.status}` })))
    .then(data => {
        if (data.error || !data.prompt) {
            throw new Error(data.error || 'the server returned no prompt');
        }
        return copyText(data.prompt).then(copied => {
            if (copied) {
                setButton('Copied!', true);
                setTimeout(() => setButton(label, false), 1500);
                return;
            }
            // The browser refused: show the prompt so it can be copied by hand
            setButton(label, false);
            const box = document.createElement('textarea');
            box.className = 'result-notice';
            box.readOnly = true;
            box.rows = 8;
            box.style.width = '100%';
            box.value = data.prompt;
            elements.resultsContainer.prepend(box);
            revealResultsSection();
            box.select();
        });
    })
    .catch(error => {
        setButton(label, false);
        console.error('[Client] ❌ Error generating prompt:', error);
        alert(`The prompt could not be generated: ${error.message}`);
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

async function handleProcessInsertedResponse() {
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
    
    // The server reads the answer, with the same tolerance as for a story it requested itself (text around
    // the JSON, several objects, an answer that was cut off).
    let explanation = '';
    let sentenceQueries = [];
    try {
        const response = await fetch('/parse_story_response', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ response: responseText })
        });
        const parsed = await response.json().catch(() => ({}));
        if (!response.ok) {
            throw new Error(parsed.error || `the server answered with HTTP ${response.status}`);
        }
        explanation = parsed.explanation || '';
        sentenceQueries = (parsed.sentences || []).map(item => [item.sentence, item.queries]);
    } catch (error) {
        alert(`The response could not be read: ${error.message}`);
        return;
    }
    if (!sentenceQueries.length) {
        alert('No sentences found in the response. Expected a "sentences" array.');
        return;
    }

    // Close the input box but keep the pasted text until the user discards it
    if (elements.insertResponseContainer) {
        elements.insertResponseContainer.style.display = 'none';
    }
    cancelActiveSearch();
    stopClipPlayback();
    hideFloatingPreview();
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }

    // Now search for each source_segment and add to results
    // Each source_segment will be searched separately and create a separate result card
    processStorySentences(sentenceQueries, selectedFiles, explanation);
}

/**
 * Story from a pasted LLM answer: look up every source segment (exact match, first hit) and show the result
 * like a generated story. Segments that are not in the selected videos are listed instead of being dropped.
 */
async function processStorySentences(sentenceQueries, selectedFiles, explanation) {
    const allSegments = [];
    const unmatched = [];
    let requestCounter = 0;
    const search = beginSearch();
    showSearchLoading(`Processing ${sentenceQueries.length} sentences...`, cancelActiveSearch);

    for (const [sentenceText, searchQueries] of sentenceQueries) {
        for (const searchQuery of searchQueries) {
            if (!search.isCurrent) {
                return;     // cancelled, or the project was switched
            }
            requestCounter += 1;
            let segment = null;
            try {
                const response = await fetch('/search_longest_segments_stream', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        // One group, searched as a whole (commas included)
                        sentence_groups: [searchQuery],
                        files: selectedFiles,
                        sentenceSearchCounter: requestCounter,
                        includePartialMatches: false,
                        maxResultsPerSegment: 1
                    }),
                    signal: search.controller.signal
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                await readEventStream(response, data => {
                    if (Array.isArray(data.files) && data.files.length > 0) {
                        segment = { phrase: searchQuery, files: data.files, created_sentence: sentenceText };
                        return false;
                    }
                    return !data.done;
                });
            } catch (error) {
                console.error(`Story segment "${searchQuery}" could not be searched:`, error);
            }
            if (segment) {
                allSegments.push(segment);
            } else {
                unmatched.push(searchQuery);
            }
        }
    }

    if (!search.isCurrent) {
        return;
    }
    finishSearch(search);
    if (allSegments.length > 0) {
        displayStorySegments(allSegments, explanation);
    } else {
        showNoResults('No matches found for the inserted response.');
    }
    if (unmatched.length) {
        showResultNotice(`${unmatched.length} segment(s) of the story were not found in the selected videos: ${unmatched.map(text => `"${text}"`).join(', ')}`);
    }
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
        debugHTML += `<strong>Corpus Entries:</strong> ${escapeHtml(debugData.corpus_entry_count)}<br><br>`;
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
        debugHTML += `<strong>Selected Indices (legacy format):</strong> [${escapeHtml([].concat(debugData.selected_indices).join(', '))}]<br>`;
    }
    
    debugDiv.innerHTML = debugHTML;
    
    // Insert at the beginning of results container
    if (elements.resultsContainer.firstChild) {
        elements.resultsContainer.insertBefore(debugDiv, elements.resultsContainer.firstChild);
    } else {
        elements.resultsContainer.appendChild(debugDiv);
    }
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
        // The explanation is LLM output (or pasted text): never insert it as HTML.
        const label = document.createElement('strong');
        label.textContent = 'AI Story:';
        explanationDiv.append(label, ` ${storyExplanation}`);
        elements.resultsContainer.appendChild(explanationDiv);
    }

    // Display each segment
    segments.forEach((segmentData, index) => {
        addSegmentResult(segmentData, index + 1);
    });

    // A story is meant to be watched in order: put its segments on the timeline right away
    addAllMatchesToTimeline();
}

function handleStartOver() {
    sentenceSearchCounter = 0;
    globalPhraseContainerIDCounter = 0;
    cancelActiveSearch();
    stopClipPlayback();
    hideFloatingPreview();
    if (mainStage) {
        mainStage.hide();
    }
    if (elements.resultsContainer) {
        elements.resultsContainer.innerHTML = '';
    }
    if (elements.videoPlayer) {
        elements.videoPlayer.pause();
        elements.videoPlayer.removeAttribute('src');
        elements.videoPlayer.load();
    }
    updateVideoPlayerTitle('Preview');
}

// ---------------------------------------------------------------------------------------------------------
// Playback
//
// Clips (search results, timeline items, "Play timeline") are played by ClipPlayer on the Web Audio clock:
// sound starts and stops on the exact sample the merge will cut at, pictures follow. The plain <video> element
// is only used for finished merges.
// ---------------------------------------------------------------------------------------------------------

async function restoreClipFile(item) {
    try {
        const response = await fetch('/ensure_clip', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(makeClipRef(item))
        });
        return response.ok;
    } catch (error) {
        return false;
    }
}

function getClipPlayer() {
    if (!clipPlayer) {
        clipPlayer = ClipPlayer.create({ restoreClip: restoreClipFile });
    }
    return clipPlayer;
}

/** The clip stage of the main viewer. While it is shown the <video> element (merged videos) is hidden. */
function getMainStage() {
    if (!mainStage && elements.videoPlayer) {
        const video = elements.videoPlayer;
        const host = document.createElement('div');
        host.className = 'clip-stage-host';
        video.insertAdjacentElement('beforebegin', host);
        mainStage = ClipPlayer.createStage(host, {
            onShow() {
                video.pause();
                video.hidden = true;
            },
            onHide() {
                video.hidden = false;
            }
        });
    }
    return mainStage;
}

function stopClipPlayback() {
    if (clipPlayer) {
        clipPlayer.stop();
    }
}

/** Play a finished merge in the native player (with its own controls and sound). */
function playMergedVideo(filePath, shouldLoop = false) {
    const video = elements.videoPlayer;
    if (!video || !filePath) {
        return;
    }
    stopClipPlayback();
    if (mainStage) {
        mainStage.hide();
    }
    video.loop = Boolean(shouldLoop);
    video.playbackRate = 1;
    video.src = getVideoUrl(filePath);
    updateVideoPlayerTitle('Merged video');
    video.play().catch(error => console.warn('Could not start the merged video automatically:', error));
}

function updateVideoPlayerTitle(title) {
    const titleElement = document.querySelector('.video-card h2');
    if (titleElement) {
        titleElement.textContent = title || 'Preview';
    }
}

/** Preview the selected match of a result card in the floating preview. */
function playVideo(phraseContainer) {
    const card = resultCards.get(phraseContainer);
    const match = card ? card.selectedMatch() : null;
    if (!match) {
        return Promise.resolve();
    }
    updateFloatingPreview(phraseContainer);
    const waveform = card.waveform;
    return getClipPlayer().play(getFloatingStage(), [card.toItem(match)], {
        onProgress: (index, fraction) => waveform.setPlayhead(fraction),
        onEnd: () => waveform.setPlayhead(null),
        replay: () => playVideo(phraseContainer)
    });
}

/** "Play all": the selected match of every result card, back to back, in the main viewer. */
function playAllVideos() {
    const stage = getMainStage();
    const cards = Array.from(document.querySelectorAll('.phrase-container'))
        .map(container => resultCards.get(container))
        .filter(card => card && card.selectedMatch());
    if (!stage || !cards.length) {
        return;
    }
    hideFloatingPreview();
    const highlight = active => cards.forEach(card => card.container.classList.toggle('is-playing', card === active));
    getClipPlayer().play(stage, cards.map(card => card.toItem()), {
        onItemStart: index => {
            highlight(cards[index]);
            updateVideoPlayerTitle(cards[index].phrase);
        },
        onEnd: () => {
            highlight(null);
            updateVideoPlayerTitle('Preview');
        },
        replay: playAllVideos
    });
    if (isMobileLayout()) {
        switchMobileSection('viewer');
    }
}

// ---------------------------------------------------------------------------------------------------------
// Result cards
// ---------------------------------------------------------------------------------------------------------

function isSilenceFile(file) {
    return typeof file.silence_start === 'number' && typeof file.silence_end === 'number';
}

/** Normalise one entry of a search result's `files` list (object, or a bare clip path from old responses). */
function describeMatch(file, phrase) {
    const info = (typeof file === 'object' && file !== null) ? file : { file };
    const silence = isSilenceFile(info);
    const ref = makeClipRef({
        clip: info.file,
        source: info.source_video,
        originalStart: info.original_start,
        originalEnd: info.original_end
    });
    // The cut bounds give the exact clip length; the container duration reported by the server is rounded up
    // to whole video frames and only used when the bounds are unknown.
    let durationMs = ref.originalStart !== null && ref.originalEnd !== null
        ? (ref.originalEnd - ref.originalStart) * 1000
        : Number.parseFloat(info.duration_ms);
    durationMs = Number.isFinite(durationMs) && durationMs > 0 ? durationMs : null;
    // The server reports how much padding surrounds the match: less than the default when the match sits at
    // the very start or end of the video, where the default trim would cut into the words.
    const padding = value => (silence ? 0 : (Number.isFinite(value) && value >= 0 ? value : DEFAULT_TRIM_MS));
    const trims = clampTrims(padding(info.lead_ms), padding(info.tail_ms), durationMs || 0);
    return {
        ...ref,
        startTrim: trims.start,
        endTrim: trims.end,
        durationMs,
        text: info.text || phrase || '',
        isSilence: silence,
        silenceStart: silence ? info.silence_start : null,
        silenceEnd: silence ? info.silence_end : null,
        wordBefore: info.word_before || '',
        wordAfter: info.word_after || '',
        label: ''
    };
}

function matchLabel(match, index) {
    const videoName = match.source ? match.source.split('/').pop() : '';
    const length = formatTrimmedLength(match.durationMs, match.startTrim, match.endTrim);
    if (match.isSilence) {
        const seconds = Math.max(0, match.silenceEnd - match.silenceStart);
        const context = match.wordBefore || match.wordAfter
            ? ` between "${match.wordBefore || '…'}" and "${match.wordAfter || '…'}"`
            : '';
        return `Silence ${seconds.toFixed(2)}s (${length}${videoName})${context}`;
    }
    const text = match.text || `Match ${index + 1}`;
    return videoName || length ? `${text} (${length}${videoName})` : text;
}

/**
 * Build the card of one result segment: its matches in a listbox, the waveform with trim handles of the
 * selected match and the play / add buttons. Returns the card controller (also stored in `resultCards`).
 */
function createResultCard(segmentData) {
    if (!segmentData || !segmentData.phrase || !Array.isArray(segmentData.files)) {
        console.error('Invalid segment data:', segmentData);
        return null;
    }
    if (!elements.resultsContainer) {
        return null;
    }

    const container = document.createElement('div');
    container.id = `phraseContainer${globalPhraseContainerIDCounter++}`;
    container.classList.add('phrase-container');

    const title = document.createElement('h4');
    title.textContent = segmentData.phrase;
    title.classList.add('phrase-title');
    title.setAttribute('draggable', 'true');
    title.title = 'Drag onto the timeline';
    container.appendChild(title);
    registerSearchContainerDrag(title, container);

    const listbox = document.createElement('select');
    listbox.id = `phrase${container.id}`;
    listbox.size = 5;
    listbox.classList.add('phrase-video-listbox');
    listbox.setAttribute('aria-label', `Matches for "${segmentData.phrase}". Space replays, Enter adds to the timeline.`);
    container.appendChild(listbox);
    // "25 of 312": the list is cut at "Max results per segment", which the card would otherwise hide
    const moreNote = document.createElement('div');
    moreNote.className = 'phrase-more-note';
    moreNote.hidden = true;
    container.appendChild(moreNote);

    const waveform = createWaveformVisualization({
        onTrimChange: (start, end) => {
            const match = card.selectedMatch();
            if (match) {
                match.startTrim = start;
                match.endTrim = end;
            }
        },
        onTrimRelease: () => playVideo(container)
    });
    container.appendChild(waveform);

    const card = {
        container,
        listbox,
        waveform,
        phrase: segmentData.phrase,
        matches: [],
        shownIndex: -1,
        selectedMatch() {
            return card.matches[listbox.selectedIndex >= 0 ? listbox.selectedIndex : 0] || null;
        },
        toItem(match = card.selectedMatch()) {
            return {
                clip: match.clip,
                source: match.source,
                originalStart: match.originalStart,
                originalEnd: match.originalEnd,
                startTrim: match.startTrim,
                endTrim: match.endTrim,
                speed: 1,
                title: card.phrase
            };
        },
        /** Append matches that are not listed yet (silence buckets grow while the search is streaming). */
        addFiles(files, totalMatches) {
            if (Number.isFinite(totalMatches)) {
                card.totalMatches = Math.max(card.totalMatches || 0, totalMatches);
            }
            (files || []).forEach(file => {
                const match = describeMatch(file, card.phrase);
                if (!match.clip || card.matches.some(existing => existing.clip === match.clip)) {
                    return;
                }
                match.label = matchLabel(match, card.matches.length);
                card.matches.push(match);
                const option = document.createElement('option');
                option.value = match.clip;
                option.text = match.label;
                option.title = match.source || match.clip;
                listbox.appendChild(option);
            });
            if (listbox.selectedIndex < 0 && listbox.options.length > 0) {
                listbox.selectedIndex = 0;
            }
            moreNote.hidden = !(card.totalMatches > card.matches.length);
            if (!moreNote.hidden) {
                moreNote.textContent = `${card.matches.length} of ${card.totalMatches} matches shown (raise "Max results per segment" for more)`;
            }
            if (card.shownIndex < 0 && card.matches.length > 0) {
                card.showSelected();
            }
        },
        /** Point the waveform at the selected match (with the trims remembered for it). */
        showSelected() {
            const match = card.selectedMatch();
            if (!match) {
                return;
            }
            card.shownIndex = listbox.selectedIndex;
            waveform.setClip(makeClipRef(match), { start: match.startTrim, end: match.endTrim });
        }
    };
    resultCards.set(container, card);

    let lastChangeAt = 0;
    listbox.addEventListener('change', () => {
        lastChangeAt = performance.now();
        card.showSelected();
        playVideo(container);
    });
    // 'change' does not fire when the already selected match is clicked again: treat that as "replay".
    listbox.addEventListener('click', () => {
        if (performance.now() - lastChangeAt > 100) {
            playVideo(container);
        }
    });
    listbox.addEventListener('keydown', event => {
        if (event.key === ' ') {
            event.preventDefault();
            playVideo(container);
        } else if (event.key === 'Enter') {
            event.preventDefault();
            handleAddContainerToTimeline(container);
        }
    });

    const controls = document.createElement('div');
    controls.classList.add('phrase-controls');
    const addControl = (label, className, handler) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.classList.add(className);
        button.addEventListener('click', handler);
        controls.appendChild(button);
        return button;
    };
    addControl('Play', 'play-button', () => playVideo(container));
    const addButton = addControl('Add to Timeline', 'add-button', () => {
        if (handleAddContainerToTimeline(container)) {
            flashButton(addButton, 'Added ✓');
        }
    });
    addControl('Add All Variants', 'add-all-button', () => handleAddAllVariantsToTimeline(container));
    container.appendChild(controls);

    elements.resultsContainer.appendChild(container);
    card.addFiles(segmentData.files, segmentData.total_matches);
    initializeFloatingPreview();
    return card;
}

function addSegmentResult(segmentData) {
    createResultCard(segmentData);
}

// ---------------------------------------------------------------------------------------------------------
// Floating preview (next to the result card that is being auditioned)
// ---------------------------------------------------------------------------------------------------------

function initializeFloatingPreview() {
    if (floatingPreviewContainer) {
        return;
    }
    floatingPreviewContainer = document.createElement('div');
    floatingPreviewContainer.id = 'floatingPreview';
    floatingPreviewContainer.classList.add('floating-preview');
    floatingPreviewContainer.style.display = 'none';

    const previewContent = document.createElement('div');
    previewContent.classList.add('floating-preview-content');
    const previewTitle = document.createElement('h4');
    previewTitle.classList.add('floating-preview-title');
    const stageHost = document.createElement('div');
    stageHost.classList.add('clip-stage-host');
    previewContent.appendChild(previewTitle);
    previewContent.appendChild(stageHost);
    floatingPreviewContainer.appendChild(previewContent);
    document.body.appendChild(floatingPreviewContainer);

    floatingStage = ClipPlayer.createStage(stageHost);
    floatingStage.show();
}

function getFloatingStage() {
    initializeFloatingPreview();
    return floatingStage;
}

function isFloatingPreviewVisible() {
    return Boolean(floatingPreviewContainer) && floatingPreviewContainer.style.display !== 'none';
}

function hideFloatingPreview() {
    if (!floatingPreviewContainer) {
        return;
    }
    if (clipPlayer && floatingStage && clipPlayer.isPlaying(floatingStage)) {
        clipPlayer.stop();
    }
    floatingPreviewContainer.style.display = 'none';
    currentSelectedCard = null;
}

/** Show the floating preview next to `phraseContainer` and title it after the selected match. */
function updateFloatingPreview(phraseContainer) {
    initializeFloatingPreview();
    const card = resultCards.get(phraseContainer);
    const match = card ? card.selectedMatch() : null;
    if (!match) {
        hideFloatingPreview();
        return;
    }

    const inline = isMobileLayout();
    floatingPreviewContainer.classList.toggle('is-inline', inline);
    if (inline) {
        // Narrow screens: the preview sits inside its card. As an overlay it covered the card being auditioned
        // (list, waveform, buttons) and could only be dismissed by scrolling.
        floatingPreviewContainer.style.cssText = '';
        if (floatingPreviewContainer.parentElement !== phraseContainer) {
            phraseContainer.insertBefore(floatingPreviewContainer, card.listbox);
        }
    } else {
        if (floatingPreviewContainer.parentElement !== document.body) {
            document.body.appendChild(floatingPreviewContainer);
        }
        const cardRect = phraseContainer.getBoundingClientRect();
        const previewWidth = 400;
        const previewHeight = floatingPreviewContainer.offsetHeight || 320;
        const gap = 20;
        let left = cardRect.right + gap;
        if (left + previewWidth > window.innerWidth) {
            left = cardRect.left - previewWidth - gap;
        }
        left = Math.max(gap, left);
        let top = Math.min(cardRect.top, window.innerHeight - previewHeight - gap);
        top = Math.max(gap, top);
        floatingPreviewContainer.style.left = `${left}px`;
        floatingPreviewContainer.style.top = `${top}px`;
        floatingPreviewContainer.style.right = 'auto';
        floatingPreviewContainer.style.width = `${previewWidth}px`;
    }
    floatingPreviewContainer.style.display = 'block';
    floatingPreviewContainer.style.pointerEvents = 'auto';

    const previewTitle = floatingPreviewContainer.querySelector('.floating-preview-title');
    previewTitle.textContent = '';
    const matchNumber = document.createElement('span');
    matchNumber.textContent = `Match ${card.listbox.selectedIndex + 1}:`;
    if (match.text) {
        matchNumber.className = 'match-number-tooltip';
        matchNumber.title = `Matched: "${match.text}"`;
    }
    previewTitle.append(matchNumber, ` ${card.phrase}`);

    currentSelectedCard = phraseContainer;
}

// Hide the floating preview when clicking outside of it and outside any result card
document.addEventListener('click', event => {
    if (!isFloatingPreviewVisible()) {
        return;
    }
    const target = event.target;
    if (!floatingPreviewContainer.contains(target) && !(target.closest && target.closest('.phrase-container'))) {
        hideFloatingPreview();
    }
});

// The preview is positioned next to its card: once the page scrolls it would point at the wrong place
let scrollTimeout = null;
window.addEventListener('scroll', () => {
    if (!isFloatingPreviewVisible() || floatingPreviewContainer.classList.contains('is-inline')) {
        return;     // inside its card it scrolls along
    }
    clearTimeout(scrollTimeout);
    scrollTimeout = setTimeout(hideFloatingPreview, 150);
}, { passive: true });

// Move floating preview away when mouse hovers over cards behind it
document.addEventListener('mousemove', (event) => {
    if (!floatingPreviewContainer || floatingPreviewContainer.style.display === 'none' || !elements.resultsContainer
            || floatingPreviewContainer.classList.contains('is-inline')) {
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

async function handleLoadSentences() {
    try {
        const total = await loadSentences();
        if (total === null) {
            return;
        }
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

/** A word as the search compares it: lower case, typographic apostrophes/dashes folded, punctuation stripped. */
function normalizeSearchToken(word) {
    return String(word || '').normalize('NFKC').toLowerCase()
        .replace(/[\u2019\u2018\u02bc\u00b4`]/g, "'")
        .replace(/[\u2010-\u2014]/g, '-')
        .replace(/\u00ad/g, '')
        .replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, '');
}

function searchTokens(text) {
    return String(text || '').split(/\s+/).map(normalizeSearchToken).filter(Boolean);
}

/**
 * Whether the words of `text` are spoken one after the other somewhere in the corpus (the last one may be
 * unfinished). Punctuation and transcript segment boundaries do not matter, exactly as in the search itself.
 */
function isSpokenContiguously(text) {
    const tokens = searchTokens(text);
    if (!tokens.length) {
        return false;
    }
    const needle = ` ${tokens.join(' ')}`;
    return Object.values(combinedTranscriptions).some(entry => entry.tokenText.includes(needle));
}

/** The word that follows `text` where it is spoken in the corpus (as written in the transcript), or null. */
function nextSpokenWord(text) {
    const tokens = searchTokens(text);
    if (!tokens.length) {
        return null;
    }
    for (const entry of Object.values(combinedTranscriptions)) {
        let at = entry.tokens.indexOf(tokens[0]);
        while (at !== -1 && at + tokens.length < entry.tokens.length) {
            if (tokens.every((token, offset) => entry.tokens[at + offset] === token)) {
                return entry.words[at + tokens.length];
            }
            at = entry.tokens.indexOf(tokens[0], at + 1);
        }
    }
    return null;
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
    sentences.forEach(entry => {
        if (entry) {
            entry.tokens = searchTokens(entry.current);     // once, not on every keystroke
        }
    });
    
    // Combine segments for each file into one long transcription
    for (const [file, segments] of Object.entries(fileSegments)) {
        // Join all segments with spaces to create one long transcription
        const combined = segments.join(' ');
        const words = combined.split(/\s+/).filter(word => normalizeSearchToken(word));
        const tokens = words.map(normalizeSearchToken);
        combinedTranscriptions[file] = {
            text: combined,
            normalized: combined.toLowerCase(),
            segments: segments, // Keep original segments for reference
            words,                                  // words that count for the search, as written
            tokens,                                 // the same words as the search compares them
            tokenText: ` ${tokens.join(' ')} `
        };
    }
}

let corpusRequestToken = 0;

/** Load the corpus of the project's files. Resolves to null when a newer request has replaced this one. */
async function loadSentences() {
    const token = ++corpusRequestToken;
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
    if (token !== corpusRequestToken) {
        return null;    // the project or its files changed while this was loading
    }
    sentences = Array.isArray(data.sentences)
        ? data.sentences.map(normalizeCorpusEntry).filter(Boolean)
        : [];
    buildCombinedTranscriptions(); // Build combined transcriptions after loading
    return sentences.length;
}

// Sentence autocomplete functions

/** Set the sentence field from code (completion, auto-split, undo) and store it like typed text. */
function setSentenceInput(value) {
    elements.sentenceInput.value = value;
    updateProjectData(data => {
        data.currentSentence = value.trim();
    });
}

/** Refresh the suggestions shortly after the last keystroke (the corpus can be large). */
function scheduleSentenceAutocomplete() {
    cancelScheduledSentenceAutocomplete();
    sentenceAutocompleteState.timer = setTimeout(() => {
        sentenceAutocompleteState.timer = null;
        updateSentenceAutocomplete();
    }, 120);
}

function cancelScheduledSentenceAutocomplete() {
    clearTimeout(sentenceAutocompleteState.timer);
    sentenceAutocompleteState.timer = null;
}
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
    
    // Filter and prioritize autocompletion suggestions
    // Use simple approach like updateAutocomplete - show all matches that contain input
    const MAX_SUGGESTIONS = 100;
    const suggestions = [];
    
    // Whether the typed words occur in a segment one after the other, compared like the search compares
    // them (punctuation ignored). The last word may be unfinished unless a space was typed after it;
    // a word never matches inside another one ("euch" does not match "beleuchten").
    const inputTokens = searchTokens(normalizedInput);
    function matchesAtWordBoundary(textTokens) {
        if (!inputTokens.length) {
            return false;
        }
        const last = inputTokens.length - 1;
        for (let start = 0; start <= textTokens.length - inputTokens.length; start++) {
            let matches = true;
            for (let offset = 0; offset <= last && matches; offset++) {
                const textToken = textTokens[start + offset];
                matches = offset < last || hasTrailingSpace
                    ? textToken === inputTokens[offset]
                    : textToken.startsWith(inputTokens[offset]);
            }
            if (matches) {
                return true;
            }
        }
        return false;
    }

    // Wildcards: * stands for the rest of a word, ** for any text
    const hasWildcards = normalizedInput.includes('*');
    const wildcardPattern = hasWildcards
        ? new RegExp(normalizedInput.split('**').map(part => part.split('*').map(escapeRegExp).join('[^\\s]*')).join('.*'), 'i')
        : null;

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
        else if (wildcardPattern ? wildcardPattern.test(segmentNormalized) : matchesAtWordBoundary(entry.tokens || [])) {
            allMatches.push(entry);
        }
    });
    
    // Sort all matches: those starting with input first, then others
    // Within each group, sort by length (longest first)
    allMatches.sort((a, b) => {
        const aText = a.currentNormalized || a.current?.toLowerCase() || '';
        const bText = b.currentNormalized || b.current?.toLowerCase() || '';
        
        if (hasWildcards) {
            // For wildcard searches, prioritize exact matches, then by length
            const aExact = aText === normalizedInput;
            const bExact = bText === normalizedInput;
            
            if (aExact && !bExact) return -1;
            if (!aExact && bExact) return 1;
            
            // Within same group, sort by length (longest first)
            return bText.length - aText.length;
        } else {
            // Normal sorting for non-wildcard searches
            const aStarts = aText.startsWith(normalizedInput);
            const bStarts = bText.startsWith(normalizedInput);
            
            // Prioritize matches that start with input
            if (aStarts && !bStarts) return -1;
            if (!aStarts && bStarts) return 1;
            
            // Within same group, sort by length (longest first)
            return bText.length - aText.length;
        }
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
    // Skip if we're in a multi-sentence completion context (recent tab completion with sentence-ending text)
    const currentInputValue = elements.sentenceInput.value.trim();
    const currentSemicolonIndex = currentInputValue.lastIndexOf(';');
    const activeGroupCheck = currentSemicolonIndex === -1 ? currentInputValue : currentInputValue.slice(currentSemicolonIndex + 1).trim();
    
    // A group restored with Backspace stays whole while the user keeps extending it
    if (sentenceAutocompleteState.noSplitPrefix && !normalizedInput.startsWith(sentenceAutocompleteState.noSplitPrefix)) {
        sentenceAutocompleteState.noSplitPrefix = '';
    }
    const shouldSkipAutoSemicolon = sentenceAutocompleteState.skipAutoSemicolon ||
                                   Boolean(sentenceAutocompleteState.noSplitPrefix) ||
                                   sentenceAutocompleteState.pasted ||
                                   hasWildcards ||
                                   (activeGroupCheck.match(/[.!?]\s+[A-ZÄÖÜ]/));

    sentenceAutocompleteState.skipAutoSemicolon = false; // Reset flag
    
    // Special handling for multi-sentence contexts: always check for next sentence suggestions
    if (activeGroupCheck.match(/[.!?]\s+[A-ZÄÖÜ]/)) {
        // Extract the last sentence and look for what comes after it
        // Find the LAST sentence boundary, not the first one
        const splitSentences = activeGroupCheck.split(/[.!?]\s+/);
        const sentenceEndings = [...activeGroupCheck.matchAll(/[.!?]\s+/g)];
        
        if (splitSentences.length > 1 && sentenceEndings.length > 0) {
            // Get the last complete sentence and what comes after
            const lastSentenceIndex = splitSentences.length - 2; // Second to last (last sentence)
            const lastSentence = splitSentences[lastSentenceIndex].trim();
            const lastSentenceEnd = sentenceEndings[sentenceEndings.length - 1][0]; // The punctuation + space
            const nextPart = splitSentences[splitSentences.length - 1].trim(); // What's after the last sentence
            const nextWords = nextPart.split(/\s+/);
            
            // Reconstruct the full last sentence with punctuation
            const fullLastSentence = lastSentence + lastSentenceEnd.trim();
            
            if (nextWords.length > 0) {
                // Look for suggestions that start with the next words
                const nextSentenceSuggestions = [];
                
                // Search through all sentences for matches
                for (const sentence of sentences) {
                    const segmentNormalized = sentence.currentNormalized;
                    const segmentWords = segmentNormalized.split(/\s+/);
                    
                    // Check if segment starts with the next words
                    if (segmentWords.length >= nextWords.length) {
                        let matches = true;
                        for (let i = 0; i < nextWords.length; i++) {
                            if (segmentWords[i] !== nextWords[i].toLowerCase()) {
                                matches = false;
                                break;
                            }
                        }
                        
                        if (matches) {
                            nextSentenceSuggestions.push(sentence);
                            if (nextSentenceSuggestions.length >= 10) break; // Limit for performance
                        }
                    }
                }
                
                // If we have no current suggestions, use next sentence suggestions
                if (suggestions.length === 0 && nextSentenceSuggestions.length > 0) {
                    suggestions.push(...nextSentenceSuggestions);
                }
                // If we have current suggestions, add next sentence suggestions for continuation
                else if (suggestions.length > 0 && nextSentenceSuggestions.length > 0) {
                    // Add next sentence suggestions that aren't already in suggestions
                    for (const nextSuggestion of nextSentenceSuggestions) {
                        if (!suggestions.some(s => s.currentNormalized === nextSuggestion.currentNormalized)) {
                            suggestions.push(nextSuggestion);
                            if (suggestions.length >= 15) break; // Limit total
                        }
                    }
                }
            }
        }
    }
    
    // The split is only right at a real dead end: not when the words are spoken one after the other across
    // punctuation or a transcript segment boundary (the search finds such phrases).
    if (suggestions.length === 0 && inputWords.length > 1 && !previouslyHadPrefixMatch && !shouldSkipAutoSemicolon
            && !isSpokenContiguously(inputValue)) {
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
                setSentenceInput(newValue);
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
    sentenceAutocompleteState.keyboardSelection = false;

    // Render suggestions - match styling from Sentence Suggestions
    sentenceAutocompleteContainer.innerHTML = '';
    suggestions.forEach((entry, index) => {
        const suggestionItem = document.createElement('div');
        suggestionItem.className = 'suggestion-item';
        suggestionItem.style.cssText = 'padding: 8px 12px; cursor: pointer; border-bottom: 1px solid var(--border);';
        suggestionItem.addEventListener('mouseenter', () => {
            sentenceAutocompleteState.selectedIndex = index;
            sentenceAutocompleteState.keyboardSelection = false;
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
            sourceHtml = `<span class="suggestion-source">${escapeHtml(fileName)}</span>`;
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
        const selected = index === sentenceAutocompleteState.selectedIndex;
        item.classList.toggle('is-selected', selected);
        if (selected && sentenceAutocompleteState.keyboardSelection) {
            item.scrollIntoView({ block: 'nearest' });
        }
    });
}

function navigateSentenceAutocomplete(direction) {
    if (!sentenceAutocompleteState.isVisible || sentenceAutocompleteState.suggestions.length === 0) {
        return;
    }
    
    sentenceAutocompleteState.selectedIndex += direction;
    sentenceAutocompleteState.keyboardSelection = true;
    
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
                    setSentenceInput(newFullValue);
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
    
    // If still no word found, continue where these words are spoken in a transcript (whole words only,
    // also across segment boundaries)
    if (!bestNextWord) {
        bestNextWord = nextSpokenWord(currentInput);
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
        // But also check if we're in the middle of a multi-sentence segment
        if (isComplete) {
            // First, try to find if current input is part of a longer multi-sentence segment
            let foundContinuation = false;
            
            for (const entry of sentenceAutocompleteState.suggestions) {
                const segmentNormalized = entry.currentNormalized;
                const segmentText = entry.current;
                
                // Check if current input is a prefix of a longer segment (multi-sentence)
                if (segmentNormalized.startsWith(normalizedInput + ' ') && segmentNormalized.length > normalizedInput.length) {
                    // Get the next word/phrase after current input
                    const remaining = segmentText.substring(normalizedInput.length).trim();
                    if (remaining) {
                        // For multi-sentence segments, take more than just the next word
                        // Take up to the next sentence boundary or reasonable chunk
                        let nextChunk = remaining;
                        const sentenceEnd = remaining.search(/[.!?]/);
                        if (sentenceEnd > 0 && sentenceEnd < 50) {
                            // Take up to first sentence ending if it's reasonable
                            nextChunk = remaining.substring(0, sentenceEnd + 1);
                        } else {
                            // Otherwise take first few words (up to 3 words or 30 chars)
                            const words = remaining.split(/\s+/);
                            nextChunk = words.slice(0, Math.min(3, words.length)).join(' ');
                            if (nextChunk.length < remaining.length && !nextChunk.match(/[.!?]$/)) {
                                nextChunk += ' '; // Add space if not complete sentence
                            }
                        }
                        
                        bestNextWord = nextChunk;
                        bestSegment = segmentText;
                        foundContinuation = true;
                        break;
                    }
                }
            }
            
            // If no multi-sentence continuation found, fall back to single word approach
            if (!foundContinuation) {
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

        setSentenceInput(newFullValue);

        // Update accepted text for next Tab press (track only the active group)
        sentenceAutocompleteState.currentAcceptedText = newGroupValue;

        // Update suggestions for the new input
        updateSentenceAutocomplete();
    }
    // No continuation: the text stays as it is (a new group is started by typing ";").
}

function selectSentenceSuggestion(sentence) {
    if (!elements.sentenceInput) {
        return;
    }
    const value = elements.sentenceInput.value;
    const lastSemicolonIndex = value.lastIndexOf(';');
    setSentenceInput(lastSemicolonIndex === -1 ? sentence : `${value.slice(0, lastSemicolonIndex + 1)} ${sentence}`);
    sentenceAutocompleteState.currentAcceptedText = sentence;
    hideSentenceAutocomplete();
    elements.sentenceInput.focus();
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
    setSentenceInput(autoSemicolon.previousValue);
    sentenceAutocompleteState.lastAutoSemicolon = null;
    
    // Reset accepted text to match restored state
    const lastSemicolonIndex = autoSemicolon.previousValue.lastIndexOf(';');
    const activeGroup = lastSemicolonIndex === -1 
        ? autoSemicolon.previousValue 
        : autoSemicolon.previousValue.slice(lastSemicolonIndex + 1);
    sentenceAutocompleteState.currentAcceptedText = activeGroup.trim();
    sentenceAutocompleteState.noSplitPrefix = activeGroup.trim().toLowerCase();

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
    markTimelineDropTarget(null);
    if (elements.timelineList) {
        elements.timelineList.classList.remove('drag-active');
    }
    document.querySelectorAll('.timeline-item.drag-source').forEach(item => item.classList.remove('drag-source'));
    document.querySelectorAll('.phrase-container.drag-source').forEach(item => item.classList.remove('drag-source'));
}

/**
 * Trims of a timeline entry in ms, relative to its clip.
 * Entries written by versions that re-rendered clips on every trim keep the real values in
 * displayStartTrim/displayEndTrim (startTrim/endTrim were zeroed once a "_trimmed_" file existed).
 */
function getEntryTrims(entry) {
    const pick = (display, plain) => {
        const preferred = entry.originalClipPath && display !== undefined && display !== null ? Number(display) : Number(plain);
        return Number.isFinite(preferred) && preferred >= 0 ? preferred : 0;
    };
    return {
        start: pick(entry.displayStartTrim, entry.startTrim),
        end: pick(entry.displayEndTrim, entry.endTrim)
    };
}

function getEntrySpeed(entry) {
    const speed = Number(entry.speed);
    return Number.isFinite(speed) && speed > 0 ? speed : 1;
}

/** Timeline entry -> the item ClipPlayer and the merge endpoint understand. */
function entryToItem(entry) {
    const trims = getEntryTrims(entry);
    return {
        ...makeClipRef({
            clip: entry.originalClipPath || entry.file,
            source: entry.sourceVideo,
            originalStart: entry.originalStart,
            originalEnd: entry.originalEnd
        }),
        startTrim: trims.start,
        endTrim: trims.end,
        speed: getEntrySpeed(entry),
        title: entry.phrase || 'Clip'
    };
}

function buildTimelineEntryFromContainer(container, match = null) {
    const card = resultCards.get(container);
    const selected = match || (card ? card.selectedMatch() : null);
    if (!card || !selected || !selected.clip) {
        return null;
    }
    const trims = clampTrims(selected.startTrim, selected.endTrim, card.selectedMatch() === selected ? card.waveform.getDuration() : (selected.durationMs || 0));
    return {
        phrase: card.phrase || 'Clip',
        file: selected.clip,
        matchLabel: selected.label,
        startTrim: trims.start,
        endTrim: trims.end,
        originalClipPath: selected.clip,
        originalStart: selected.originalStart,
        originalEnd: selected.originalEnd,
        sourceVideo: selected.source,
        silenceStart: selected.silenceStart,
        silenceEnd: selected.silenceEnd,
        isSilenceEntry: selected.isSilence
    };
}

function createTimelineEntry(baseEntry) {
    if (!baseEntry || !baseEntry.file) {
        return null;
    }
    const startTrim = Math.max(0, Number.parseInt(baseEntry.startTrim, 10) || 0);
    const endTrim = Math.max(0, Number.parseInt(baseEntry.endTrim, 10) || 0);
    return {
        id: generateId('timeline'),
        phrase: baseEntry.phrase || 'Clip',
        file: baseEntry.file,
        matchLabel: baseEntry.matchLabel || '',
        startTrim,
        endTrim,
        // Mirrors of the trims, kept so projects stay readable by versions that expect them.
        displayStartTrim: startTrim,
        displayEndTrim: endTrim,
        rerendered: false,
        speed: 1,
        enabled: baseEntry.enabled !== undefined ? baseEntry.enabled : true,
        addedAt: new Date().toISOString(),
        originalClipPath: baseEntry.originalClipPath || baseEntry.file,
        originalStart: baseEntry.originalStart,
        originalEnd: baseEntry.originalEnd,
        sourceVideo: baseEntry.sourceVideo || '',
        silenceStart: baseEntry.silenceStart,
        silenceEnd: baseEntry.silenceEnd,
        isSilenceEntry: baseEntry.isSilenceEntry || false
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
        // A private type: text fields ignore it (plain text would be inserted where the drag is released)
        event.dataTransfer.setData('application/x-cinephrase-clip', 'search');
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

    // dragstart always targets the item itself, so remember where the pointer went down and refuse to start a
    // card drag from the controls inside it (trim handles, sliders, buttons).
    let pressOrigin = null;
    item.addEventListener('pointerdown', event => {
        pressOrigin = event.target;
    }, true);

    item.addEventListener('dragstart', event => {
        const interactive = '.waveform-container, .timeline-move-controls, input, button, select, label';
        if (pressOrigin && pressOrigin.closest && pressOrigin.closest(interactive)) {
            event.preventDefault();
            return;
        }

        dragState.type = 'timelineItem';
        dragState.data = { id: entry.id };
        item.classList.add('drag-source');
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('application/x-cinephrase-clip', 'timeline');
        elements.timelineList.classList.add('drag-active');
    });
    item.addEventListener('dragend', resetDragState);
}

/**
 * Where a drop at the pointer inserts into the timeline: before the first card whose middle lies beyond the
 * pointer, otherwise at the end. Cards are not shifted while dragging, so the preview and the drop always
 * agree. The list is a row on wide screens and a column on narrow ones.
 * Returns the insert position (0 … number of clips) and the card edge that shows it.
 */
function timelineDropTarget(event) {
    const items = Array.from(elements.timelineList.querySelectorAll('.timeline-item'));
    const vertical = getComputedStyle(elements.timelineList).flexDirection.startsWith('column');
    const pointer = vertical ? event.clientY : event.clientX;
    const next = items.find(item => {
        const rect = item.getBoundingClientRect();
        return pointer < (vertical ? rect.top + rect.height / 2 : rect.left + rect.width / 2);
    });
    if (next) {
        return { index: Number(next.dataset.index), marker: next, edge: 'drop-before' };
    }
    return { index: getTimelineEntries().length, marker: items[items.length - 1] || null, edge: 'drop-after' };
}

/** Position a timeline clip ends up at when it is dropped at `insertIndex`, or -1 if that leaves it in place. */
function timelineMovePosition(entryId, insertIndex) {
    const from = getTimelineEntries().findIndex(entry => entry.id === entryId);
    const to = insertIndex > from ? insertIndex - 1 : insertIndex;
    return from === -1 || to === from ? -1 : to;
}

let timelineDropMarker = null;

function markTimelineDropTarget(target) {
    const element = target ? target.marker : null;
    const edge = target ? target.edge : null;
    if (timelineDropMarker && (timelineDropMarker.element !== element || timelineDropMarker.edge !== edge)) {
        timelineDropMarker.element.classList.remove(timelineDropMarker.edge);
        timelineDropMarker = null;
    }
    if (element && !timelineDropMarker) {
        element.classList.add(edge);
        timelineDropMarker = { element, edge };
    }
}

function handleTimelineDragEnter(event) {
    if (!dragState.type || !activeProject) {
        return;
    }
    event.preventDefault();
    elements.timelineList.classList.add('drag-active');
}

function handleTimelineDragOver(event) {
    if (!dragState.type || !activeProject) {
        return;
    }
    event.preventDefault();
    const moving = dragState.type === 'timelineItem';
    if (event.dataTransfer) {
        event.dataTransfer.dropEffect = moving ? 'move' : 'copy';
    }
    const target = timelineDropTarget(event);
    const staysInPlace = moving && timelineMovePosition(dragState.data.id, target.index) === -1;
    markTimelineDropTarget(staysInPlace ? null : target);
}

function handleTimelineDragLeave(event) {
    if (!dragState.type) {
        return;
    }
    const related = event.relatedTarget;
    if (!related || !elements.timelineList.contains(related)) {
        elements.timelineList.classList.remove('drag-active');
        markTimelineDropTarget(null);
    }
}

function handleTimelineDrop(event) {
    const { type, data } = dragState;
    if (type && data && activeProject) {
        event.preventDefault();
        const insertIndex = timelineDropTarget(event).index;
        if (type === 'searchClip') {
            const entry = createTimelineEntry(data);
            if (entry) {
                updateTimeline(entries => {
                    entries.splice(Math.min(insertIndex, entries.length), 0, entry);
                });
            }
        } else if (type === 'timelineItem') {
            moveTimelineEntryTo(data.id, timelineMovePosition(data.id, insertIndex));
        }
    }
    resetDragState();
}

function getTimelineEntries() {
    return Array.isArray(activeProject?.data?.timeline) ? activeProject.data.timeline : [];
}

function setTimelineTrimValue(entryId, field, value) {
    if (!activeProject || !entryId) {
        return;
    }
    let safeValue;
    if (field === 'speed') {
        const parsedValue = Number.parseFloat(value);
        safeValue = Number.isFinite(parsedValue) && parsedValue > 0 ? parsedValue : 1.0;
    } else {
        const parsedValue = Number.parseInt(value, 10);
        safeValue = Number.isFinite(parsedValue) && parsedValue >= 0 ? parsedValue : 0;
    }
    recordTimelineUndo(`${field}:${entryId}`);
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
    // The cards are rebuilt: remember which control had the focus so the keyboard does not lose its place
    const focusable = 'button, input, select, [tabindex]';
    const focused = document.activeElement && elements.timelineList.contains(document.activeElement) ? document.activeElement : null;
    const focusedCard = focused ? focused.closest('.timeline-item') : null;
    const focusPosition = focusedCard
        ? { id: focusedCard.dataset.id, index: Array.from(focusedCard.querySelectorAll(focusable)).indexOf(focused) }
        : null;        // index -1: the card itself had the focus
    elements.timelineList.innerHTML = '';
    updateTimelineActionButtons();

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
        item.tabIndex = 0;
        item.setAttribute('aria-label', `Clip ${index + 1}: ${entry.phrase || 'clip'}. Enter plays, Alt+arrow keys move, Delete removes.`);

        const indexBadge = document.createElement('div');
        indexBadge.className = 'timeline-index';
        indexBadge.textContent = String(index + 1);

        // Add move up/down buttons for mobile reordering
        const moveControls = document.createElement('div');
        moveControls.className = 'timeline-move-controls';
        moveControls.draggable = false;
        
        const moveUpBtn = document.createElement('button');
        moveUpBtn.className = 'timeline-move-btn timeline-move-up';
        moveUpBtn.innerHTML = '↑';
        moveUpBtn.title = 'Move up';
        moveUpBtn.draggable = false;
        moveUpBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            e.preventDefault();
            moveTimelineItem(entry.id, 'up');
        });
        
        const moveDownBtn = document.createElement('button');
        moveDownBtn.className = 'timeline-move-btn timeline-move-down';
        moveDownBtn.innerHTML = '↓';
        moveDownBtn.title = 'Move down';
        moveDownBtn.draggable = false;
        moveDownBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            e.preventDefault();
            moveTimelineItem(entry.id, 'down');
        });
        
        // Disable up button for first item, down button for last item
        if (index === 0) {
            moveUpBtn.disabled = true;
        }
        if (index === entries.length - 1) {
            moveDownBtn.disabled = true;
        }
        
        moveControls.appendChild(moveUpBtn);
        moveControls.appendChild(moveDownBtn);

        const body = document.createElement('div');
        body.className = 'timeline-body';

        const meta = document.createElement('div');
        meta.className = 'timeline-meta';

        const title = document.createElement('div');
        title.className = 'timeline-title';
        title.style.display = 'flex';
        title.style.alignItems = 'center';
        title.style.gap = '6px';
        const titleText = document.createElement('strong');
        titleText.className = 'timeline-title-text';
        titleText.textContent = entry.phrase || `Clip ${index + 1}`;
        titleText.title = titleText.textContent;        // long phrases are cut with an ellipsis
        title.appendChild(titleText);
        // Add info icon with tooltip
        if (entry.file) {
            const infoIcon = document.createElement('span');
            infoIcon.className = 'info-icon';
            infoIcon.title = `Original Source: ${entry.sourceVideo || entry.originalSource || 'Unknown'}\n` +
                `Current Clip: ${entry.file}\n` +
                `Start: ${Number.isFinite(Number(entry.originalStart)) ? Number(entry.originalStart).toFixed(2) : '0.00'}s\n` +
                `End: ${Number.isFinite(Number(entry.originalEnd)) && entry.originalEnd !== null ? Number(entry.originalEnd).toFixed(2) : 'N/A'}s`;
            infoIcon.textContent = 'ⓘ';
            infoIcon.style.cursor = 'pointer';
            infoIcon.style.fontSize = '0.8em';
            infoIcon.style.opacity = '0.6';
            infoIcon.addEventListener('click', async (e) => {
                e.stopPropagation();
                const sourcePath = entry.sourceVideo || entry.originalSource || 'Unknown';
                if (sourcePath !== 'Unknown') {
                    try {
                        if (!await copyText(sourcePath)) {
                            return;
                        }
                        // Visual feedback
                        infoIcon.style.opacity = '1';
                        infoIcon.style.backgroundColor = '#4CAF50';
                        infoIcon.style.color = 'white';
                        infoIcon.style.padding = '2px 4px';
                        infoIcon.style.borderRadius = '3px';
                        setTimeout(() => {
                            infoIcon.style.opacity = '0.6';
                            infoIcon.style.backgroundColor = '';
                            infoIcon.style.color = '';
                            infoIcon.style.padding = '';
                            infoIcon.style.borderRadius = '';
                        }, 1000);
                    } catch (err) {
                        console.error('Failed to copy to clipboard:', err);
                    }
                }
            });
            title.appendChild(infoIcon);
        }
        meta.appendChild(title);
        // Which match this is: variants of one phrase are otherwise indistinguishable
        const origin = entry.matchLabel || (entry.sourceVideo ? entry.sourceVideo.split('/').pop() : '');
        if (origin) {
            const originLine = document.createElement('div');
            originLine.className = 'timeline-origin';
            originLine.textContent = origin;
            originLine.title = origin;
            meta.appendChild(originLine);
        }

        body.appendChild(meta);

        const trimControls = document.createElement('div');
        trimControls.className = 'timeline-trim-controls';

        const trims = getEntryTrims(entry);
        const currentSpeed = getEntrySpeed(entry);
        // Trimming only stores the new values and previews them; the clip itself is cut once, at merge time.
        const timelineWaveform = createWaveformVisualization({
            clipRef: makeClipRef(entryToItem(entry)),
            startTrim: trims.start,
            endTrim: trims.end,
            speed: currentSpeed,
            onTrimChange: (startTrim, endTrim, byUser) => setEntryTrims(entry.id, startTrim, endTrim, byUser),
            onTrimRelease: () => playTimelineItem(entry.id)
        });
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
        speedSlider.setAttribute('aria-label', 'Playback speed');
        speedSlider.className = 'speed-slider';
        speedSlider.min = '50';
        speedSlider.max = '100';
        speedSlider.value = String(Math.round(currentSpeed * 100));
        speedSlider.step = '5';
        speedSlider.draggable = false;
        
        speedSlider.addEventListener('input', () => {
            const speedPercent = parseInt(speedSlider.value, 10);
            const speed = speedPercent / 100;
            speedValue.textContent = `${speedPercent}%`;
            setTimelineTrimValue(entry.id, 'speed', speed);
            timelineWaveform.setSpeed(speed);
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
            // (the entry itself is changed inside updateTimeline, after the undo snapshot has been taken)
            
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
        item.appendChild(moveControls);
        item.appendChild(body);
        item.appendChild(actions);
        item.dataset.index = String(index);
        registerTimelineItemDrag(item, entry);
        fragment.appendChild(item);
    });

    elements.timelineList.appendChild(fragment);
    isRenderingTimeline = false;
    if (focusPosition) {
        const card = Array.from(elements.timelineList.querySelectorAll('.timeline-item')).find(node => node.dataset.id === focusPosition.id);
        const control = card && focusPosition.index === -1 ? card : card ? card.querySelectorAll(focusable)[focusPosition.index] : null;
        if (control && !control.disabled) {
            control.focus({ preventScroll: true });
        }
    }
}

/** "Play timeline" and "Merge timeline" need at least one enabled clip. */
function updateTimelineActionButtons() {
    const playable = getTimelineEntries().some(entry => entry && (entry.originalClipPath || entry.file) && entry.enabled !== false);
    if (elements.timelinePlayButton) {
        elements.timelinePlayButton.disabled = !playable && !timelinePlaybackActive;
    }
    if (elements.timelineMergeButton) {
        elements.timelineMergeButton.disabled = !playable || mergeRunning || !activeProject;
    }
}

// ---------------------------------------------------------------------------------------------------------
// Timeline undo / redo (Ctrl+Z, Ctrl+Shift+Z or Ctrl+Y, and the buttons in the timeline header)
// ---------------------------------------------------------------------------------------------------------

const TIMELINE_UNDO_LIMIT = 100;
const timelineHistory = { undo: [], redo: [], lastKey: null, lastAt: 0 };

function resetTimelineHistory() {
    timelineHistory.undo.length = 0;
    timelineHistory.redo.length = 0;
    timelineHistory.lastKey = null;
    updateTimelineUndoButtons();
}

function updateTimelineUndoButtons() {
    if (elements.timelineUndoButton) {
        elements.timelineUndoButton.disabled = !timelineHistory.undo.length;
    }
    if (elements.timelineRedoButton) {
        elements.timelineRedoButton.disabled = !timelineHistory.redo.length;
    }
}

/**
 * Remember the timeline as it is now, before a change. Changes with the same `key` in quick succession (the
 * steps of one trim drag or one move of the speed slider) are one undo step.
 */
function recordTimelineUndo(key = null) {
    if (!activeProject || isApplyingProject) {
        return;
    }
    const now = performance.now();
    if (key && timelineHistory.lastKey === key && now - timelineHistory.lastAt < 1500) {
        timelineHistory.lastAt = now;
        return;
    }
    timelineHistory.undo.push(JSON.stringify(getTimelineEntries()));
    if (timelineHistory.undo.length > TIMELINE_UNDO_LIMIT) {
        timelineHistory.undo.shift();
    }
    timelineHistory.redo.length = 0;
    timelineHistory.lastKey = key;
    timelineHistory.lastAt = now;
    updateTimelineUndoButtons();
}

function restoreTimelineSnapshot(from, to) {
    if (!activeProject || !from.length) {
        return;
    }
    to.push(JSON.stringify(getTimelineEntries()));
    const snapshot = JSON.parse(from.pop());
    timelineHistory.lastKey = null;
    stopClipPlayback();
    updateProjectData(data => {
        data.timeline = snapshot;
    });
    renderTimeline();
    updateTimelineUndoButtons();
}

function undoTimeline() {
    restoreTimelineSnapshot(timelineHistory.undo, timelineHistory.redo);
}

function redoTimeline() {
    restoreTimelineSnapshot(timelineHistory.redo, timelineHistory.undo);
}

document.addEventListener('keydown', event => {
    if (!(event.ctrlKey || event.metaKey) || event.altKey) {
        return;
    }
    const key = event.key.toLowerCase();
    if (key !== 'z' && key !== 'y') {
        return;
    }
    // Text fields keep their own undo; everything else (match lists, sliders, buttons) has none
    const target = event.target;
    const textEntry = target && (target.isContentEditable || target.tagName === 'TEXTAREA'
        || (target.tagName === 'INPUT' && !/^(range|checkbox|radio|button|submit|file|color)$/.test(target.type)));
    if (textEntry) {
        return;
    }
    event.preventDefault();
    if (key === 'y' || event.shiftKey) {
        redoTimeline();
    } else {
        undoTimeline();
    }
});

function updateTimeline(mutator, options) {
    if (!activeProject || typeof mutator !== 'function') {
        return;
    }
    if (timelinePlaybackActive) {
        stopClipPlayback();     // it plays a snapshot of the clips: what is changed now would not be heard
    }
    recordTimelineUndo();
    // Adding, removing and moving clips is saved right away, not after the typing delay
    updateProjectData(data => {
        if (!Array.isArray(data.timeline)) {
            data.timeline = [];
        }
        mutator(data.timeline);
    }, { immediate: true, ...options });
    renderTimeline();
}

/** Show for a moment that a button's action happened (the effect may be off screen). */
function flashButton(button, text) {
    if (!button || button.dataset.label) {
        return;
    }
    button.dataset.label = button.textContent;
    button.textContent = text;
    button.classList.add('is-confirmed');
    setTimeout(() => {
        button.textContent = button.dataset.label;
        delete button.dataset.label;
        button.classList.remove('is-confirmed');
    }, 1200);
}

/** Bring a timeline clip into view inside the strip (the page itself does not scroll). */
function revealTimelineItem(entryId) {
    const item = Array.from(elements.timelineList.querySelectorAll('.timeline-item')).find(node => node.dataset.id === entryId);
    const scroller = item ? item.closest('.timeline-scroll') : null;
    if (scroller && scroller.scrollWidth > scroller.clientWidth) {
        const offset = item.getBoundingClientRect().left - scroller.getBoundingClientRect().left;
        scroller.scrollBy({ left: offset - (scroller.clientWidth - item.offsetWidth) / 2, behavior: 'smooth' });
    }
}

/** Append the selected match of a result card to the timeline. Returns whether a clip was added. */
function handleAddContainerToTimeline(container) {
    if (!activeProject || !container) {
        return false;
    }
    const timelineEntry = createTimelineEntry(buildTimelineEntryFromContainer(container));
    if (!timelineEntry) {
        return false;
    }
    updateTimeline(entries => {
        entries.push(timelineEntry);
    });
    revealTimelineItem(timelineEntry.id);
    return true;
}

/** Add every match of a card (each with the trims set for it) so the variants can be compared in place. */
function handleAddAllVariantsToTimeline(container) {
    const card = resultCards.get(container);
    if (!activeProject || !card) {
        return;
    }
    const additions = card.matches
        .map(match => createTimelineEntry(buildTimelineEntryFromContainer(container, match)))
        .filter(Boolean);
    if (additions.length) {
        updateTimeline(entries => {
            entries.push(...additions);
        });
    }
}

function addAllMatchesToTimeline() {
    if (!activeProject) {
        return;
    }
    const additions = Array.from(document.querySelectorAll('.phrase-container'))
        .map(container => createTimelineEntry(buildTimelineEntryFromContainer(container)))
        .filter(Boolean);
    if (additions.length) {
        updateTimeline(entries => {
            entries.push(...additions);
        });
    }
}

/** Keys on a focused timeline card (not on the controls inside it). */
function handleTimelineListKeydown(event) {
    const card = event.target.classList && event.target.classList.contains('timeline-item') ? event.target : null;
    if (!card || event.ctrlKey || event.metaKey) {
        return;
    }
    const entryId = card.dataset.id;
    const backward = event.key === 'ArrowLeft' || event.key === 'ArrowUp';
    const forward = event.key === 'ArrowRight' || event.key === 'ArrowDown';
    if (event.altKey && (backward || forward)) {
        moveTimelineItem(entryId, backward ? 'up' : 'down');
    } else if (event.altKey) {
        return;
    } else if (event.key === 'Delete' || event.key === 'Backspace') {
        // keep the keyboard on the strip: focus goes to the clip that takes this one's place
        const cards = Array.from(elements.timelineList.querySelectorAll('.timeline-item'));
        const position = cards.indexOf(card);
        removeTimelineItem(entryId);
        const remaining = elements.timelineList.querySelectorAll('.timeline-item');
        const next = remaining[Math.min(position, remaining.length - 1)];
        if (next) {
            next.focus();
        }
    } else if (event.key === 'Enter' || event.key === ' ') {
        playTimelineItem(entryId);
    } else {
        return;
    }
    event.preventDefault();
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

/** Store new trims for a timeline entry (the clip itself is never re-rendered for a trim). */
function setEntryTrims(entryId, startTrim, endTrim, undoable = true) {
    if (undoable) {
        recordTimelineUndo(`trim:${entryId}`);
    }
    updateProjectData(data => {
        const target = Array.isArray(data.timeline) ? data.timeline.find(item => item && item.id === entryId) : null;
        if (!target) {
            return;
        }
        target.file = target.originalClipPath || target.file;
        target.rerendered = false;
        target.startTrim = startTrim;
        target.endTrim = endTrim;
        target.displayStartTrim = startTrim;
        target.displayEndTrim = endTrim;
    });
}

/** Highlight the timeline item that is on air (and keep it in view during "Play timeline"). */
function markTimelinePlaying(entryId, scrollIntoView = false) {
    if (!elements.timelineList) {
        return;
    }
    elements.timelineList.querySelectorAll('.timeline-item.is-playing').forEach(item => {
        item.classList.remove('is-playing');
        const waveform = item.querySelector('.waveform-container');
        if (waveform && waveform.setPlayhead) {
            waveform.setPlayhead(null);
        }
    });
    if (!entryId) {
        return null;
    }
    const item = Array.from(elements.timelineList.querySelectorAll('.timeline-item')).find(node => node.dataset.id === entryId);
    if (!item) {
        return null;
    }
    item.classList.add('is-playing');
    const scroller = scrollIntoView ? item.closest('.timeline-scroll') : null;
    if (scroller && scroller.scrollWidth > scroller.clientWidth) {
        // Only the timeline strip scrolls (scrollIntoView would also pull the page away from the preview).
        const offset = item.getBoundingClientRect().left - scroller.getBoundingClientRect().left;
        scroller.scrollBy({ left: offset - (scroller.clientWidth - item.offsetWidth) / 2, behavior: 'smooth' });
    }
    return item.querySelector('.waveform-container');
}

function playTimelineItem(itemId) {
    const entry = getTimelineEntries().find(item => item && item.id === itemId);
    const stage = getMainStage();
    if (!entry || !stage || !(entry.originalClipPath || entry.file)) {
        return;
    }
    let waveform = null;
    getClipPlayer().play(stage, [entryToItem(entry)], {
        onItemStart: () => {
            updateVideoPlayerTitle(entry.phrase || 'Timeline item');
            waveform = markTimelinePlaying(entry.id);
        },
        onProgress: (index, fraction) => {
            if (waveform && waveform.setPlayhead) waveform.setPlayhead(fraction);
        },
        onEnd: () => markTimelinePlaying(null),
        replay: () => playTimelineItem(itemId)
    });
}

function removeTimelineItem(itemId) {
    updateTimeline(entries => {
        const index = entries.findIndex(item => item.id === itemId);
        if (index !== -1) {
            entries.splice(index, 1);
        }
    });
}

let timelinePlaybackActive = false;

function updateTimelinePlayButton() {
    if (elements.timelinePlayButton) {
        elements.timelinePlayButton.textContent = timelinePlaybackActive ? 'Stop' : 'Play timeline';
    }
}

/**
 * Play all enabled timeline clips back to back exactly as the merge will cut them (gapless, on the audio
 * clock). Pressing the button again stops.
 */
function playTimeline() {
    if (timelinePlaybackActive) {
        stopClipPlayback();
        return;
    }
    const stage = getMainStage();
    const entries = getTimelineEntries().filter(entry => entry && (entry.originalClipPath || entry.file) && entry.enabled !== false);
    if (!entries.length || !stage) {
        return;
    }
    let waveform = null;
    getClipPlayer().play(stage, entries.map(entryToItem), {
        onItemStart: index => {
            updateVideoPlayerTitle(entries[index].phrase || `Timeline item ${index + 1}`);
            waveform = markTimelinePlaying(entries[index].id, true);
        },
        onProgress: (index, fraction) => {
            if (waveform && waveform.setPlayhead) waveform.setPlayhead(fraction);
        },
        onEnd: () => {
            timelinePlaybackActive = false;
            updateTimelinePlayButton();
            markTimelinePlaying(null);
            updateVideoPlayerTitle('Preview');
        },
        replay: playTimeline
    });
    // Set after play(): starting a playback first ends the one before it, whose onEnd clears the flag.
    timelinePlaybackActive = getClipPlayer().isPlaying(stage);
    updateTimelinePlayButton();
    if (isMobileLayout()) {
        switchMobileSection('viewer');
    }
}

let mergeRunning = false;

function setMergeBusy(busy) {
    mergeRunning = busy;
    updateTimelineActionButtons();
}

/** Forget the merged video and its download link (another project, or a new merge is on its way). */
function clearMergedOutput() {
    const link = document.getElementById('mergedDownloadLink');
    if (link) {
        link.remove();
    }
    document.querySelectorAll('.merge-note').forEach(note => note.remove());
    const video = elements.videoPlayer;
    if (video && video.getAttribute('src')) {
        video.pause();
        video.removeAttribute('src');
        video.load();
        updateVideoPlayerTitle('Preview');
    }
}

function showMergedDownloadLink(mergedPath, warning) {
    const downloadId = 'mergedDownloadLink';
    const existing = document.getElementById(downloadId);
    if (existing) {
        existing.remove();
    }
    document.querySelectorAll('.merge-note').forEach(note => note.remove());
    const link = document.createElement('a');
    link.id = downloadId;
    link.className = 'button ghost-button';
    link.href = getVideoUrl(mergedPath);
    link.download = mergedPath.split('/').pop();
    link.textContent = 'Download merged video';
    const videoCard = elements.videoPlayer ? elements.videoPlayer.closest('.video-card') : null;
    (videoCard || document.body).appendChild(link);
    if (warning) {
        const note = document.createElement('p');
        note.className = 'result-notice merge-note';
        note.textContent = warning;
        (videoCard || document.body).appendChild(note);
    }
}

/**
 * Render the enabled timeline clips into one video. The server cuts every clip from its library source at
 * exactly the trims previewed here; progress arrives as server-sent events.
 */
function mergeTimeline() {
    const entries = getTimelineEntries().filter(entry => entry && (entry.originalClipPath || entry.file) && entry.enabled !== false);
    if (!entries.length || mergeRunning) {
        return;
    }
    clearMergedOutput();        // a failed merge must not leave the previous video looking like its result
    const videosToMerge = entries.map(entry => {
        const item = entryToItem(entry);
        return {
            title: item.title,
            clip: item.clip,
            source: item.source,
            originalStart: item.originalStart,
            originalEnd: item.originalEnd,
            startTrim: item.startTrim,
            endTrim: item.endTrim,
            speed: item.speed
        };
    });

    // Closing the connection is the cancel signal: the server stops rendering and removes its work files.
    const controller = new AbortController();
    let finished = false;
    const finish = () => {
        finished = true;
        hideMergeProgress();
        setMergeBusy(false);
    };
    showMergeProgress('Preparing merge...', 0, 1, () => {
        finish();
        controller.abort();
    });
    setMergeBusy(true);

    const handleEvent = data => {
        if (data.done) {
            finish();
            if (data.merged_video) {
                playMergedVideo(data.merged_video, elements.loopMergedVideo ? elements.loopMergedVideo.checked : false);
                showMergedDownloadLink(data.merged_video, data.warning);
                if (isMobileLayout()) {
                    setTimeout(() => switchMobileSection('viewer'), 500);
                }
            }
        } else if (data.error) {
            finish();
            console.error('[Merge] Error:', data.error);
            alert(`Error merging videos: ${data.error}`);
        } else if (data.message && data.progress !== undefined && data.total !== undefined) {
            updateMergeProgress(data.message, data.progress, data.total);
        }
    };

    fetch('/merge_videos_stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ videos: videosToMerge }),
        signal: controller.signal
    })
        .then(async response => {
            if (!response.ok) {
                const failure = await response.json().catch(() => ({}));
                throw new Error(failure.error || `HTTP error! status: ${response.status}`);
            }
            await readEventStream(response, data => {
                if (finished) {
                    return false;
                }
                handleEvent(data);
                return !finished;
            });
            if (!finished) {
                finish();
                alert('Error merging videos: the server closed the connection before the merge finished.');
            }
        })
        .catch(error => {
            if (finished) {
                return;
            }
            finish();
            console.error('Error merging timeline videos:', error);
            alert(`Error merging videos: ${error.message}`);
        });
}

function clearTimeline() {
    if (!getTimelineEntries().length) {
        return;
    }
    // No confirmation: Undo (Ctrl+Z) brings the clips back.
    stopClipPlayback();
    updateTimeline(entries => {
        entries.length = 0;
    });
}

/** Move a clip to position `target` of the timeline. Does nothing (no undo step, no save) if it is already there. */
function moveTimelineEntryTo(entryId, target) {
    const entries = getTimelineEntries();
    const index = entries.findIndex(entry => entry.id === entryId);
    if (!activeProject || index === -1 || target === index || target < 0 || target >= entries.length) {
        return;
    }
    const cardOf = () => Array.from(elements.timelineList.querySelectorAll('.timeline-item')).find(node => node.dataset.id === entryId);
    const before = cardOf() ? cardOf().getBoundingClientRect() : null;
    updateTimeline(list => {
        list.splice(target, 0, list.splice(index, 1)[0]);
    });
    const card = cardOf();
    if (before && card) {
        // The list was rebuilt and the card now sits one place further: scroll so it is where it was on screen.
        // (Otherwise a second tap on the same spot hits the neighbour and moves the clip back.)
        const after = card.getBoundingClientRect();
        const scroller = card.closest('.timeline-scroll');
        if (scroller && scroller.scrollWidth > scroller.clientWidth) {
            scroller.scrollLeft += after.left - before.left;
        }
        window.scrollBy(0, after.top - before.top);
    }
}

function moveTimelineItem(entryId, direction) {
    const index = getTimelineEntries().findIndex(entry => entry.id === entryId);
    if (index !== -1) {
        moveTimelineEntryTo(entryId, index + (direction === 'up' ? -1 : 1));
    }
}

/** Clips of older versions are named by a running index (or "_trimmed_"): a later search could overwrite them. */
function isLegacyClipName(clipPath) {
    return /_\d{5}\.mp4$|_trimmed_\d+_\d+\.mp4$/.test(clipPath || '');
}

/**
 * Preview clips live in temp/ and disappear when that folder is cleaned. Re-cut the missing ones from their
 * library sources. Entries from older versions are moved to a clip file named after its content, so their
 * picture can no longer be replaced by another search. Single clips are also restored on demand when played.
 */
async function restoreMissingClips() {
    const button = elements.timelineRestoreButton;
    const entries = getTimelineEntries().filter(entry => entry && (entry.originalClipPath || entry.file));
    if (!entries.length || (button && button.disabled)) {
        return;
    }
    const references = new Map();
    entries.forEach(entry => {
        const item = entryToItem(entry);
        references.set(clipRefKey(item), item);
    });

    const label = button ? button.textContent : '';
    if (button) {
        button.disabled = true;
    }
    let restored = 0;
    const failed = [];
    const moved = new Map();      // clip reference key -> new clip path
    let checked = 0;
    for (const [key, item] of references) {
        checked += 1;
        if (button) {
            button.textContent = `Checking ${checked}/${references.size}…`;
        }
        try {
            const response = await fetch('/ensure_clip', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ...makeClipRef(item), canonical: isLegacyClipName(item.clip) })
            });
            const result = await response.json().catch(() => ({}));
            if (!response.ok) {
                failed.push(`${item.title}: ${result.error || response.status}`);
                continue;
            }
            if (result.status === 'restored') {
                restored += 1;
            }
            if (result.clip && result.clip !== item.clip) {
                moved.set(key, result.clip);
            }
        } catch (error) {
            failed.push(`${item.title}: ${error.message}`);
        }
    }
    if (moved.size) {
        updateProjectData(data => {
            (Array.isArray(data.timeline) ? data.timeline : []).forEach(entry => {
                const clip = entry && moved.get(clipRefKey(entryToItem(entry)));
                if (clip) {
                    const trims = getEntryTrims(entry);
                    entry.file = clip;
                    entry.originalClipPath = clip;
                    entry.rerendered = false;
                    entry.startTrim = entry.displayStartTrim = trims.start;
                    entry.endTrim = entry.displayEndTrim = trims.end;
                }
            });
        });
    }
    if (button) {
        button.disabled = false;
        button.textContent = label;
    }
    if (restored || moved.size || failed.length) {
        waveformCache.clear();
        renderTimeline();
    }
    if (failed.length) {
        alert(`${restored} clip(s) restored, ${failed.length} could not be restored:\n\n${failed.slice(0, 10).join('\n')}`);
    } else {
        const changed = Math.max(restored, moved.size);
        setAutosaveStatus(changed ? `Restored ${changed} clip(s)` : 'All timeline clips are present', 'ready');
    }
}

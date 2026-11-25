// ================================================================
// Job Scheduler — Core Client Runtime
// Theme, API client, Formatting, Toasts, Modals, Log Drawer, Live Poller
// ================================================================

(function initThemeImmediate() {
  const saved = localStorage.getItem('theme');
  const preferred = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', saved || preferred);
})();

function getTheme() {
  return document.documentElement.getAttribute('data-theme') || 'light';
}

function updateThemeButtonUI() {
  const isDark = getTheme() === 'dark';
  const label = document.getElementById('theme-label');
  const sun = document.getElementById('theme-icon-sun');
  const moon = document.getElementById('theme-icon-moon');
  if (label) label.textContent = isDark ? 'Light Mode' : 'Dark Mode';
  if (sun) sun.style.display = isDark ? 'block' : 'none';
  if (moon) moon.style.display = isDark ? 'none' : 'block';
}

function toggleTheme() {
  const next = getTheme() === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  updateThemeButtonUI();
}

// --- Sidebar Mobile Toggle ---
function toggleSidebar(force) {
  const sb = document.getElementById('sidebar');
  const bd = document.getElementById('sidebar-backdrop');
  if (!sb || !bd) return;
  const open = typeof force === 'boolean' ? force : !sb.classList.contains('open');
  sb.classList.toggle('open', open);
  bd.classList.toggle('open', open);
}

// --- Safe HTML Escaping ---
function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// --- API Helper ---
async function apiCall(url, options = {}) {
  const opts = { ...options };
  opts.headers = { ...(options.headers || {}) };
  const token = localStorage.getItem('auth_token');
  if (token && !opts.headers['Authorization']) {
    opts.headers['Authorization'] = `Bearer ${token}`;
  }
  const response = await fetch(url, opts);
  if (response.status === 401) {
    window.location.href = '/login';
    return null;
  }
  return response;
}

async function apiJson(url, options = {}) {
  const res = await apiCall(url, options);
  if (!res) return null;
  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }
  if (!res.ok) {
    const msg = (data && (data.message || data.error)) || `Request failed (${res.status})`;
    throw new Error(msg);
  }
  return data;
}

// --- Time & Duration Helpers ---
function parseTime(timeStr) {
  if (!timeStr || typeof timeStr !== 'string') return null;
  const trimmed = timeStr.trim();
  if (!trimmed || trimmed === '-' || trimmed === 'null') return null;
  const normalized = trimmed
    .replace(' ', 'T')
    .replace(/\s+([+-]\d{2}:\d{2}):\d{2}$/, '$1')
    .replace(/\s+([+-]\d{2}:\d{2})$/, '$1')
    .replace(/\s+Z$/, 'Z');
  const d = new Date(normalized);
  if (isNaN(d.getTime()) || d.getFullYear() <= 1970) return null;
  return d;
}

function formatTime(timeStr) {
  const d = parseTime(timeStr);
  if (!d) return '—';
  return d.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function formatShortTime(timeStr) {
  const d = parseTime(timeStr);
  if (!d) return '—';
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  if (sameDay) {
    return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
  return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function formatRelativeTime(timeStr) {
  const d = parseTime(timeStr);
  if (!d) return '—';
  const diffSec = Math.round((d.getTime() - Date.now()) / 1000);
  const abs = Math.abs(diffSec);
  const future = diffSec > 0;
  let txt;
  if (abs < 5) return 'just now';
  if (abs < 60) txt = `${abs}s`;
  else if (abs < 3600) txt = `${Math.floor(abs / 60)}m`;
  else if (abs < 86400) {
    const h = Math.floor(abs / 3600);
    const m = Math.floor((abs % 3600) / 60);
    txt = m > 0 ? `${h}h ${m}m` : `${h}h`;
  } else {
    txt = `${Math.floor(abs / 86400)}d`;
  }
  return future ? `in ${txt}` : `${txt} ago`;
}

function formatDurationMs(ms) {
  if (ms === null || ms === undefined || isNaN(ms)) return '—';
  const safe = Math.max(0, ms);
  const totalSec = Math.floor(safe / 1000);
  if (totalSec < 60) return `${totalSec}s`;
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function computeDurationMs(startStr, endStr, status) {
  const s = parseTime(startStr);
  if (!s) return null;
  const e = parseTime(endStr);
  if (e && status !== 'Running') {
    return Math.max(0, e.getTime() - s.getTime());
  }
  if (status === 'Running') {
    return Math.max(0, Date.now() - s.getTime());
  }
  return null;
}

function formatDurationCell(startStr, endStr, status) {
  const ms = computeDurationMs(startStr, endStr, status);
  if (ms === null) return '—';
  if (status === 'Running') {
    return `<span class="tabular live-duration" data-start="${escapeHtml(startStr)}">${formatDurationMs(ms)}</span>`;
  }
  return `<span class="tabular">${formatDurationMs(ms)}</span>`;
}

// Live 1s duration ticker for any `.live-duration[data-start]` element
setInterval(() => {
  if (document.hidden) return;
  document.querySelectorAll('.live-duration[data-start]').forEach(el => {
    const s = parseTime(el.getAttribute('data-start'));
    if (s) {
      el.textContent = formatDurationMs(Date.now() - s.getTime());
    }
  });
}, 1000);

// --- Status & Schedule Formatters ---
function formatStatus(status, clickableExecId) {
  const st = status || 'Unknown';
  const clsMap = {
    Success: 'success',
    Failed: 'failed',
    Cancelled: 'cancelled',
    Lost: 'lost',
    Running: 'running',
    Scheduled: 'scheduled',
    Triggered: 'triggered'
  };
  const cls = clsMap[st] || '';
  if (clickableExecId) {
    return `<span class="status-badge ${cls} clickable" onclick="openLogDrawer(${Number(clickableExecId)})" title="Click to inspect log"><span class="dot"></span>${escapeHtml(st)}</span>`;
  }
  return `<span class="status-badge ${cls}"><span class="dot"></span>${escapeHtml(st)}</span>`;
}

function formatScheduleSummary(task) {
  if (!task) return '—';
  const st = task.scheduleType || '';
  if (st === 'cron' || st === 'stCron') {
    return `<code>${escapeHtml(task.cronExpr || '—')}</code>`;
  }
  if (st === 'time' || st === 'stTime') {
    return `<span class="badge">Daily ${escapeHtml(task.timeOfDay || '—')}</span>`;
  }
  if (st === 'interval' || st === 'stInterval') {
    const win = task.intervalStart && task.intervalEnd ? ` (${escapeHtml(task.intervalStart)}–${escapeHtml(task.intervalEnd)})` : '';
    return `<span class="badge">Every ${Number(task.intervalMinutes || 0)}m${win}</span>`;
  }
  if (st === 'onstart' || st === 'stOnStart') {
    return `<span class="badge">On Start</span>`;
  }
  return '<span class="muted">Manual</span>';
}

// --- Sidebar Running Badge ---
function updateSidebarRunningBadge(count) {
  const el = document.getElementById('nav-running-badge');
  if (!el) return;
  const n = Number(count || 0);
  if (n > 0) {
    el.textContent = String(n);
    el.classList.add('visible');
  } else {
    el.classList.remove('visible');
  }
}

// --- Toast Notifications ---
function showToast(opts) {
  const stack = document.getElementById('toast-stack');
  if (!stack) return;
  const {
    title = '',
    message = '',
    type = 'info',
    duration = 5000,
    actionLabel = '',
    onAction = null
  } = typeof opts === 'string' ? { title: opts, type: 'info' } : opts;

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <div class="toast-content">
      ${title ? `<div class="toast-title">${escapeHtml(title)}</div>` : ''}
      ${message ? `<div class="toast-msg">${escapeHtml(message)}</div>` : ''}
      ${actionLabel ? `<div class="toast-actions"><button type="button" class="btn btn-xs btn-secondary toast-action-btn">${escapeHtml(actionLabel)}</button></div>` : ''}
    </div>
    <button type="button" class="toast-close" aria-label="Dismiss">&times;</button>
  `;

  const remove = () => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(6px)';
    toast.style.transition = 'opacity 0.15s, transform 0.15s';
    setTimeout(() => toast.remove(), 160);
  };

  toast.querySelector('.toast-close').onclick = remove;
  if (actionLabel && onAction) {
    toast.querySelector('.toast-action-btn').onclick = () => {
      onAction();
      remove();
    };
  }

  stack.appendChild(toast);
  if (duration > 0) {
    setTimeout(remove, duration);
  }
}

// --- Confirm Modal ---
const confirmState = { onConfirm: null };

function showConfirmModal(titleOrOpts, message, onConfirm) {
  const modal = document.getElementById('confirm-modal');
  if (!modal) return;
  const opts = typeof titleOrOpts === 'object'
    ? titleOrOpts
    : { title: titleOrOpts, message, onConfirm, confirmText: 'Confirm', danger: true };

  document.getElementById('modal-title').textContent = opts.title || 'Confirm Action';
  document.getElementById('modal-message').textContent = opts.message || 'Are you sure?';
  const confirmBtn = document.getElementById('modal-confirm');
  confirmBtn.textContent = opts.confirmText || 'Confirm';
  confirmBtn.className = opts.danger === false ? 'btn btn-primary' : 'btn btn-danger';
  confirmBtn.disabled = false;
  confirmState.onConfirm = opts.onConfirm || null;
  modal.classList.add('open');
  setTimeout(() => confirmBtn.focus(), 30);
}

function hideConfirmModal() {
  const modal = document.getElementById('confirm-modal');
  if (!modal) return;
  modal.classList.remove('open');
  confirmState.onConfirm = null;
}

// --- In-Place Trigger Actions (No Page Jump!) ---
async function triggerTaskInPlace(taskId, taskName, { onTriggered = null, confirm = true } = {}) {
  const run = async () => {
    try {
      const res = await apiJson(`/api/trigger_task?id=${encodeURIComponent(taskId)}`, { method: 'POST' });
      const execId = res && res.executionId > 0 ? res.executionId : null;
      showToast({
        title: `Triggered "${taskName || 'Task #' + taskId}"`,
        message: execId ? `Execution #${execId} started.` : 'Task queued for execution.',
        type: 'success',
        duration: 7000,
        actionLabel: execId ? 'View Live Log' : '',
        onAction: execId ? () => openLogDrawer(execId) : null
      });
      if (onTriggered) onTriggered(res);
    } catch (err) {
      showToast({ title: 'Failed to trigger task', message: err.message, type: 'error' });
    }
  };
  if (confirm) {
    showConfirmModal({
      title: 'Run Task Now',
      message: `Manually trigger "${taskName || 'Task #' + taskId}" now?`,
      confirmText: 'Run Now',
      danger: false,
      onConfirm: run
    });
  } else {
    await run();
  }
}

async function triggerJobInPlace(jobId, jobName, { onTriggered = null, confirm = true } = {}) {
  const run = async () => {
    try {
      const res = await apiJson(`/api/trigger_job?id=${encodeURIComponent(jobId)}`, { method: 'POST' });
      const execId = res && res.executionId > 0 ? res.executionId : null;
      showToast({
        title: `Triggered "${jobName || 'Job #' + jobId}"`,
        message: execId ? `Execution #${execId} started.` : 'Job queued for execution.',
        type: 'success',
        duration: 7000,
        actionLabel: execId ? 'View Live Log' : '',
        onAction: execId ? () => openLogDrawer(execId) : null
      });
      if (onTriggered) onTriggered(res);
    } catch (err) {
      showToast({ title: 'Failed to trigger job', message: err.message, type: 'error' });
    }
  };
  if (confirm) {
    showConfirmModal({
      title: 'Run Job Now',
      message: `Manually trigger "${jobName || 'Job #' + jobId}" now?`,
      confirmText: 'Run Now',
      danger: false,
      onConfirm: run
    });
  } else {
    await run();
  }
}

// ================================================================
// Slide-Over Live Log Drawer
// ================================================================

const logDrawerState = {
  execId: null,
  offset: -1,
  rawText: '',
  status: '',
  autoScroll: true,
  wrap: true,
  search: '',
  timer: null,
  onStatusChange: null
};

function openLogDrawer(execId, { onStatusChange = null } = {}) {
  const drawer = document.getElementById('log-drawer');
  const backdrop = document.getElementById('log-drawer-backdrop');
  if (!drawer || !backdrop) {
    window.location.href = `/execution_log?id=${execId}`;
    return;
  }

  stopLogDrawerPoll();
  logDrawerState.execId = Number(execId);
  logDrawerState.offset = -1;
  logDrawerState.rawText = '';
  logDrawerState.status = '';
  logDrawerState.search = '';
  logDrawerState.onStatusChange = onStatusChange;

  const searchInput = document.getElementById('drawer-log-search');
  if (searchInput) searchInput.value = '';
  document.getElementById('drawer-title-id').textContent = `Execution #${execId}`;
  document.getElementById('drawer-sub').textContent = 'Loading execution metadata…';
  document.getElementById('drawer-meta').innerHTML = '';
  document.getElementById('drawer-log-content').textContent = 'Loading log…';
  document.getElementById('drawer-full-link').href = `/execution_log?id=${execId}`;
  document.getElementById('drawer-download-link').href = `/api/download_log?id=${execId}`;
  document.getElementById('drawer-cancel-btn').style.display = 'none';

  backdrop.classList.add('open');
  drawer.classList.add('open');
  document.body.style.overflow = 'hidden';

  fetchLogDrawerChunk();
}

function closeLogDrawer() {
  stopLogDrawerPoll();
  const drawer = document.getElementById('log-drawer');
  const backdrop = document.getElementById('log-drawer-backdrop');
  if (drawer) drawer.classList.remove('open');
  if (backdrop) backdrop.classList.remove('open');
  document.body.style.overflow = '';
  logDrawerState.execId = null;
}

function stopLogDrawerPoll() {
  if (logDrawerState.timer) {
    clearTimeout(logDrawerState.timer);
    logDrawerState.timer = null;
  }
}

function renderLogDrawerContent() {
  const el = document.getElementById('drawer-log-content');
  if (!el) return;
  const text = logDrawerState.rawText || '(No output yet)';
  const q = logDrawerState.search.trim();
  if (!q) {
    el.textContent = text;
  } else {
    const escaped = escapeHtml(text);
    const safeQ = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(safeQ, 'gi');
    el.innerHTML = escaped.replace(re, m => `<mark>${m}</mark>`);
  }
  el.classList.toggle('wrap', logDrawerState.wrap);
  if (logDrawerState.autoScroll) {
    el.scrollTop = el.scrollHeight;
  }
}

async function fetchLogDrawerChunk() {
  const execId = logDrawerState.execId;
  if (!execId) return;
  try {
    const data = await apiJson(`/api/execution_log?id=${execId}&offset=${logDrawerState.offset}&maxLen=131072`);
    if (!data || logDrawerState.execId !== execId) return;

    const prevStatus = logDrawerState.status;
    logDrawerState.status = data.status || '';

    document.getElementById('drawer-sub').innerHTML =
      `<a href="/task_detail?id=${data.taskId}">${escapeHtml(data.taskName || 'Task')}</a> / ${escapeHtml(data.jobName || 'Job')}`;

    const isRunning = data.status === 'Running' || data.status === 'Scheduled' || data.status === 'Triggered';
    document.getElementById('drawer-meta').innerHTML = `
      <span class="log-meta-item">${formatStatus(data.status)}</span>
      <span class="log-meta-item"><strong>Started</strong> ${formatTime(data.startTime)}</span>
      <span class="log-meta-item"><strong>Duration</strong> ${formatDurationCell(data.startTime, data.endTime, data.status)}</span>
      <span class="log-meta-item"><strong>PID</strong> <span class="mono">${data.pid || '—'}</span></span>
      ${!isRunning && data.exitCode !== undefined ? `<span class="log-meta-item"><strong>Exit</strong> <span class="mono">${data.exitCode}</span></span>` : ''}
    `;

    const cancelBtn = document.getElementById('drawer-cancel-btn');
    cancelBtn.style.display = isRunning ? 'inline-flex' : 'none';
    cancelBtn.onclick = () => {
      showConfirmModal({
        title: 'Cancel Execution',
        message: `Terminate running execution #${execId}?`,
        confirmText: 'Cancel Execution',
        danger: true,
        onConfirm: async () => {
          try {
            await apiJson(`/api/cancel_execution?id=${execId}`, { method: 'POST' });
            showToast({ title: `Cancelled execution #${execId}`, type: 'warning' });
            fetchLogDrawerChunk();
          } catch (e) {
            showToast({ title: 'Cancel failed', message: e.message, type: 'error' });
          }
        }
      });
    };

    if (logDrawerState.offset < 0 || data.reset) {
      logDrawerState.rawText = (data.skippedBytes ? '… (earlier output truncated; download full log to inspect) …\n' : '') + (data.content || '');
    } else if (data.content) {
      logDrawerState.rawText += data.content;
    }
    if (data.logFound) {
      logDrawerState.offset = data.offset;
    }
    renderLogDrawerContent();

    if (prevStatus && prevStatus !== data.status && logDrawerState.onStatusChange) {
      logDrawerState.onStatusChange(data.status);
    }

    if (isRunning && logDrawerState.execId === execId) {
      logDrawerState.timer = setTimeout(fetchLogDrawerChunk, 1500);
    }
  } catch (err) {
    if (logDrawerState.execId === execId) {
      document.getElementById('drawer-log-content').textContent = `Error loading log: ${err.message}`;
    }
  }
}

function toggleDrawerWrap() {
  logDrawerState.wrap = !logDrawerState.wrap;
  const btn = document.getElementById('drawer-wrap-btn');
  if (btn) btn.classList.toggle('btn-primary', logDrawerState.wrap);
  renderLogDrawerContent();
}

function toggleDrawerAutoScroll() {
  logDrawerState.autoScroll = !logDrawerState.autoScroll;
  const btn = document.getElementById('drawer-autoscroll-btn');
  if (btn) btn.textContent = `Follow: ${logDrawerState.autoScroll ? 'ON' : 'OFF'}`;
  renderLogDrawerContent();
}

function copyDrawerLog() {
  navigator.clipboard.writeText(logDrawerState.rawText || '')
    .then(() => showToast({ title: 'Log copied to clipboard', type: 'success', duration: 2500 }))
    .catch(() => showToast({ title: 'Unable to copy log', type: 'error' }));
}

// ================================================================
// Auto-Refresh Poller with Visibility Pause & Topbar Indicator
// ================================================================

function createAutoPoller(fetchFn, { intervalMs = 5000, fastIntervalMs = 2500, isFast = () => false } = {}) {
  let timer = null;
  let paused = false;
  let inFlight = false;
  let destroyed = false;

  const pill = document.getElementById('live-status-pill');
  const dot = document.getElementById('live-status-dot');
  const text = document.getElementById('live-status-text');

  function updatePill(syncing) {
    if (!dot || !text) return;
    dot.classList.toggle('paused', paused);
    dot.classList.toggle('syncing', syncing && !paused);
    text.textContent = paused ? 'Paused' : (syncing ? 'Syncing…' : 'Live');
  }

  async function runNow(manual = false) {
    if (inFlight || destroyed) return;
    if (paused && !manual) return;
    inFlight = true;
    updatePill(true);
    try {
      await fetchFn();
    } catch (err) {
      console.error('Auto-refresh error:', err);
    } finally {
      inFlight = false;
      updatePill(false);
      scheduleNext();
    }
  }

  function scheduleNext() {
    if (timer) clearTimeout(timer);
    if (paused || destroyed || document.hidden) return;
    const delay = isFast() ? fastIntervalMs : intervalMs;
    timer = setTimeout(() => runNow(false), delay);
  }

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && !paused && !destroyed) {
      runNow(false);
    }
  });

  if (pill) {
    pill.style.display = 'inline-flex';
    pill.onclick = () => {
      paused = !paused;
      updatePill(false);
      if (!paused) runNow(true);
    };
    pill.title = 'Click to pause/resume live updates';
  }

  const refreshBtn = document.getElementById('topbar-refresh-btn');
  if (refreshBtn) {
    refreshBtn.style.display = 'inline-flex';
    refreshBtn.onclick = () => runNow(true);
  }

  updatePill(false);
  scheduleNext();

  return {
    refresh: () => runNow(true),
    stop: () => {
      destroyed = true;
      if (timer) clearTimeout(timer);
    }
  };
}

// --- Global DOM Wiring ---
document.addEventListener('DOMContentLoaded', () => {
  updateThemeButtonUI();

  // Background stat check for sidebar running badge (if page doesn't update it itself)
  apiJson('/api/stats').then(s => {
    if (s) updateSidebarRunningBadge(s.running);
  }).catch(() => {});

  const modal = document.getElementById('confirm-modal');
  const cancelBtn = document.getElementById('modal-cancel');
  const confirmBtn = document.getElementById('modal-confirm');

  if (cancelBtn) cancelBtn.onclick = hideConfirmModal;
  if (modal) {
    modal.onclick = (e) => {
      if (e.target === modal) hideConfirmModal();
    };
  }
  if (confirmBtn) {
    confirmBtn.onclick = async () => {
      const fn = confirmState.onConfirm;
      hideConfirmModal();
      if (fn) await fn();
    };
  }

  const drawerSearch = document.getElementById('drawer-log-search');
  if (drawerSearch) {
    drawerSearch.addEventListener('input', (e) => {
      logDrawerState.search = e.target.value;
      renderLogDrawerContent();
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (modal && modal.classList.contains('open')) {
        hideConfirmModal();
      } else if (logDrawerState.execId !== null) {
        closeLogDrawer();
      } else {
        toggleSidebar(false);
      }
    }
  });
});

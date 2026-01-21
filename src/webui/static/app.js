// Theme switcher
let theme = localStorage.getItem('theme') || 'light';
document.documentElement.setAttribute('data-theme', theme);

function toggleTheme() {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'light' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
}

// Add theme toggle button to navbar
document.addEventListener('DOMContentLoaded', () => {
    const nav = document.querySelector('.navbar .container');
    if (nav) {
        const themeBtn = document.createElement('button');
        themeBtn.className = 'theme-toggle';
        themeBtn.innerHTML = '🌙';
        themeBtn.onclick = toggleTheme;
        nav.appendChild(themeBtn);
    }
});

// API helper with auth
async function apiCall(url, options = {}) {
    const token = localStorage.getItem('auth_token');
    if (token) {
        options.headers = options.headers || {};
        options.headers['Authorization'] = `Bearer ${token}`;
    }
    
    const response = await fetch(url, options);
    
    if (response.status === 401) {
        window.location.href = '/login';
        return null;
    }
    
    return response;
}

// SSE log streaming
function streamLog(execId, targetElement) {
    const eventSource = new EventSource(`/api/executions/${execId}/log/stream`);
    
    eventSource.onmessage = (event) => {
        if (targetElement) {
            targetElement.textContent += event.data;
            if (window.autoScroll) {
                targetElement.scrollTop = targetElement.scrollHeight;
            }
        }
    };
    
    eventSource.onerror = () => {
        eventSource.close();
    };
    
    return eventSource;
}

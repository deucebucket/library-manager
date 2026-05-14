/**
 * Common utility functions shared across all templates.
 * Loaded via base.html before page-specific scripts.
 */

/**
 * Escape HTML entities to prevent XSS.
 * @param {string} text - Raw text to escape
 * @returns {string} HTML-safe string
 */
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    var div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

/**
 * Show a toast notification.
 * @param {string} message - Message to display
 * @param {string} [type='info'] - One of: success, danger, info, warning
 */
function showToast(message, type) {
    type = type || 'info';
    var colors = {
        success: 'var(--theme-accent-success)',
        danger: 'var(--theme-accent-primary)',
        info: 'var(--theme-accent-secondary)',
        warning: 'var(--theme-accent-warning)'
    };
    var toast = document.createElement('div');
    toast.className = 'toast-notification';
    toast.style.background = colors[type] || colors.info;
    toast.textContent = message;
    document.body.appendChild(toast);
    requestAnimationFrame(function() { toast.style.opacity = '1'; });
    setTimeout(function() {
        toast.style.opacity = '0';
        setTimeout(function() { toast.remove(); }, 300);
    }, 3000);
}

// Reload every 30 seconds
setInterval(function () {
    location.reload();
}, 30000);

document.addEventListener('keydown', function (e) {
    if (e.key === 'F11') {
        // Toggle fullscreen is usually browser handled, but we can help layout
        document.body.classList.toggle('fullscreen');
    }
});

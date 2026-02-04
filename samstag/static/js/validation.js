document.addEventListener('DOMContentLoaded', function () {
    var scoreInputs = document.querySelectorAll('input[type="number"]');

    scoreInputs.forEach(function (input) {
        input.addEventListener('change', function () {
            validateScore(this);
        });
    });
});

function validateScore(input) {
    var val = parseInt(input.value);
    if (val > 42) {
        alert("Maximale Punktzahl ist 42!");
        input.value = 42;
        input.classList.add('is-invalid');
    } else if (val < 0) {
        alert("Punktzahl darf nicht negativ sein!");
        input.value = 0;
        input.classList.add('is-invalid');
    } else {
        input.classList.remove('is-invalid');
    }
}

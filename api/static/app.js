let currentStep = 1
const totalSteps = 4

function showStep(step) {
    document.querySelectorAll(".form-step").forEach(s => s.classList.remove("active"));
    document.getElementById(`step-${step}`).classList.add("active");
    document.getElementById("progressBar").style.width = `${(step / totalSteps) * 100}%`;
}

document.querySelectorAll(".next").forEach(btn =>
    btn.addEventListener("click", () => showStep(++currentStep))
);

document.querySelectorAll(".prev").forEach(btn =>
    btn.addEventListener("click", () => showStep(--currentStep))
);

document.getElementById("rentForm").addEventListener("submit", async (e) => {
    e.preventDefault();

    const data = {
        zipCode: +zipCode.value,
        bedrooms: +bedrooms.value,
        bathrooms: +bathrooms.value,
        rent: +rent.value,
        squareFootage: squareFootage.value ? +squareFootage.value : null,
        yearBuilt: yearBuilt.value ? +yearBuilt.value : null
    };

    console.log(data)

    const req = await fetch("/predict", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data)
    });

    const response = await req.json();
    renderResults(response);
    showStep(4);
});

function renderResults(response) {
    const e = document.getElementById("resultsContent");
    e.innerHTML = "";

    Object.entries(response).forEach(([window, r]) => {
        const needleId = `needle-${window}`;
        
        e.innerHTML += `
            <div class="card mb-3">
                <div class="card-body">
                    <h5>${window}-day Market</h5>
                    <p>We believe this listing is <strong>${r.classification}</strong></p>
                    <p>It seems to be ${Math.abs(r.percent_difference)}% ${r.percent_difference >= 0 ? "above" : "below"} our market prediction</p>

                    <div class="gauge-container">
                        <div class="gauge-bar">
                            <div class="zone under"></div>
                            <div class="zone fair"></div>
                            <div class="zone over"></div>
                        </div>
                        <div class="needle" id="${needleId}"></div>
                        <div class="gauge-labels">
                            <span>-50%</span>
                            <span>0%</span>
                            <span>+50%</span>
                        </div>
                    </div>
                </div>
            </div>
        `;

        setTimeout(() => {
            positionNeedle(r.percent_difference, needleId);
        }, 50)
    });
}

function positionNeedle(percent, needleId) {
    const clamped = Math.max(-30, Math.min(30, percent));
    const normalized = (clamped + 30) / 60 * 100;
    const needle = document.getElementById(needleId);
    needle.style.left = `${normalized}%`;
}

function resetForm() {
    document.getElementById("rentForm").reset();
    currentStep = 1;
    showStep(1);
}
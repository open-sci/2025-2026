function updateSunburst() {
    // Get active institution button
    const instActive = document.querySelector(".sunburst-inst-btn.active");
    // Get active direction button
    const dirActive = document.querySelector(".sunburst-dir-btn.active");
    const chartDiv = document.getElementById("sunburst-chartdiv");

    if (!instActive || !dirActive || !chartDiv) {
        console.error("Sunburst elements not found!");
        return;
    }

    const inst = instActive.dataset.inst;
    const dir = dirActive.dataset.dir;
    const src = `visualizations/sunburst/sunburst_${inst}_${dir}.html`;

    console.log("Loading sunburst iframe:", src);

    chartDiv.innerHTML = `<iframe src="${src}" width="100%" height="750px" frameborder="0" scrolling="no" style="border-radius: 12px; background-color: transparent; overflow: hidden;"></iframe>`;
}

window.addEventListener('load', function () {
    console.log("Initializing Sunburst");

    const instBtns = document.querySelectorAll(".sunburst-inst-btn");
    const dirBtns = document.querySelectorAll(".sunburst-dir-btn");

    if (instBtns.length > 0 && dirBtns.length > 0) {
        instBtns.forEach(btn => {
            btn.addEventListener("click", function () {
                instBtns.forEach(b => b.classList.remove("active"));
                this.classList.add("active");
                updateSunburst();
            });
        });

        dirBtns.forEach(btn => {
            btn.addEventListener("click", function () {
                dirBtns.forEach(b => b.classList.remove("active"));
                this.classList.add("active");
                updateSunburst();
            });
        });

        // Initial load
        updateSunburst();
    } else {
        console.error("Could not find sunburst button elements on load");
    }
});

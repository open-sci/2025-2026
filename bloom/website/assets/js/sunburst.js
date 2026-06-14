function updateSunburst() {
    const instSelect = document.getElementById("sunburst-inst-select");
    const dirSelect = document.getElementById("sunburst-dir-select");
    const chartDiv = document.getElementById("sunburst-chartdiv");

    if (!instSelect || !dirSelect || !chartDiv) {
        console.error("Sunburst elements not found!");
        return;
    }

    const inst = instSelect.value;
    const dir = dirSelect.value;
    const src = `visualizations/sunburst/sunburst_${inst}_${dir}.html`;
    
    console.log("Loading sunburst iframe:", src);

    chartDiv.innerHTML = `<iframe src="${src}" width="100%" height="700px" frameborder="0" style="border-radius: 12px; background-color: #2A0C32;"></iframe>`;
}

window.addEventListener('load', function() {
    console.log("Initializing Sunburst");
    const instSelect = document.getElementById("sunburst-inst-select");
    const dirSelect = document.getElementById("sunburst-dir-select");
    
    if (instSelect && dirSelect) {
        instSelect.addEventListener("change", updateSunburst);
        dirSelect.addEventListener("change", updateSunburst);
        
        // Initial load
        updateSunburst();
    } else {
        console.error("Could not find sunburst select elements on load");
    }
});

am5.ready(function () {
    console.log("[bubble] am5.ready fired");

    var el = document.getElementById("bubble-chartdiv");
    if (!el) {
        console.error("[bubble] #bubble-chartdiv not found in DOM");
        return;
    }
    console.log("[bubble] #bubble-chartdiv found, dimensions:", el.offsetWidth, "x", el.offsetHeight);

    var initialized = false;
    var bubbleDataReady = false;
    var globalBubbleData = null;

    const institutions = ["UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO", "SNS"];

    // ── Data loading ──────────────────────────────────────────────────────────

    function fetchCSV(url) {
        return new Promise((resolve, reject) => {
            Papa.parse(url, {
                download: true,
                header: true,
                skipEmptyLines: true,
                complete: resolve,
                error: reject
            });
        });
    }

    async function loadBubbleData() {
        try {
            let globalStats = {};
            let top12PerInst = {};

            for (let inst of institutions) {
                let inPath = `visualizations/data/${inst}/citation_counts_countries_incoming_clean.csv`;
                let outPath = `visualizations/data/${inst}/citation_counts_countries_outgoing_clean.csv`;

                let [inRes, outRes] = await Promise.all([fetchCSV(inPath), fetchCSV(outPath)]);

                let instTotals = {};

                let processRow = (row) => {
                    if (!row.country_code || !row.count) return;
                    let count = parseInt(row.count);
                    let code = row.country_code;
                    let name = row.country_name;

                    if (!instTotals[code]) instTotals[code] = { name, count: 0 };
                    instTotals[code].count += count;

                    if (!globalStats[code]) globalStats[code] = { name, total: 0, instCounts: {} };
                    globalStats[code].name = name;
                    globalStats[code].total += count;
                    if (!globalStats[code].instCounts[inst]) globalStats[code].instCounts[inst] = 0;
                    globalStats[code].instCounts[inst] += count;
                };

                inRes.data.forEach(processRow);
                outRes.data.forEach(processRow);

                let sortedCodes = Object.keys(instTotals)
                    .sort((a, b) => instTotals[b].count - instTotals[a].count);
                top12PerInst[inst] = new Set(sortedCodes.slice(0, 12));
            }

            let displayCountries = [];
            for (let code in globalStats) {
                let inTop12Count = 0;
                let connectedInsts = [];
                for (let inst of institutions) {
                    if (top12PerInst[inst] && top12PerInst[inst].has(code)) {
                        inTop12Count++;
                        connectedInsts.push(inst);
                    }
                }
                if (inTop12Count > 0) {
                    displayCountries.push({
                        code,
                        name: globalStats[code].name,
                        value: globalStats[code].total,
                        universal: inTop12Count === 6,
                        inTop12Count,
                        connectedInsts,
                        instCounts: globalStats[code].instCounts
                    });
                }
            }

            displayCountries.sort((a, b) => b.value - a.value);

            // Data includes all children so amCharts can natively manage expand/collapse
            globalBubbleData = {
                name: "Italian Science",
                value: 0,
                children: displayCountries.map(c => ({
                    name: c.name,
                    value: c.value,
                    universal: c.universal,
                    details: c.universal
                        ? `All 6 institutions`
                        : `${c.inTop12Count} of 6 institutions`,
                    children: c.connectedInsts.map(inst => ({
                        name: inst,
                        value: c.instCounts[inst]
                    }))
                }))
            };

            bubbleDataReady = true;
            console.log("[bubble] Dynamic data loaded successfully");

        } catch (error) {
            console.error("[bubble] Error loading data:", error);
        }
    }

    // ── Chart init ────────────────────────────────────────────────────────────

    function initBubbleChart() {
        if (initialized) return;
        initialized = true;

        var el = document.getElementById("bubble-chartdiv");
        console.log("[bubble] initBubbleChart called, dimensions:", el.offsetWidth, "x", el.offsetHeight);

        var root = am5.Root.new("bubble-chartdiv");
        root.setThemes([am5themes_Animated.new(root)]);
        console.log("[bubble] root created");

        var container = root.container.children.push(
            am5.Container.new(root, {
                width: am5.percent(100),
                height: am5.percent(100),
                layout: root.verticalLayout
            })
        );
        console.log("[bubble] container created");

        var series = container.children.push(
            am5hierarchy.ForceDirected.new(root, {
                singleBranchOnly: false,
                topDepth: 1,
                initialDepth: 0,
                downDepth: 1,
                valueField: "value",
                categoryField: "name",
                childDataField: "children",
                centerStrength: 0.8,
                manyBodyStrength: -15,
                nodePadding: 10,
                minRadius: 15,
                maxRadius: 60
            })
        );
        console.log("[bubble] series created");

        series.labels.template.setAll({
            fontSize: 12,
            fontWeight: "500",
            fill: am5.color(0xFFFFFF),
            text: "{name}",
            oversizedBehavior: "fit"
        });

        series.circles.template.adapters.add("fill", function (fill, target) {
            if (!target.dataItem) return fill;
            var depth = target.dataItem.get("depth");
            var ctx = target.dataItem.dataContext;
            if (depth === 1) {
                return ctx && ctx.universal === true
                    ? am5.color(0xB7990D)
                    : am5.color(0x4a3070);
            }
            if (depth === 2) {
                return am5.color(0x6B3E7A);
            }
            return fill;
        });

        series.circles.template.setAll({
            strokeOpacity: 0.2,
            stroke: am5.color(0x320E3B),
            strokeWidth: 1
        });

        series.outerCircles.template.setAll({
            strokeOpacity: 0.15,
            stroke: am5.color(0x320E3B),
            strokeWidth: 1,
            fillOpacity: 0.04,
            fill: am5.color(0x320E3B)
        });

        series.links.template.setAll({
            strokeOpacity: 0.15,
            strokeWidth: 1,
            stroke: am5.color(0x888888)
        });

        series.nodes.template.setAll({
            tooltipText: "[bold]{name}[/]\n{details}",
            cursorOverStyle: "pointer"
        });

        series.data.setAll([globalBubbleData]);
        series.appear(1000, 100);
        console.log("[bubble] data set, calling appear()");
    }

    // ── Init trigger logic ────────────────────────────────────────────────────

    function tryInitBubbleChart() {
        if (initialized) return;
        var el = document.getElementById("bubble-chartdiv");
        if (el && el.offsetWidth > 10 && bubbleDataReady) {
            initBubbleChart();
        } else {
            console.log("[bubble] container or data not ready (width:", el ? el.offsetWidth : "null", ", data:", bubbleDataReady, ") — retrying in 50ms");
            setTimeout(tryInitBubbleChart, 50);
        }
    }

    // Start loading data immediately
    loadBubbleData();

    // Walk up to find the .rq-section ancestor
    var parentSection = el.closest(".rq-section");
    console.log("[bubble] parentSection:", parentSection ? parentSection.id : "NOT FOUND");

    if (!parentSection) {
        console.log("[bubble] no rq-section parent — trying to init");
        tryInitBubbleChart();
        return;
    }

    console.log("[bubble] parentSection classes:", parentSection.className);

    if (parentSection.classList.contains("active")) {
        console.log("[bubble] section already active — trying to init");
        tryInitBubbleChart();
        return;
    }

    console.log("[bubble] section not active — setting up MutationObserver");

    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (mutation) {
            if (mutation.attributeName === "class" &&
                parentSection.classList.contains("active") &&
                !initialized) {
                console.log("[bubble] section became active — trying to init");
                tryInitBubbleChart();
            }
        });
    });

    observer.observe(parentSection, { attributes: true });
    console.log("[bubble] MutationObserver attached to:", parentSection.id);
});
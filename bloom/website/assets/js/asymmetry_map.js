am5.ready(function () {

    // ==============================================================================
    // 1. INITIALIZATION
    // ==============================================================================
    var root = am5.Root.new("asymmetry_chartdiv");
    root.setThemes([am5themes_Animated.new(root)]);

    // ==============================================================================
    // 2. MAP CHART SETUP
    // ==============================================================================
    var chart = root.container.children.push(
        am5map.MapChart.new(root, {
            panX: "rotateX",
            panY: "translateY",
            projection: am5map.geoNaturalEarth1()
        })
    );

    // ==============================================================================
    // 3. POLYGON SERIES (countries)
    // ==============================================================================
    var polygonSeries = chart.series.push(
        am5map.MapPolygonSeries.new(root, {
            geoJSON: am5geodata_worldLow,
            exclude: ["AQ"],
            valueField: "value",
            calculateAggregates: true
        })
    );

    polygonSeries.mapPolygons.template.setAll({
        interactive: true,
        fill: am5.color(0xE8E8E8),
        stroke: am5.color(0xffffff),
        strokeWidth: 0.5
        // NOTE: do NOT set tooltipText here — the adapter owns it
    });

    polygonSeries.mapPolygons.template.states.create("hover", {
        stroke: am5.color(0x23022E),
        strokeWidth: 1,
        fillOpacity: 0.7
    });

    // Attach the tooltip to the SERIES, not the template polygon.
    // When attached to the series, amCharts passes the correct hovered
    // dataItem context and the adapter fires reliably.
    var tooltip = am5.Tooltip.new(root, {
        getFillFromSprite: false,
        autoTextColor: false
    });
    tooltip.get("background").setAll({
        fill: am5.color(0xffffff),
        stroke: am5.color(0xcccccc),
        strokeWidth: 1
    });
    tooltip.label.setAll({
        fill: am5.color(0x222222),
        fontSize: 12,
        lineHeight: 1.6
    });

    // Set tooltip on the series (not the template) — this is the key fix.
    // Then set tooltipText on the template so each polygon triggers it.
    polygonSeries.set("tooltip", tooltip);
    polygonSeries.mapPolygons.template.set("tooltipText", "x"); // non-empty to activate hover

    polygonSeries.mapPolygons.template.adapters.add("tooltipText", function (text, target) {
        var di = target.dataItem;
        if (!di) return "";

        // After data.setAll(), your JSON fields are merged into dataContext.
        // geodata name lives at dataContext.name; your custom fields do too
        // once the merge happens (amCharts merges by matching the "id" field).
        var d = di.dataContext || {};
        var countryName = d.name || d.NAME || di.get("id") || "Unknown";
        var value = di.get("value"); // valueField is always read via di.get()

        if (value === undefined || value === null) {
            return "[bold]" + countryName + "[/]\nInsufficient data (< 500 citations)";
        }

        var sign      = value >= 0 ? "+" : "";
        var direction = value > 0.1
            ? "Knowledge provider"
            : value < -0.1
                ? "Knowledge consumer"
                : "Balanced";

        return (
            "[bold]" + countryName + "[/]\n" +
            "Incoming: " + (d.incoming !== undefined ? d.incoming.toLocaleString() : "—") + "\n" +
            "Outgoing: " + (d.outgoing !== undefined ? d.outgoing.toLocaleString() : "—") + "\n" +
            "Total: "    + (d.total    !== undefined ? d.total.toLocaleString()    : "—") + "\n" +
            "log₂(out/in): " + sign + value.toFixed(2) + "\n" +
            "[italic]" + direction + "[/]"
        );
    });

    // ==============================================================================
    // 4. COLOR SCALE
    // Mirrors your Plotly colorscale:
    // -3 → #B7990D (strong incoming/yellow), 0 → #F5F0E8 (balanced/cream), +3 → #23022E (strong outgoing/purple)
    // ==============================================================================
    polygonSeries.set("heatRules", [{
        target: polygonSeries.mapPolygons.template,
        dataField: "value",
        customFunction: function (sprite, min, max, value) {
            if (value === undefined || value === null) {
                sprite.set("fill", am5.color(0xE8E8E8)); // grey = no data
                return;
            }

            // Clamp to [-3, +3]
            var clamped = Math.max(-3, Math.min(3, value));
            var t = (clamped + 3) / 6; // normalize to [0, 1]

            // Color stops matching Plotly scale
            var stops = [
                { t: 0.0, r: 0xB7, g: 0x99, b: 0x0D }, // #B7990D
                { t: 0.25, r: 0xD4, g: 0xBC, b: 0x5E }, // midpoint between #B7990D and #F5F0E8
                { t: 0.5, r: 0xF5, g: 0xF0, b: 0xE8 }, // #F5F0E8
                { t: 0.75, r: 0x6B, g: 0x3E, b: 0x7A }, // #6B3E7A
                { t: 1.0, r: 0x23, g: 0x02, b: 0x2E }  // #23022E
            ];

            // Find the two stops to interpolate between
            var lower = stops[0], upper = stops[stops.length - 1];
            for (var i = 0; i < stops.length - 1; i++) {
                if (t >= stops[i].t && t <= stops[i + 1].t) {
                    lower = stops[i];
                    upper = stops[i + 1];
                    break;
                }
            }

            var range = upper.t - lower.t || 1;
            var ratio = (t - lower.t) / range;

            var r = Math.round(lower.r + (upper.r - lower.r) * ratio);
            var g = Math.round(lower.g + (upper.g - lower.g) * ratio);
            var b = Math.round(lower.b + (upper.b - lower.b) * ratio);

            sprite.set("fill", am5.color((r << 16) | (g << 8) | b));
        }
    }]);

    // ==============================================================================
    // 5. LEGEND
    // ==============================================================================
    var legendContainer = chart.children.push(am5.Container.new(root, {
        layout: root.verticalLayout,
        width: 220,
        paddingLeft: 20,
        x: 0,
        y: am5.p50,
        centerY: am5.p50
    }));

    legendContainer.children.push(am5.Label.new(root, {
        text: "log₂(out/in)",
        fontSize: 12,
        fontWeight: "bold",
        paddingBottom: 8,
        fill: am5.color(0xFFFFFF)
    }));

    var legendWrapper = legendContainer.children.push(am5.Container.new(root, {
        layout: root.horizontalLayout,
        width: am5.p100
    }));

    var labelsCol = legendWrapper.children.push(am5.Container.new(root, {
        width: 120,
        height: 200,
        paddingRight: 10
    }));

    var barsCol = legendWrapper.children.push(am5.Container.new(root, {
        layout: root.verticalLayout,
        width: 20,
        height: 200
    }));

    // Top half: White to Purple (0 to 3)
    var topLegend = barsCol.children.push(am5.HeatLegend.new(root, {
        orientation: "vertical",
        startColor: am5.color(0xF5F0E8),
        endColor: am5.color(0x23022E),
        startValue: 0,
        endValue: 3,
        stepCount: 100,
        height: 100
    }));
    topLegend.startLabel.set("forceHidden", true);
    topLegend.endLabel.set("forceHidden", true);

    // Bottom half: Yellow to White (-3 to 0)
    var bottomLegend = barsCol.children.push(am5.HeatLegend.new(root, {
        orientation: "vertical",
        startColor: am5.color(0xFFD500),
        endColor: am5.color(0xF5F0E8),
        startValue: -3,
        endValue: 0,
        stepCount: 100,
        height: 100
    }));
    bottomLegend.startLabel.set("forceHidden", true);
    bottomLegend.endLabel.set("forceHidden", true);

    // Custom precise labels
    labelsCol.children.push(am5.Label.new(root, {
        text: "+3\nKnowledge provider",
        fontSize: 10,
        fill: am5.color(0xFFFFFF),
        textAlign: "right",
        x: am5.p100,
        centerX: am5.p100,
        y: 0,
        centerY: 0
    }));

    labelsCol.children.push(am5.Label.new(root, {
        text: "0\nBalanced",
        fontSize: 10,
        fill: am5.color(0xFFFFFF),
        textAlign: "right",
        x: am5.p100,
        centerX: am5.p100,
        y: 100,
        centerY: am5.p50
    }));

    labelsCol.children.push(am5.Label.new(root, {
        text: "−3\nKnowledge consumer",
        fontSize: 10,
        fill: am5.color(0xFFFFFF),
        textAlign: "right",
        x: am5.p100,
        centerX: am5.p100,
        y: 200,
        centerY: am5.p100
    }));

    // Show value on legend on hover
    polygonSeries.mapPolygons.template.events.on("pointerover", function (ev) {
        var di = ev.target.dataItem;
        if (!di) return;
        var val = di.get("value");  // use di.get() not di.dataContext.value
        if (val === undefined || val === null) return;
        if (val > 0) {
            topLegend.showValue(val);
        } else {
            bottomLegend.showValue(val);
        }
    });

    polygonSeries.mapPolygons.template.events.on("pointerout", function (ev) {
        topLegend.hideTooltip();
        bottomLegend.hideTooltip();
    });

    // ==============================================================================
    // 6. TITLE
    // ==============================================================================
    var titleLabel = chart.children.unshift(am5.Label.new(root, {
        text: "",
        fontSize: 16,
        fontWeight: "bold",
        fontFamily: "Playfair Display, serif",
        fill: am5.color(0xB7990D),
        x: am5.p50,
        centerX: am5.p50,
        y: 20,
        paddingBottom: 8,
        paddingTop: 4
    }));

    // ==============================================================================
    // 7. DATA LOADING & SWITCHING
    // ==============================================================================
    // Your Python export should produce one JSON per institution, e.g.:
    // asymmetry_UNIBO.json, asymmetry_UNIMI.json, ...
    //
    // Each JSON should be an array of objects like:
    // [
    //   {
    //     "id": "USA",                  // ISO 3166-1 alpha-3, must match am5geodata IDs
    //     "name": "United States",
    //     "value": 1.23,               // log2_ratio (null if below threshold)
    //     "incoming": 4521,
    //     "outgoing": 9103,
    //     "total": 13624
    //   }, ...
    // ]
    //
    // Countries below the 500-citation threshold should have value: null
    // so they render as the default grey.

    var INSTITUTIONS = ["UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO", "SNS"];
    var LABELS = {
        UNIBO: "University of Bologna",
        UNIMI: "University of Milan",
        UNIPD: "University of Padua",
        UNITO: "University of Turin",
        UPO: "University of Eastern Piedmont",
        SNS: "Scuola Normale Superiore"
    };

    var cache = {};

    function loadAndRender(inst) {
        var label = LABELS[inst] || inst;
        titleLabel.set("text", "Citation Asymmetry — " + label + " (log₂ outgoing / incoming)");

        if (cache[inst]) {
            polygonSeries.data.setAll(cache[inst]);
            return;
        }

        fetch("visualizations/asymmetry/asymmetry_" + inst + ".json")
            .then(function (r) { return r.json(); })
            .then(function (data) {
                // Map alpha-3 IDs to alpha-2 IDs so amCharts can link the data
                var nameToAlpha2 = {};
                if (am5geodata_worldLow && am5geodata_worldLow.features) {
                    am5geodata_worldLow.features.forEach(function (f) {
                        if (f.properties && f.properties.name && f.properties.id) {
                            nameToAlpha2[f.properties.name.toLowerCase()] = f.properties.id;
                        }
                    });
                }

                // Manual overrides for name mismatches
                nameToAlpha2["the netherlands"] = "NL";
                nameToAlpha2["united states"] = "US";
                nameToAlpha2["russia"] = "RU";
                nameToAlpha2["south korea"] = "KR";
                nameToAlpha2["czechia"] = "CZ";
                nameToAlpha2["macao"] = "MO";

                data.forEach(function (d) {
                    var lowerName = d.name.toLowerCase();
                    if (nameToAlpha2[lowerName]) {
                        d.id = nameToAlpha2[lowerName];
                    }
                });

                cache[inst] = data;
                polygonSeries.data.setAll(data);
            })
            .catch(function (err) {
                console.error("Failed to load data for " + inst, err);
            });
    }

    // Bind buttons
    document.querySelectorAll(".asymmetry-inst-btn").forEach(function (btn) {
        btn.addEventListener("click", function (e) {
            document.querySelectorAll(".asymmetry-inst-btn").forEach(function (b) {
                b.classList.remove("active");
            });
            e.target.classList.add("active");
            var inst = e.target.getAttribute("data-inst");
            loadAndRender(inst);
        });
    });

    // Initial load
    loadAndRender("UNIBO");
    chart.appear(1000);

});
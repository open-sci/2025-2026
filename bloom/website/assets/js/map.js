am5.ready(function () {

    // ── Color System ──────────────────────────────────────────────────────────
    var C = {
        ocean: 0x0d1b2a,  // deep navy background
        land: 0x2d2040,  // default country fill
        landStroke: 0x4a3560,  // default country border
        landIncoming: 0x4a3070,  // active country — incoming side (lit purple)
        landIncomingStroke: 0x7a5aaa,
        landOutgoing: 0x4a1a40,  // active country — outgoing side (dark magenta)
        landOutgoingStroke: 0x8a3070,
        italy: 0xB7990D,  // gold — Italy node fill
        italyRing: 0xffe066,  // gold ring around Italy
        incoming: 0xB7990D,  // gold — incoming flow bands & bullets
        outgoing: 0xc084fc,  // lilac — outgoing flow bands & bullets
        graticule: 0xffffff,  // white at low opacity
    };

    // ── 1. Core Setup ─────────────────────────────────────────────────────────
    var root = am5.Root.new("chartdiv");
    root.setThemes([am5themes_Animated.new(root)]);

    var chart = root.container.children.push(
        am5map.MapChart.new(root, {
            panX: "translateX",
            panY: "translateY",
            projection: am5map.geoMercator()
        })
    );

    // Zoom controls
    var zoomControl = chart.set("zoomControl", am5map.ZoomControl.new(root, {}));
    zoomControl.homeButton.set("visible", true);
    zoomControl.plusButton.get("background").set("fill", am5.color(C.italy));
    zoomControl.minusButton.get("background").set("fill", am5.color(C.italy));
    zoomControl.homeButton.get("background").set("fill", am5.color(C.italy));

    // ── 2. Base Map ───────────────────────────────────────────────────────────
    chart.chartContainer.set("background", am5.Rectangle.new(root, {
        fill: am5.color(C.ocean),
        fillOpacity: 1
    }));

    // Graticule grid
    var graticuleSeries = chart.series.push(am5map.GraticuleSeries.new(root, {}));
    graticuleSeries.mapLines.template.setAll({
        stroke: am5.color(C.graticule),
        strokeOpacity: 0.07,
        strokeWidth: 0.5
    });

    var polygonSeries = chart.series.push(
        am5map.MapPolygonSeries.new(root, {
            geoJSON: am5geodata_worldLow,
            exclude: ["AQ"]
        })
    );

    polygonSeries.mapPolygons.template.setAll({
        fill: am5.color(C.land),
        stroke: am5.color(C.landStroke),
        strokeWidth: 0.5,
        // Not interactive — highlightSeries sits on top and owns all pointer events
        interactive: false
    });

    // ── 3. Country Highlight Series ───────────────────────────────────────────
    // Pushed HERE — before lineSeries — so lines and bullets render on top of it.
    // This series owns ALL mouse interaction: it covers the full world map and
    // is the only layer that receives pointer events for country hover/tooltip.
    var highlightSeries = chart.series.push(
        am5map.MapPolygonSeries.new(root, {
            geoJSON: am5geodata_worldLow,
            exclude: ["AQ"]
        })
    );

    // Attach tooltip to the series so {name} resolves from geodata
    highlightSeries.set("tooltip", am5.Tooltip.new(root, {
        labelText: "{name}"
    }));

    highlightSeries.mapPolygons.template.setAll({
        // Fully transparent by default — polygonSeries shows through underneath
        fill: am5.color(0x000000),
        fillOpacity: 0,
        strokeOpacity: 0,
        interactive: true,
        cursorOverStyle: "pointer",
        tooltipText: "{name}"
    });

    // Hover: slight brightening on all countries (active ones already have fill set)
    highlightSeries.mapPolygons.template.states.create("hover", {
        fillOpacity: 0.15,
        strokeOpacity: 0.4,
        stroke: am5.color(0xffffff),
        strokeWidth: 0.8
    });

    // ── 4. Flow Lines ─────────────────────────────────────────────────────────
    var lineSeries = chart.series.push(am5map.MapLineSeries.new(root, {}));

    lineSeries.mapLines.template.setAll({
        strokeWidth: 1.8,
        strokeOpacity: 0.5,
        tooltipText: "{tooltipText}"
    });

    // Arrow bullets — direction indicator
    var flowSeries = chart.series.push(am5map.MapPointSeries.new(root, {}));

    flowSeries.bullets.push(function (root, series, dataItem) {
        var arrowColor = dataItem.dataContext && dataItem.dataContext.customColor
            ? dataItem.dataContext.customColor
            : am5.color(C.incoming);

        var arrow = am5.Graphics.new(root, {
            svgPath: "M-5,-3 L7,0 L-5,3 Z",
            fill: arrowColor,
            fillOpacity: 0.9
        });

        return am5.Bullet.new(root, {
            sprite: arrow,
            autoRotate: true
        });
    });

    // ── 5. City / Node Circles ─────────────────────────────────────────────────
    // The official amCharts 5 pattern for per-point sized bullets on a map is:
    //   1. Declare valueField + calculateAggregates: true on the series
    //   2. Create a named Template for the sprite
    //   3. Reference it via templateField in each data item
    //   4. Use heatRules to drive the radius from the value field
    // This is the only approach that actually works — adapters fire too early.

    // Shared circle template for country nodes
    var circleTemplate = am5.Template.new({});

    var citySeries = chart.series.push(am5map.MapPointSeries.new(root, {
        calculateAggregates: true,
        valueField: "value"
    }));

    citySeries.bullets.push(function () {
        return am5.Bullet.new(root, {
            sprite: am5.Circle.new(root, {
                radius: 4,
                fill: am5.color(C.incoming), // Fixed: Nodes will now be Gold
                fillOpacity: 0.85,
                stroke: am5.color(0xffffff),
                strokeWidth: 0.8,
                strokeOpacity: 0.4,
                tooltipText: "{tooltipText}",
                templateField: "circleTemplate"
            }, circleTemplate)
        });
    });

    // heatRules scales radius from min→max based on the value field.
    // Using a log scale: we store Math.log10(count) as the value so the
    // rule operates on log-space, giving perceptually even sizing.
    citySeries.set("heatRules", [{
        target: circleTemplate,
        min: 3,
        max: 16,
        key: "radius",
        dataField: "value"
    }]);

    // Italy — own series so it always renders on top with distinct styling.
    // Fixed size (not heat-driven) since Italy is always the hub.
    var italySeries = chart.series.push(am5map.MapPointSeries.new(root, {}));

    italySeries.bullets.push(function () {
        var ring = am5.Circle.new(root, {
            radius: 22,
            fill: am5.color(C.italyRing),
            fillOpacity: 0.18,
            strokeOpacity: 0
        });

        var circle = am5.Circle.new(root, {
            radius: 18,
            fill: am5.color(C.italy),
            fillOpacity: 1,
            stroke: am5.color(C.italyRing),
            strokeWidth: 2,
            strokeOpacity: 0.9,
            tooltipText: "{tooltipText}"
        });

        var label = am5.Label.new(root, {
            text: "IT",
            fontSize: 9,
            fontWeight: "700",
            fill: am5.color(0x1a1200),
            centerX: am5.p50,
            centerY: am5.p50
        });

        var g = am5.Container.new(root, { interactiveChildren: false });
        g.children.push(ring);
        g.children.push(circle);
        g.children.push(label);

        return am5.Bullet.new(root, { sprite: g });
    });

    // ── 6. Data Loading ────────────────────────────────────────────────────────
    const DIR_COLORS = {
        incoming: am5.color(C.incoming),
        outgoing: am5.color(C.outgoing)
    };

    const HOME_COUNTRY = "IT";
    let activeTimeouts = [];

    // Track which country codes are active and in which direction, so we can
    // highlight their polygons after data loads.
    let activeCountries = {}; // { "US": "incoming", "DE": "outgoing", ... }

    function applyCountryHighlights() {
        highlightSeries.mapPolygons.each(function (polygon) {
            var dataItem = polygon.dataItem;
            if (!dataItem) return;
            var id = dataItem.get("id");
            if (!id) return;

            if (id === HOME_COUNTRY) {
                // Italy — handled by the city node, keep neutral here
                polygon.setAll({ fillOpacity: 0, strokeOpacity: 0 });
                return;
            }

            var dir = activeCountries[id];
            if (dir === "incoming") {
                polygon.setAll({
                    fill: am5.color(C.landIncoming),
                    stroke: am5.color(C.landIncomingStroke),
                    fillOpacity: 1,
                    strokeOpacity: 1,
                    strokeWidth: 0.8
                });
            } else if (dir === "outgoing") {
                polygon.setAll({
                    fill: am5.color(C.landOutgoing),
                    stroke: am5.color(C.landOutgoingStroke),
                    fillOpacity: 1,
                    strokeOpacity: 1,
                    strokeWidth: 0.8
                });
            } else {
                polygon.setAll({ fillOpacity: 0, strokeOpacity: 0 });
            }
        });
    }

    function loadData(institution, directionFilter) {
        lineSeries.data.clear();
        flowSeries.data.clear();
        citySeries.data.clear();
        italySeries.data.clear();
        activeCountries = {};

        // Reset highlight series
        if (highlightSeries.mapPolygons) {
            highlightSeries.mapPolygons.each(function (p) {
                p.setAll({ fillOpacity: 0, strokeOpacity: 0 });
            });
        }

        activeTimeouts.forEach(t => clearTimeout(t));
        activeTimeouts = [];

        var incomingPath = `visualizations/data/${institution.toUpperCase()}/citation_counts_countries_incoming_clean.csv`;
        var outgoingPath = `visualizations/data/${institution.toUpperCase()}/citation_counts_countries_outgoing_clean.csv`;

        Promise.all([
            fetchCSV(incomingPath),
            fetchCSV(outgoingPath)
        ]).then(function (results) {
            var incomingData = results[0].data
                .filter(row => row.country_code && row.count)
                .sort((a, b) => parseInt(b.count) - parseInt(a.count))
                .slice(0, 40);

            var outgoingData = results[1].data
                .filter(row => row.country_code && row.count)
                .sort((a, b) => parseInt(b.count) - parseInt(a.count))
                .slice(0, 40);

            if (polygonSeries.dataItems.length > 0) {
                processData(incomingData, outgoingData);
            } else {
                polygonSeries.events.once("datavalidated", () => processData(incomingData, outgoingData));
            }

            function processData(inData, outData) {
                var homeLon = 12.5;
                var homeLat = 41.9;

                var countryTotals = {};

                function createFlow(row, isIncoming) {
                    if (!row.country_code || !row.count) return;
                    let count = parseInt(row.count);
                    if (count === 0) return;

                    if (directionFilter === "incoming" && !isIncoming) return;
                    if (directionFilter === "outgoing" && isIncoming) return;

                    // Track direction for country highlight and node color
                    var dir = isIncoming ? "incoming" : "outgoing";
                    activeCountries[row.country_code] = dir;

                    countryTotals[row.country_code] = (countryTotals[row.country_code] || 0) + count;
                    countryTotals[HOME_COUNTRY] = (countryTotals[HOME_COUNTRY] || 0) + count;

                    let otherDataItem = polygonSeries.getDataItemById(row.country_code);
                    if (!otherDataItem) return;

                    let otherLon = otherDataItem.get("visualLongitude");
                    let otherLat = otherDataItem.get("visualLatitude");

                    if (otherLon === undefined || otherLat === undefined) {
                        let mapPoly = otherDataItem.get("mapPolygon");
                        if (mapPoly && mapPoly.visualCentroid) {
                            let centroid = mapPoly.visualCentroid();
                            if (centroid) {
                                otherLon = centroid.longitude;
                                otherLat = centroid.latitude;
                            }
                        }
                    }

                    if (otherLon === undefined || otherLat === undefined) return;

                    let coords = isIncoming
                        ? [[otherLon, otherLat], [homeLon, homeLat]]
                        : [[homeLon, homeLat], [otherLon, otherLat]];

                    let color = isIncoming ? DIR_COLORS.incoming : DIR_COLORS.outgoing;

                    let tooltip = isIncoming
                        ? `${row.country_name} → Italy: ${count.toLocaleString()} citations`
                        : `Italy → ${row.country_name}: ${count.toLocaleString()} citations`;

                    var lineDataItem = lineSeries.pushDataItem({
                        geometry: { type: "LineString", coordinates: coords },
                        tooltipText: tooltip
                    });

                    // Tint the line
                    lineDataItem.get("mapLine").set("stroke", color);

                    if (count > 100) {
                        var flowDataItem = flowSeries.pushDataItem({
                            lineDataItem: lineDataItem,
                            positionOnLine: 0.0,
                            autoRotate: true,
                            customColor: color
                        });

                        function loopCitationFlow() {
                            var duration = 3000 + Math.random() * 2000;
                            flowDataItem.animate({
                                key: "positionOnLine",
                                from: 0,
                                to: 1,
                                duration: duration
                            });
                            var t = setTimeout(loopCitationFlow, duration + 1000 + Math.random() * 2000);
                            activeTimeouts.push(t);
                        }
                        var t2 = setTimeout(loopCitationFlow, Math.random() * 2000);
                        activeTimeouts.push(t2);
                    }
                }

                inData.forEach(row => createFlow(row, true));
                outData.forEach(row => createFlow(row, false));

                // Apply country polygon highlights
                if (highlightSeries.dataItems.length > 0) {
                    applyCountryHighlights();
                } else {
                    highlightSeries.events.once("datavalidated", applyCountryHighlights);
                }

                // ── Node sizing via heatRules ────────────────────────────────
                // Store log10(total) as the value so heatRules operates in
                // log-space — this compresses the US-vs-small-country range
                // from ~1000x down to ~3x before the min/max radius mapping.

                for (let code in countryTotals) {
                    let total = countryTotals[code];
                    let logValue = Math.log10(Math.max(1, total));

                    let lon, lat, name;
                    let isItaly = (code === HOME_COUNTRY);

                    if (isItaly) {
                        lon = homeLon;
                        lat = homeLat;
                        name = "Italy";
                    } else {
                        let poly = polygonSeries.getDataItemById(code);
                        if (!poly) continue;
                        lon = poly.get("visualLongitude");
                        lat = poly.get("visualLatitude");
                        name = poly.dataContext ? poly.dataContext.name : code;

                        if (lon === undefined || lat === undefined) {
                            let mapPoly = poly.get("mapPolygon");
                            if (mapPoly && mapPoly.visualCentroid) {
                                let centroid = mapPoly.visualCentroid();
                                if (centroid) {
                                    lon = centroid.longitude;
                                    lat = centroid.latitude;
                                }
                            }
                        }
                    }

                    if (lon === undefined || lat === undefined) continue;

                    if (isItaly) {
                        italySeries.pushDataItem({
                            geometry: { type: "Point", coordinates: [lon, lat] },
                            tooltipText: `Italy: ${total.toLocaleString()} total citations`
                        });
                    } else {
                        citySeries.pushDataItem({
                            geometry: { type: "Point", coordinates: [lon, lat] },
                            value: logValue,
                            tooltipText: `${name}: ${total.toLocaleString()} total citations`
                        });
                    }
                }
            }

        }).catch(err => console.error("Error loading data", err));
    }

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

    // ── 7. UI Controls ─────────────────────────────────────────────────────────
    var mapInstValue = "unibo";
    var mapDirValue = "incoming";

    var mapInstBtns = document.querySelectorAll(".map-inst-btn");
    var mapDirBtns = document.querySelectorAll(".map-dir-btn");

    function updateMap() {
        loadData(mapInstValue, mapDirValue);
    }

    mapInstBtns.forEach(function(btn) {
        btn.addEventListener("click", function() {
            mapInstBtns.forEach(b => b.classList.remove("active"));
            this.classList.add("active");
            mapInstValue = this.getAttribute("data-inst");
            updateMap();
        });
    });

    mapDirBtns.forEach(function(btn) {
        btn.addEventListener("click", function() {
            mapDirBtns.forEach(b => b.classList.remove("active"));
            this.classList.add("active");
            mapDirValue = this.getAttribute("data-dir");
            updateMap();
        });
    });

    polygonSeries.events.once("datavalidated", function () {
        updateMap();
    });

    chart.appear(1000, 100);
});
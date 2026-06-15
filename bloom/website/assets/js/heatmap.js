am5.ready(function () {

    var root = am5.Root.new("heatmap_chartdiv");
    root.setThemes([am5themes_Animated.new(root)]);

    var chart = root.container.children.push(
        am5xy.XYChart.new(root, {
            panX: false,
            panY: false,
            wheelX: "none",
            wheelY: "none",
            layout: root.verticalLayout
        })
    );

    var xRenderer = am5xy.AxisRendererX.new(root, {
        minGridDistance: 20,
        opposite: true
    });
    xRenderer.grid.template.setAll({ strokeOpacity: 0 });
    xRenderer.labels.template.setAll({
        fontWeight: "bold",
        fontSize: 13,
        fill: am5.color(0x320E3B)
    });

    var xAxis = chart.xAxes.push(
        am5xy.CategoryAxis.new(root, {
            categoryField: "institution",
            renderer: xRenderer
        })
    );

    var yRenderer = am5xy.AxisRendererY.new(root, {
        minGridDistance: 15,   // ← CELL HEIGHT: lower = smaller cells, higher = taller cells
        inversed: true
    });
    yRenderer.grid.template.setAll({ strokeOpacity: 0 });
    yRenderer.labels.template.setAll({
        fontSize: 12,
        fontWeight: "bold",
        fontFamily: "Inter, sans-serif",
        fill: am5.color(0x333333),
        text: "{displayName}",
        tooltipText: "{avgText}"
    });

    // Attach a tooltip so that the avgText shows on hover
    yRenderer.labels.template.set("tooltip", am5.Tooltip.new(root, {}));

    var yAxis = chart.yAxes.push(
        am5xy.CategoryAxis.new(root, {
            categoryField: "countryLabel",
            renderer: yRenderer
        })
    );

    var series = chart.series.push(
        am5xy.ColumnSeries.new(root, {
            calculateAggregates: true,
            xAxis: xAxis,
            yAxis: yAxis,
            valueField: "deviation",
            categoryXField: "institution",
            categoryYField: "countryLabel",
            tooltip: am5.Tooltip.new(root, {
                pointerOrientation: "horizontal",
                labelText: "[bold]{institution}[/] → [bold]{countryName}[/]\nDeviation: [bold]{deviationFormatted}[/] pp\nTrue proportion: [bold]{trueProportion}%[/]\nAverage: [bold]{avgText}%[/]"
            })
        })
    );

    // Cell sizing:
    // - width is driven by xAxis width divided by number of institutions
    // - height is driven by minGridDistance on yRenderer (above)
    // Both can also be capped via maxWidth / maxHeight here:
    series.columns.template.setAll({
        tooltipText: "{deviation}",
        strokeOpacity: 1,
        stroke: am5.color(0xffffff),
        strokeWidth: 2,
        cornerRadiusTL: 0,
        cornerRadiusTR: 0,
        cornerRadiusBL: 0,
        cornerRadiusBR: 0,
        width: am5.p100,
        height: am5.p100,
        maxWidth: 80,    // ← CELL WIDTH: max pixel width per cell
        maxHeight: 30    // ← CELL HEIGHT: max pixel height per cell
    });

    series.set("heatRules", [{
        target: series.columns.template,
        customFunction: function (sprite, min, max, value) {
            // Fix maxDev to 3.0 so the cell colors perfectly align with the static -3 to 3 legend
            var maxDev = 3.0;
            var ratio = Math.max(-1, Math.min(1, value / maxDev));

            var under = { r: 0xE0, g: 0x5D, b: 0x53 };
            var mid = { r: 0xF4, g: 0xF4, b: 0xF6 };
            var over = { r: 0x32, g: 0x0E, b: 0x3B };

            var r, g, b, t;
            if (ratio < 0) {
                t = Math.abs(ratio);
                r = Math.round(mid.r + (under.r - mid.r) * t);
                g = Math.round(mid.g + (under.g - mid.g) * t);
                b = Math.round(mid.b + (under.b - mid.b) * t);
            } else {
                t = ratio;
                r = Math.round(mid.r + (over.r - mid.r) * t);
                g = Math.round(mid.g + (over.g - mid.g) * t);
                b = Math.round(mid.b + (over.b - mid.b) * t);
            }
            sprite.set("fill", am5.color((r << 16) | (g << 8) | b));
        },
        dataField: "value"
    }]);

    var titleLabel = chart.children.insertIndex(0, am5.Label.new(root, {
        text: "Institutional Fingerprints:\nHow Citation Geography Differs Across Italian Universities",
        fontSize: 17,
        fontWeight: "bold",
        fontFamily: "'Playfair Display', serif",
        fill: am5.color(0x23022E),
        paddingBottom: 14,
        textAlign: "center",
        oversizedBehavior: "wrap",
        x: am5.p50,
        centerX: am5.p50,
        lineHeight: 1.4
    }));

    titleLabel.adapters.add("maxWidth", function(maxWidth, target) {
        if (root.container) {
            return root.container.innerWidth() - 20;
        }
        return 300;
    });

    titleLabel.adapters.add("fontSize", function(fontSize, target) {
        if (root.container) {
            return root.container.innerWidth() < 600 ? 14 : 17;
        }
        return 17;
    });

    var heatLegendContainer = chart.children.push(am5.Container.new(root, {
        layout: root.verticalLayout,
        width: am5.p100,
        centerX: am5.p50,
        x: am5.p50,
        paddingTop: 16
    }));

    heatLegendContainer.children.push(am5.Label.new(root, {
        text: "Deviation (pp)",
        fontSize: 12,
        fontFamily: "Inter, sans-serif",
        fontWeight: "bold",
        centerX: am5.p50,
        x: am5.p50,
        paddingBottom: 5
    }));

    var legendWrapper = heatLegendContainer.children.push(am5.Container.new(root, {
        width: 500,
        layout: root.horizontalLayout,
        centerX: am5.p50,
        x: am5.p50
    }));

    legendWrapper.adapters.add("width", function(width, target) {
        if (root.container) {
            var w = root.container.innerWidth() - 20; // 20px padding
            return w < 500 ? w : 500;
        }
        return 500;
    });

    // Left half: Red to White (-3 to 0)
    var leftLegend = legendWrapper.children.push(am5.HeatLegend.new(root, {
        orientation: "horizontal",
        startColor: am5.color(0xE05D53),
        endColor: am5.color(0xF4F4F6),
        startValue: -3,
        endValue: 0,
        stepCount: 100,
        width: am5.p50
    }));
    leftLegend.startLabel.set("forceHidden", true);
    leftLegend.endLabel.set("forceHidden", true);

    // Right half: White to Purple (0 to 3)
    var rightLegend = legendWrapper.children.push(am5.HeatLegend.new(root, {
        orientation: "horizontal",
        startColor: am5.color(0xF4F4F6),
        endColor: am5.color(0x320E3B),
        startValue: 0,
        endValue: 3,
        stepCount: 100,
        width: am5.p50
    }));
    rightLegend.startLabel.set("forceHidden", true);
    rightLegend.endLabel.set("forceHidden", true);

    var labelsContainer = heatLegendContainer.children.push(am5.Container.new(root, {
        layout: root.horizontalLayout,
        width: 500,
        centerX: am5.p50,
        x: am5.p50,
        paddingTop: 5
    }));

    labelsContainer.adapters.add("width", function(width, target) {
        if (root.container) {
            var w = root.container.innerWidth() - 20; // 20px padding
            return w < 500 ? w : 500;
        }
        return 500;
    });

    ["-3", "−2", "−1", "0", "+1", "+2", "+3"].forEach(function (text) {
        labelsContainer.children.push(am5.Label.new(root, {
            text: text,
            fontFamily: "Inter, sans-serif",
            fontSize: 11,
            textAlign: "center",
            width: am5.percent(14.28)
        }));
    });

    series.columns.template.events.on("pointerover", function (event) {
        var di = event.target.dataItem;
        if (di) {
            var val = di.get("value", 0);
            val = Math.max(-3, Math.min(3, val));
            if (val <= 0) {
                leftLegend.showValue(val);
                rightLegend.hideTooltip();
            } else {
                rightLegend.showValue(val);
                leftLegend.hideTooltip();
            }
        }
    });

    series.columns.template.events.on("pointerout", function (event) {
        leftLegend.hideTooltip();
        rightLegend.hideTooltip();
    });

    var datasets = {};

    function computeVmax(deviations) {
        var flat = [].concat.apply([], deviations);
        return Math.max.apply(null, flat.map(Math.abs)) || 2;
    }

    function formatDataset(data) {
        var vmax = computeVmax(data.deviations);
        var seriesData = [];

        for (var i = 0; i < data.categoriesY.length; i++) {
            var fullLabel = data.categoriesY[i];
            var match = fullLabel.match(/^(.*?)\s*\(Avg:\s*(.*?)\%\)$/);
            var pureAvg = match ? match[2] : "";

            for (var j = 0; j < data.categoriesX.length; j++) {
                var dev = data.deviations[i][j];
                seriesData.push({
                    institution: data.categoriesX[j],
                    countryName: data.countries[i],
                    countryLabel: fullLabel,
                    deviation: dev,
                    deviationFormatted: (dev >= 0 ? "+" : "") + dev.toFixed(1),
                    trueProportion: data.trueProportions[i][j].toFixed(1),
                    avgText: pureAvg,
                    _vmax: vmax
                });
            }
        }

        return {
            title: data.title,
            categoriesX: data.categoriesX,
            categoriesY: data.categoriesY,
            seriesData: seriesData
        };
    }

    function switchDirection(directionKey) {
        var activeSet = datasets[directionKey];

        xAxis.data.setAll(activeSet.categoriesX.map(function (item) {
            return { institution: item };
        }));
        var reversedY = [].concat(activeSet.categoriesY).reverse();
        yAxis.data.setAll(reversedY.map(function (item) {
            var match = item.match(/^(.*?)\s*\(Avg:\s*(.*?)\%\)$/);
            var pureName = match ? match[1] : item;
            var pureAvg = match ? match[2] : "";
            return {
                countryLabel: item,
                displayName: pureName,
                avgText: pureAvg ? "Avg: " + pureAvg + "%" : ""
            };
        }));

        series.data.setAll(activeSet.seriesData);
        // titleLabel.set("text", activeSet.title);

        // Dynamically reverse the arrow based on flow direction
        if (directionKey === "incoming") {
            series.get("tooltip").set("labelText", "[bold]{countryName}[/] → [bold]{institution}[/]\nDeviation: [bold]{deviationFormatted}[/] pp\nTrue proportion: [bold]{trueProportion}%[/]\nAverage: [bold]{avgText}%[/]");
        } else {
            series.get("tooltip").set("labelText", "[bold]{institution}[/] → [bold]{countryName}[/]\nDeviation: [bold]{deviationFormatted}[/] pp\nTrue proportion: [bold]{trueProportion}%[/]\nAverage: [bold]{avgText}%[/]");
        }
    }

    document.querySelectorAll(".heatmap-dir-btn").forEach(function (btn) {
        btn.addEventListener("click", function (e) {
            document.querySelectorAll(".heatmap-dir-btn").forEach(function (b) {
                b.classList.remove("active");
            });
            e.target.classList.add("active");

            var dir = e.target.getAttribute("data-dir");
            if (datasets[dir]) switchDirection(dir);
        });
    });

    Promise.all([
        fetch("visualizations/heatmap/heatmap_incoming.json").then(r => r.json()),
        fetch("visualizations/heatmap/heatmap_outgoing.json").then(r => r.json())
    ]).then(function (results) {
        datasets.incoming = formatDataset(results[0]);
        datasets.outgoing = formatDataset(results[1]);
        switchDirection("incoming");
        series.appear(1000);
        chart.appear(1000);
    }).catch(function (error) {
        console.error("Error loading heatmap data:", error);
    });

});
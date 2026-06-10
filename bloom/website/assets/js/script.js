document.addEventListener('DOMContentLoaded', () => {
    // Progress Bar
    const progressBar = document.getElementById('progress-bar');

    window.addEventListener('scroll', () => {
        const totalHeight = document.body.scrollHeight - window.innerHeight;
        const progress = (window.scrollY / totalHeight) * 100;
        progressBar.style.width = `${progress}%`;
    });

    // Intersection Observer for scroll animations
    const observerOptions = {
        root: null,
        rootMargin: '0px',
        threshold: 0.2 // Trigger when 20% of the element is visible
    };

    const observer = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                // Optional: Stop observing once it's visible if we don't want it to fade out again
                // observer.unobserve(entry.target);
            }
        });
    }, observerOptions);

    // Observe all elements with animation classes
    const animatedElements = document.querySelectorAll('.fade-in, .slide-up');
    animatedElements.forEach(el => observer.observe(el));

    // Trigger animations for elements already in viewport on load
    setTimeout(() => {
        animatedElements.forEach(el => {
            const rect = el.getBoundingClientRect();
            if (rect.top < window.innerHeight) {
                el.classList.add('visible');
            }
        });
    }, 100);
});



// --- amCharts Map Initialization ---
am5.ready(function () {
    var root = am5.Root.new("hero-map");
    root.setThemes([am5themes_Animated.new(root)]);

    var isMobile = window.innerWidth <= 900;
    var targetZoom = isMobile ? 1.5 : 1.8; // Set custom zoom levels for both screen sizes
    var chart = root.container.children.push(
        am5map.MapChart.new(root, {
            panX: "none",
            panY: "none",
            wheelX: "none",
            wheelY: "none",
            pinchZoomX: "none",
            pinchZoomY: "none",
            projection: am5map.geoMercator(),
            minZoomLevel: targetZoom,
            maxZoomLevel: targetZoom,
            zoomLevel: targetZoom, // <-- Crucial: Tells the map to actually load at this zoom level!
            homeGeoPoint: isMobile
                ? { longitude: 12.5, latitude: 41.9 } // Center on Italy on mobile
                : { longitude: -75.0, latitude: 42.0 } // Focus on Spain/West to push Italy to the right side on desktop
        })
    );

    chart.chartContainer.set("background", am5.Rectangle.new(root, {
        fill: am5.color(0x320E3B), // dark purple
        fillOpacity: 1
    }));

    var polygonSeries = chart.series.push(
        am5map.MapPolygonSeries.new(root, {
            geoJSON: am5geodata_worldLow,
            exclude: ["AQ"]
        })
    );

    polygonSeries.mapPolygons.template.setAll({
        fill: am5.color(0x2A0C32),
        stroke: am5.color(0x320E3B),
        strokeWidth: 0.5
    });

    // 1. Create a MapLineSeries for the visible citation paths
    var lineSeries = chart.series.push(
        am5map.MapLineSeries.new(root, {})
    );

    lineSeries.mapLines.template.setAll({
        stroke: am5.color(0xB7990D), // gold
        strokeWidth: 1.5,
        strokeOpacity: 0.4
    });

    // 2. Create a MapPointSeries for the animated flowing arrows
    var flowSeries = chart.series.push(
        am5map.MapPointSeries.new(root, {})
    );

    flowSeries.bullets.push(function (root, series, dataItem) {
        var arrow = am5.Graphics.new(root, {
            fill: am5.color(0xB7990D),
            svgPath: "M-6,-4 L8,0 L-6,4 Z" // Sleek pointed arrow
        });

        return am5.Bullet.new(root, {
            sprite: arrow,
            autoRotate: true,
            autoRotateAngle: 0
        });
    });

    // 3. Create a MapPointSeries to visualize the cities with circles
    var citySeries = chart.series.push(
        am5map.MapPointSeries.new(root, {})
    );

    citySeries.bullets.push(function (root) {
        var circle = am5.Circle.new(root, {
            radius: 6,
            fill: am5.color(0xB7990D),
            tooltipText: "{title}"
        });


        return am5.Bullet.new(root, {
            sprite: circle
        });
    });

    // Add data for the cities involved
    var citiesData = [
        { geometry: { type: "Point", coordinates: [12.5, 41.9] }, title: "Rome" },
        { geometry: { type: "Point", coordinates: [-74.0, 40.7] }, title: "New York" },
        { geometry: { type: "Point", coordinates: [-0.1, 51.5] }, title: "London" },
        { geometry: { type: "Point", coordinates: [139.6, 35.6] }, title: "Tokyo" },
        { geometry: { type: "Point", coordinates: [151.2, -33.8] }, title: "Sydney" },
        { geometry: { type: "Point", coordinates: [-58.3, -34.6] }, title: "Buenos Aires" }
    ];

    citySeries.data.setAll(citiesData);

    var linesData = [
        {
            geometry: { type: "LineString", coordinates: [[-74.0, 40.7], [12.5, 41.9]] },
            delay: 2000 // INCOMING 1 (New York -> Rome)
        },
        {
            geometry: { type: "LineString", coordinates: [[12.5, 41.9], [-0.1, 51.5]] },
            delay: 0    // OUTGOING 1 (Rome -> London: starts immediately)
        },
        {
            geometry: { type: "LineString", coordinates: [[139.6, 35.6], [12.5, 41.9]] },
            delay: 3000 // INCOMING 2 (Tokyo -> Rome)
        },
        {
            geometry: { type: "LineString", coordinates: [[12.5, 41.9], [151.2, -33.8]] },
            delay: 1000 // OUTGOING 2 (Rome -> Sydney)
        },
        {
            geometry: { type: "LineString", coordinates: [[-58.3, -34.6], [12.5, 41.9]] },
            delay: 4000 // INCOMING 3 (Buenos Aires -> Rome)
        }
    ];

    // Initialize the static lines and their corresponding flowing arrows
    linesData.forEach(function (lineData) {
        // Create the persistent static line
        var lineDataItem = lineSeries.pushDataItem(lineData);

        // Create the flowing arrow attached to this line
        var flowDataItem = flowSeries.pushDataItem({
            lineDataItem: lineDataItem,
            positionOnLine: 0.0,
            autoRotate: true
        });

        // Define the infinite looping animation for this arrow
        function loopCitationFlow() {
            var duration = 3000 + Math.random() * 2000;

            // Animate from start (0) to end (1) of the line
            flowDataItem.animate({
                key: "positionOnLine",
                from: 0,
                to: 1,
                duration: duration
            });

            // Wait for duration to finish, then pause randomly before looping again
            var pause = 1000 + Math.random() * 2000;
            setTimeout(loopCitationFlow, duration + pause);
        }

        // Start the loop with the initial staggered delay
        setTimeout(loopCitationFlow, lineData.delay);
    });

    chart.appear(1000, 100);
});

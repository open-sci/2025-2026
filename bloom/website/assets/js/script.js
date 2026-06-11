document.addEventListener('DOMContentLoaded', () => {
    // Progress Bar
    const progressBar = document.getElementById('progress-bar');

    window.addEventListener('scroll', () => {
        const totalHeight = document.body.scrollHeight - window.innerHeight;
        const progress = (window.scrollY / totalHeight) * 100;
        progressBar.style.width = `${progress}%`;

        // Toggle visibility of Research Question Navigation
        const rqNav = document.querySelector('.rq-nav');
        const projectSection = document.getElementById('project');
        if (rqNav && projectSection) {
            // Show nav when we scroll past the top of the project section minus half window height
            if (window.scrollY > projectSection.offsetTop - (window.innerHeight / 2)) {
                rqNav.classList.add('visible');
            } else {
                rqNav.classList.remove('visible');
            }
        }

        // Update active button progress dot
        const activeSection = document.querySelector('.rq-section.active');
        const activeBtn = document.querySelector('.rq-btn.active');
        if (activeSection && activeBtn) {
            const dot = activeBtn.querySelector('.progress-dot');
            const track = activeBtn.querySelector('.progress-track');
            if (dot && track) {
                const navbarHeight = document.querySelector('.navbar')?.offsetHeight || 80;
                const sectionTop = activeSection.offsetTop - navbarHeight;
                const scrollableDistance = activeSection.offsetHeight - window.innerHeight + navbarHeight;
                
                let sectionProgress = 0;
                if (scrollableDistance > 0) {
                    sectionProgress = (window.scrollY - sectionTop) / scrollableDistance;
                }
                
                // Clamp between 0 and 1
                sectionProgress = Math.max(0, Math.min(1, sectionProgress));
                
                // Max distance the dot can move
                const maxTop = track.offsetHeight - dot.offsetHeight;
                dot.style.top = `${sectionProgress * maxTop}px`;
            }
        }
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

// --- Research Question Navigation ---
document.addEventListener('DOMContentLoaded', () => {
    const rqBtns = document.querySelectorAll('.rq-btn');
    const rqSections = document.querySelectorAll('.rq-section');

    if (rqBtns.length === 0) return;

    rqBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.getAttribute('data-target');

            // Update buttons
            rqBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Update sections
            rqSections.forEach(section => {
                if (section.id === targetId) {
                    section.classList.add('active');
                } else {
                    section.classList.remove('active');
                }
            });

            // Re-trigger scroll animations for the newly visible section
            setTimeout(() => {
                const targetSection = document.getElementById(targetId);
                if (targetSection) {
                    const animatedElements = targetSection.querySelectorAll('.fade-in, .slide-up');
                    animatedElements.forEach(el => {
                        const rect = el.getBoundingClientRect();
                        if (rect.top < window.innerHeight) {
                            el.classList.add('visible');
                        }
                    });

                    // Scroll to the top of the newly active section (offset for navbar)
                    const navbarHeight = document.querySelector('.navbar')?.offsetHeight || 80;
                    const sectionTop = targetSection.getBoundingClientRect().top + window.pageYOffset;
                    window.scrollTo({
                        top: sectionTop - navbarHeight - 20, // 20px extra padding for breathing room
                        behavior: 'smooth'
                    });
                }

                // Dispatch resize event to fix any charts that need to adjust to visibility
                window.dispatchEvent(new Event('resize'));
            }, 100);
        });
    });
});

// --- Discrete Section Connectors ---
document.addEventListener('DOMContentLoaded', () => {
    const sections = document.querySelectorAll('.section');
    sections.forEach((sec, index) => {
        // Skip the very last section (footer/credits usually)
        if (index === sections.length - 1) return;
        
        // Also skip the hero section if we don't want an arrow pointing down from the map 
        // (but hero usually already has a scroll indicator, so we skip it)
        if (sec.classList.contains('hero')) return;

        const arrowContainer = document.createElement('div');
        arrowContainer.className = 'section-connector';
        arrowContainer.innerHTML = `
            <svg width="60" height="150" viewBox="0 0 60 150" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M 30 0 C 30 35, 55 35, 55 65 C 55 95, 30 95, 30 130" stroke="var(--color-accent)" stroke-width="3" stroke-dasharray="8 8" class="connector-line" stroke-linecap="round" />
                <path d="M 15 115 L 30 130 L 45 115" stroke="var(--color-accent)" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        `;
        sec.appendChild(arrowContainer);
    });
});

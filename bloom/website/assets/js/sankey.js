(function () {

  // Config
  var INSTITUTIONS = ["UNIBO", "UNIMI", "UNIPD", "UNITO", "SNS", "UPO"];
  var INST_LABELS = {
    UNIBO: "UNIBO",
    UNIMI: "UNIMI",
    UNIPD: "UNIPD",
    UNITO: "UNITO",
    SNS: "SNS",
    UPO: "UPO"
  };
  var TOP_N = 15;
  var DIV_ID = "sankey-chartdiv";

  // One colour per institution — matches the notebook sunburst INST_COLORS palette
  var INST_COLORS = {
    UNIBO: 0x264653,   // deep teal
    UNIMI: 0x2a9d8f,   // muted teal-green
    UNIPD: 0x8ab17d,   // sage green
    UNITO: 0xe9c46a,   // warm ochre
    UPO: 0xf4a261,   // soft orange
    SNS: 0xe76f51    // muted coral
  };
  // (string-quote fix no longer needed)

  // ── CSV loader ───────────────────────────────────────────────────────────────
  function loadCSV(url) {
    return new Promise(function (resolve, reject) {
      Papa.parse(url, {
        download: true,
        header: true,
        dynamicTyping: true,
        skipEmptyLines: true,
        complete: function (r) { resolve(r.data); },
        error: function (e) { reject(e); }
      });
    });
  }

  // ── Data builder ─────────────────────────────────────────────────────────────
  function buildAllData(allResults) {
    // allResults: [ [incRows, outRows], ... ] one pair per institution (same order as INSTITUTIONS)

    // 1. Compute per-institution top-15 org lists
    //    We want the top-15 by citation count *for that institution*
    function topN(rows, n) {
      return rows
        .filter(function (r) { return r.legal_name && r.count > 0; })
        .sort(function (a, b) { return b.count - a.count; })
        .slice(0, n);
    }

    var links = [];

    INSTITUTIONS.forEach(function (inst, idx) {
      var incRows = allResults[idx][0];
      var outRows = allResults[idx][1];
      var label = INST_LABELS[inst];
      var color = am5.color(INST_COLORS[inst]);

      var incTop = topN(incRows, TOP_N);
      var outTop = topN(outRows, TOP_N);

      // incoming org → institution
      incTop.forEach(function (row) {
        links.push({
          from: "INC:" + row.legal_name,
          to: label,
          value: row.count,
          inst: inst,
          linkColor: color,
          dir: "incoming"
        });
      });

      // institution → outgoing org
      outTop.forEach(function (row) {
        links.push({
          from: label,
          to: "OUT:" + row.legal_name,
          value: row.count,
          inst: inst,
          linkColor: color,
          dir: "outgoing"
        });
      });
    });

    return links;
  }

  // ── Chart builder ─────────────────────────────────────────────────────────────
  function buildChart(allResults) {
    var div = document.getElementById(DIV_ID);
    if (!div) return;

    div.innerHTML = "";

    // Root + theme
    var root = am5.Root.new(DIV_ID);
    root.setThemes([am5themes_Animated.new(root)]);

    // Sankey series
    var series = root.container.children.push(
      am5flow.Sankey.new(root, {
        sourceIdField: "from",
        targetIdField: "to",
        valueField: "value",
        nodeWidth: 20,
        nodePadding: 5,
        paddingLeft: 150,
        paddingRight: 150,
        paddingTop: 15,
        paddingBottom: 15,
        orientation: "horizontal"
      })
    );

    // Stepped colours for org nodes
    series.nodes.get("colors").set("step", 2);

    // ── Node styling ──────────────────────────────────────────────────────────
    series.nodes.rectangles.template.setAll({
      fillOpacity: 1,
      strokeOpacity: 0,
      cornerRadiusTL: 3,
      cornerRadiusTR: 3,
      cornerRadiusBL: 3,
      cornerRadiusBR: 3,
      tooltipText: "{name}"
    });

    // Calculate totals for tooltips
    var instLabelSet = {};
    var instTotalMap = {};
    INSTITUTIONS.forEach(function (inst, idx) {
      var label = INST_LABELS[inst];
      instLabelSet[label] = INST_COLORS[inst];
      var incTotal = 0, outTotal = 0;
      allResults[idx][0].forEach(function (r) { if (r.count) incTotal += r.count; });
      allResults[idx][1].forEach(function (r) { if (r.count) outTotal += r.count; });
      instTotalMap[label] = { incoming: incTotal, outgoing: outTotal };
    });

    series.nodes.rectangles.template.adapters.add("tooltipText", function (text, target) {
      var di = target.dataItem;
      if (!di) return text;
      var name = di.get("name") || "";
      var isInst = (instLabelSet[name] !== undefined);
      var cleanName = name.replace(/^INC:/, "").replace(/^OUT:/, "");

      if (isInst) {
        var totals = instTotalMap[name];
        var inc = totals ? totals.incoming : 0;
        var out = totals ? totals.outgoing : 0;
        return "[bold]" + cleanName + "[/]\nIncoming Citations: " + inc.toLocaleString() + "\nOutgoing Citations: " + out.toLocaleString();
      } else {
        var lines = ["[bold]" + cleanName + "[/]"];
        var isInc = name.startsWith("INC:");
        var links = isInc ? di.get("outgoingLinks") : di.get("incomingLinks");

        if (links && links.length > 0) {
          var linksArray = [];
          for (var i = 0; i < links.length; i++) { linksArray.push(links[i]); }
          linksArray.sort(function (a, b) { return b.get("value") - a.get("value"); });

          linksArray.forEach(function (link) {
            var instNode = isInc ? link.get("target") : link.get("source");
            if (!instNode) return;
            var instName = instNode.get("name");
            var val = link.get("value");
            var totals = instTotalMap[instName];
            if (totals) {
              var totalToUse = isInc ? totals.incoming : totals.outgoing;
              var pct = (val / totalToUse * 100).toFixed(1);
              lines.push(instName + ": " + val.toLocaleString() + " (" + pct + "%)");
            } else {
              lines.push(instName + ": " + val.toLocaleString());
            }
          });
        }
        return lines.join("\n");
      }
    });

    series.nodes.labels.template.setAll({
      fontSize: 11,
      fontFamily: "Inter, sans-serif",
      fill: am5.color(0x1A1A1A),
      maxWidth: 155,
      oversizedBehavior: "wrap",
      populateText: true,
      text: "{name}"
    });

    series.nodes.labels.template.adapters.add("text", function (text, target) {
      var di = target.dataItem;
      if (!di) return text;
      var name = di.get("name") || "";
      return name.replace(/^INC:/, "").replace(/^OUT:/, "");
    });

    // ── Link styling ──────────────────────────────────────────────────────────
    series.links.template.setAll({
      fillOpacity: 0.20,
      strokeOpacity: 0,
      tooltipText: "{sourceId} → {targetId}: {value}"
    });

    // Colour each link by institution
    series.links.template.adapters.add("fill", function (fill, target) {
      var di = target.dataItem;
      if (!di) return fill;
      var ctx = di.dataContext;
      if (!ctx) return fill;
      return ctx.linkColor || fill;
    });

    series.links.template.states.create("hover", {
      fillOpacity: 0.50
    });

    // ── Format nodes after data is validated ────────────
    series.events.on("datavalidated", function () {
      var orgColorMap = {};
      var colorSet = am5.ColorSet.new(root, { step: 2 });
      var colorIndex = 0;

      series.nodes.dataItems.forEach(function (di) {
        var name = di.get("name") || "";
        var rect = di.get("rectangle");
        var label = di.get("label");

        if (instLabelSet[name] !== undefined) {
          rect.setAll({
            fill: am5.color(0xB7990D),   // site gold accent
            fillOpacity: 1,
            strokeOpacity: 0
          });
          label.setAll({
            rotation: -90,
            x: am5.p50,
            y: am5.p50,
            centerX: am5.p50,
            centerY: am5.p50,
            fill: am5.color(0x320E3B),   // Dark purple for contrast on pale gold
            fontWeight: "700",
            fontSize: 12
          });
        } else {
          // Sync colors for incoming and outgoing nodes of the same organization
          var cleanName = name.replace(/^INC:/, "").replace(/^OUT:/, "");
          if (!orgColorMap[cleanName]) {
            orgColorMap[cleanName] = colorSet.getIndex(colorIndex++);
          }
          rect.set("fill", orgColorMap[cleanName]);
        }
      });
    });

    // ── Hide labels for smaller organization nodes ────────────
    series.nodes.rectangles.template.events.on("boundschanged", function (e) {
      var rect = e.target;
      var di = rect.dataItem;
      if (!di) return;
      var name = di.get("name") || "";
      var label = di.get("label");

      if (label && instLabelSet[name] === undefined) {
        // Hide if the node's physical height is less than 16px to prevent label clutter
        if (rect.height() < 16) {
          label.set("forceHidden", true);
        } else {
          label.set("forceHidden", false);
        }
      }
    });

    // ── Set data + animate ────────────────────────────────────────────────────
    var links = buildAllData(allResults);
    series.data.setAll(links);
    series.appear(1200, 100);
  }

  // ── Bootstrap: load all 12 CSVs in parallel ───────────────────────────────
  function init() {
    var div = document.getElementById(DIV_ID);
    if (!div) return;

    div.innerHTML = '<div class="sankey-loading">Loading data for all institutions…</div>';

    var fetchPromises = INSTITUTIONS.map(function (inst) {
      var base = "../map_of_italian_science/data/visualizations/" + inst + "/";
      return Promise.all([
        loadCSV(base + "citation_counts_organizations_incoming_clean.csv"),
        loadCSV(base + "citation_counts_organizations_outgoing_clean.csv")
      ]);
    });

    Promise.all(fetchPromises)
      .then(function (allResults) {
        buildChart(allResults);
      })
      .catch(function (err) {
        console.error("Sankey load error:", err);
        if (div) {
          div.innerHTML =
            '<div class="sankey-loading sankey-error">Could not load data. Check CSV paths.</div>';
        }
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function() {
      init();
      initUI();
    });
  } else {
    init();
    initUI();
  }

  // ── Plotly Scatter Plot Integration ───────────────────────────────
  var scatterLoaded = false;
  function loadScatter() {
    if (scatterLoaded) return;
    scatterLoaded = true;

    var scatterDiv = document.getElementById("scatter-chartdiv");
    if (!scatterDiv) return;

    scatterDiv.innerHTML = '<iframe src="visualizations/reciprocity.html" width="100%" height="100%" frameborder="0" style="border-radius: 12px; background-color: #2A0C32;"></iframe>';
  }

  // ── Toggle UI Logic ───────────────────────────────
  function initUI() {
    var select = document.getElementById("org-view-select");
    var sankeyDiv = document.getElementById("sankey-chartdiv");
    var scatterDiv = document.getElementById("scatter-chartdiv");

    if (select && sankeyDiv && scatterDiv) {
      select.addEventListener("change", function() {
        if (this.value === "sankey") {
           sankeyDiv.style.display = "block";
           scatterDiv.style.display = "none";
        } else {
           sankeyDiv.style.display = "none";
           scatterDiv.style.display = "block";
           loadScatter();
        }
      });
    }
  }

})();

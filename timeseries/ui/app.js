// OpenData Timeseries UI

(function () {
  "use strict";

  var exprInput = document.getElementById("expr");
  var executeBtn = document.getElementById("execute-btn");
  var errorDisplay = document.getElementById("error-display");
  var loadingEl = document.getElementById("loading");
  var tableOutput = document.getElementById("table-output");
  var graphOutput = document.getElementById("graph-output");
  var endTimeInput = document.getElementById("end-time");
  var rangeDuration = document.getElementById("range-duration");
  var stepInput = document.getElementById("step-input");

  var evalTimeInput = document.getElementById("eval-time");

  var tabs = document.querySelectorAll(".tab");
  var panelTable = document.getElementById("panel-table");
  var panelGraph = document.getElementById("panel-graph");

  var activeTab = "table";
  var currentPlot = null;
  var hoverTable = document.getElementById("graph-hover-table");

  // Series colors (modern palette)
  var COLORS = [
    "#6366f1", "#22c55e", "#f59e0b", "#ec4899",
    "#06b6d4", "#f97316", "#8b5cf6", "#14b8a6",
    "#ef4444", "#3b82f6", "#a855f7", "#10b981"
  ];

  // --- URL param helpers ---

  function getParams() {
    return new URLSearchParams(window.location.search);
  }

  function updateUrl() {
    var params = new URLSearchParams();
    var expr = exprInput.value.trim();
    if (expr) params.set("expr", expr);
    if (activeTab !== "table") params.set("tab", activeTab);
    if (evalTimeInput.value) params.set("eval", evalTimeInput.value);
    if (endTimeInput.value) params.set("end", endTimeInput.value);
    if (rangeDuration.value !== "3600") params.set("duration", rangeDuration.value);
    if (stepInput.value.trim() !== "15s") params.set("step", stepInput.value.trim());

    var qs = params.toString();
    var newUrl = window.location.pathname + (qs ? "?" + qs : "");
    history.replaceState(null, "", newUrl);
  }

  // --- Initialize from URL params ---

  function initFromUrl() {
    var params = getParams();

    if (params.has("expr")) {
      exprInput.value = params.get("expr");
    }
    if (params.has("tab") && (params.get("tab") === "graph" || params.get("tab") === "table")) {
      activeTab = params.get("tab");
    }
    if (params.has("duration")) {
      var d = parseInt(params.get("duration"), 10);
      if (Number.isFinite(d) && d > 0) rangeDuration.value = params.get("duration");
    }
    if (params.has("step")) {
      stepInput.value = params.get("step");
    }
    if (params.has("eval")) {
      evalTimeInput.value = params.get("eval");
    }
    if (params.has("end")) {
      endTimeInput.value = params.get("end");
    }
    if (!params.has("eval") && !params.has("end")) {
      initEndTimeToNow();
    } else {
      if (!params.has("eval")) evalTimeInput.value = formatLocalDatetime(new Date());
      if (!params.has("end")) endTimeInput.value = formatLocalDatetime(new Date());
    }

    // Apply active tab
    tabs.forEach(function (t) {
      t.classList.toggle("active", t.getAttribute("data-tab") === activeTab);
    });
    panelTable.style.display = activeTab === "table" ? "" : "none";
    panelGraph.style.display = activeTab === "graph" ? "" : "none";

    // Auto-execute if there's an expression
    if (exprInput.value.trim()) {
      execute();
    }
  }

  function formatLocalDatetime(date) {
    var y = date.getFullYear();
    var mo = String(date.getMonth() + 1).padStart(2, "0");
    var d = String(date.getDate()).padStart(2, "0");
    var h = String(date.getHours()).padStart(2, "0");
    var mi = String(date.getMinutes()).padStart(2, "0");
    var s = String(date.getSeconds()).padStart(2, "0");
    return y + "-" + mo + "-" + d + "T" + h + ":" + mi + ":" + s;
  }

  function initEndTimeToNow() {
    var now = formatLocalDatetime(new Date());
    endTimeInput.value = now;
    evalTimeInput.value = now;
  }

  // Tab switching
  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      activeTab = tab.getAttribute("data-tab");
      tabs.forEach(function (t) { t.classList.remove("active"); });
      tab.classList.add("active");
      panelTable.style.display = activeTab === "table" ? "" : "none";
      panelGraph.style.display = activeTab === "graph" ? "" : "none";
      updateUrl();
    });
  });

  // Execute on button click or Ctrl/Cmd+Enter
  executeBtn.addEventListener("click", execute);
  exprInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") {
      execute();
    }
  });

  function execute() {
    updateUrl();
    if (activeTab === "table") {
      executeInstantQuery();
    } else {
      executeRangeQuery();
    }
  }

  function showError(msg) {
    errorDisplay.textContent = msg;
    errorDisplay.style.display = "";
  }

  function hideError() {
    errorDisplay.style.display = "none";
  }

  function showLoading() {
    loadingEl.style.display = "";
  }

  function hideLoading() {
    loadingEl.style.display = "none";
  }

  function escapeHtml(str) {
    var el = document.createElement("span");
    el.textContent = str;
    return el.innerHTML;
  }

  function formatMetric(metric) {
    if (!metric || Object.keys(metric).length === 0) return "{}";
    var name = metric.__name__ || "";
    var labels = [];
    for (var k in metric) {
      if (k === "__name__") continue;
      labels.push(k + '="' + metric[k] + '"');
    }
    if (labels.length === 0) return name || "{}";
    return name + "{" + labels.join(", ") + "}";
  }

  // Instant query
  function executeInstantQuery() {
    var expr = exprInput.value.trim();
    if (!expr) return;

    hideError();
    showLoading();
    tableOutput.innerHTML = "";

    var evalStr = evalTimeInput.value;
    var evalTs = evalStr ? Math.floor(new Date(evalStr).getTime() / 1000) : Math.floor(Date.now() / 1000);
    var url = "/api/v1/query?query=" + encodeURIComponent(expr) + "&time=" + evalTs;
    fetch(url)
      .then(function (resp) { return resp.json(); })
      .then(function (data) {
        hideLoading();
        if (data.status === "error") {
          showError(data.error || "Query failed");
          return;
        }
        renderTable(data.data);
      })
      .catch(function (err) {
        hideLoading();
        showError("Request failed: " + err.message);
      });
  }

  function renderTable(data) {
    if (!data || !data.result) {
      tableOutput.innerHTML = '<div class="empty-message">No data</div>';
      return;
    }

    var resultType = data.resultType;

    if (resultType === "scalar" || resultType === "string") {
      var val = Array.isArray(data.result) ? data.result[1] : data.result;
      tableOutput.innerHTML =
        '<table class="result-table"><thead><tr><th>Value</th></tr></thead>' +
        "<tbody><tr><td>" + escapeHtml(String(val)) + "</td></tr></tbody></table>";
      return;
    }

    // vector
    var results = data.result;
    if (!results || results.length === 0) {
      tableOutput.innerHTML = '<div class="empty-message">Empty query result</div>';
      return;
    }

    var html = '<table class="result-table"><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>';
    for (var i = 0; i < results.length; i++) {
      var metric = results[i].metric || {};
      var value = results[i].value;
      var val = Array.isArray(value) ? value[1] : value;
      html += "<tr><td class=\"metric-cell\">" + escapeHtml(formatMetric(metric)) +
        "</td><td class=\"value-cell\">" + escapeHtml(String(val)) + "</td></tr>";
    }
    html += "</tbody></table>";
    tableOutput.innerHTML = html;
  }

  // Range query
  function executeRangeQuery() {
    var expr = exprInput.value.trim();
    if (!expr) return;

    hideError();
    showLoading();
    graphOutput.innerHTML = "";
    hoverTable.innerHTML = "";

    var endStr = endTimeInput.value;
    var endTs = endStr ? Math.floor(new Date(endStr).getTime() / 1000) : Math.floor(Date.now() / 1000);
    var duration = parseInt(rangeDuration.value, 10);
    if (!Number.isFinite(duration) || duration <= 0) duration = 3600;
    var startTs = endTs - duration;
    var step = stepInput.value.trim() || "15s";

    var url = "/api/v1/query_range?query=" + encodeURIComponent(expr) +
      "&start=" + startTs + "&end=" + endTs + "&step=" + encodeURIComponent(step);

    fetch(url)
      .then(function (resp) { return resp.json(); })
      .then(function (data) {
        hideLoading();
        if (data.status === "error") {
          showError(data.error || "Query failed");
          return;
        }
        renderGraph(data.data, startTs, endTs);
      })
      .catch(function (err) {
        hideLoading();
        showError("Request failed: " + err.message);
      });
  }

  function renderGraph(data, queryStart, queryEnd) {
    if (!data || !data.result || data.result.length === 0) {
      graphOutput.innerHTML = '<div class="empty-message">No data</div>';
      hoverTable.innerHTML = "";
      return;
    }

    var series = data.result;

    // Collect all unique timestamps across series
    var tsSet = {};
    for (var i = 0; i < series.length; i++) {
      var values = series[i].values || [];
      for (var j = 0; j < values.length; j++) {
        tsSet[values[j][0]] = true;
      }
    }
    var timestamps = Object.keys(tsSet).map(Number).sort(function (a, b) { return a - b; });

    if (timestamps.length === 0) {
      graphOutput.innerHTML = '<div class="empty-message">No data points</div>';
      return;
    }

    // Build aligned data: [timestamps, series0, series1, ...]
    var aligned = [timestamps];
    var uplotSeries = [{}]; // first entry = x-axis config

    for (var i = 0; i < series.length; i++) {
      var label = formatMetric(series[i].metric);
      var color = COLORS[i % COLORS.length];
      uplotSeries.push({
        label: label,
        stroke: color,
        width: 1.5
      });

      // Build a map of timestamp -> value for this series
      var valMap = {};
      var vals = series[i].values || [];
      for (var j = 0; j < vals.length; j++) {
        valMap[vals[j][0]] = parseFloat(vals[j][1]);
      }

      // Align to the common timestamps
      var seriesData = new Array(timestamps.length);
      for (var k = 0; k < timestamps.length; k++) {
        var v = valMap[timestamps[k]];
        seriesData[k] = v !== undefined ? v : null;
      }
      aligned.push(seriesData);
    }

    // Destroy previous plot
    if (currentPlot) {
      currentPlot.destroy();
      currentPlot = null;
    }

    // Build the static hover table shell
    var hoverHtml = '<table class="hover-table"><thead><tr><th></th><th>Series</th><th>Timestamp</th><th>Value</th></tr></thead><tbody>';
    for (var i = 0; i < series.length; i++) {
      var color = COLORS[i % COLORS.length];
      var label = formatMetric(series[i].metric);
      hoverHtml += '<tr><td><span class="hover-swatch" style="background:' + color + '"></span></td>' +
        '<td class="hover-label">' + escapeHtml(label) + '</td>' +
        '<td class="hover-ts" id="hover-ts-' + i + '">-</td>' +
        '<td class="hover-value" id="hover-val-' + i + '">-</td></tr>';
    }
    hoverHtml += '</tbody></table>';
    hoverTable.innerHTML = hoverHtml;

    var width = graphOutput.clientWidth || 900;
    var opts = {
      width: width,
      height: 500,
      series: uplotSeries,
      axes: [
        {
          values: function (u, vals) {
            return vals.map(function (v) {
              var d = new Date(v * 1000);
              return String(d.getHours()).padStart(2, "0") + ":" +
                String(d.getMinutes()).padStart(2, "0");
            });
          }
        },
        {}
      ],
      legend: { show: false },
      cursor: { drag: { x: true, y: false } },
      scales: { x: { time: false, min: queryStart, max: queryEnd } },
      hooks: {
        setCursor: [function (u) {
          var idx = u.cursor.idx;
          var tsStr = "-";
          if (idx != null) {
            var d = new Date(u.data[0][idx] * 1000);
            tsStr = d.getUTCFullYear() + "-" +
              String(d.getUTCMonth() + 1).padStart(2, "0") + "-" +
              String(d.getUTCDate()).padStart(2, "0") + "T" +
              String(d.getUTCHours()).padStart(2, "0") + ":" +
              String(d.getUTCMinutes()).padStart(2, "0") + ":" +
              String(d.getUTCSeconds()).padStart(2, "0") + "Z";
          }
          for (var s = 1; s < u.series.length; s++) {
            var tsCell = document.getElementById("hover-ts-" + (s - 1));
            var valCell = document.getElementById("hover-val-" + (s - 1));
            if (tsCell) tsCell.textContent = tsStr;
            if (!valCell) continue;
            if (idx == null) {
              valCell.textContent = "-";
            } else {
              var v = u.data[s][idx];
              valCell.textContent = v != null ? v.toLocaleString() : "-";
            }
          }
        }]
      }
    };

    currentPlot = new uPlot(opts, aligned, graphOutput);
  }

  // Resize chart on window resize
  var resizeTimer = null;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      if (currentPlot) {
        currentPlot.setSize({
          width: graphOutput.clientWidth,
          height: 500
        });
      }
    }, 100);
  });

  // Boot
  initFromUrl();
})();

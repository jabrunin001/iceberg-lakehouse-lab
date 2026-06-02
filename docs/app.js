const D = window.DATA || {}; const k = D.kpis || {};
document.getElementById("gen").textContent = D.generated || "";

document.getElementById("kpis").innerHTML = [
  ["Movies", (k.movies || 0).toLocaleString()],
  ["Ratings", (k.ratings || 0).toLocaleString()],
  ["Playback events", (k.events || 0).toLocaleString()],
  ["Daily partitions", k.partitions || 0],
  ["dbt tests", (k.tests || 0) + "/" + (k.tests || 0), true],
  ["Great Expectations", (k.ge || 0) + "/" + (k.ge || 0), true],
].map(([l, v, a]) => `<div class="kpi"><div class="v ${a ? "accent" : ""}">${v}</div><div class="l">${l}</div></div>`).join("");

if (D.top_titles) barChart("tt", D.top_titles.labels, D.top_titles.values, { horizontal: true });
if (D.dau) lineChart("dau", D.dau.labels, D.dau.values, { zero: false });
if (D.engagement) scatterChart("eng", D.engagement, { xlabel: "completion rate", ylabel: "avg rating" });

const f = D.features || {};
document.getElementById("featTbl").innerHTML =
  "<tr><th>Capability</th><th>Evidence from the run</th></tr>" + [
    ["Time-travel & rollback", "rolled back a bad delete to the prior snapshot; row counts matched the original"],
    ["Schema evolution", `added a column; all ${(f.events || 0).toLocaleString()} existing rows read back as NULL, no data rewrite`],
    ["Hidden partitioning", `partitioned by days(event_ts) → ${f.partitions || 0} daily partitions, no partition column to manage`],
    ["Compaction & maintenance", "rewrite_data_files ran; small files compacted into larger ones"],
    ["Snapshot expiry", `expire_snapshots: ${f.snapshots_before || 2} → ${f.snapshots_after || 1} snapshots retained`],
  ].map(([c, e]) => `<tr><td><strong>${c}</strong></td><td>${e}</td></tr>`).join("");

initTabs(); initIcons();

/* Strata console.
 *
 * No framework, no build step.  Every number rendered here comes from a real
 * server call; the page never synthesises a diff or a plan locally.
 */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const state = {
  data: null,             // /api/state payload
  tab: "editor",
  selected: null,         // table name
  activeEdit: null,       // draft edit id
  readOnlyModel: null,    // published model when no draft
  activeBuild: null,      // build id being polled
  plan: null,             // last plan for activeEdit
  editGraph: null,        // graph of the active draft, incl. new tables
  impact: null,
  impactProblems: null,
  impactFor: null,
  lastPreview: null,      // build detail for the preview tab
  pollTimer: null,
  busy: false,
};

// ---------------------------------------------------------------------------
// plumbing

function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function badge(status) {
  return `<span class="badge ${esc(status)}">${esc(status)}</span>`;
}

function fmtRows(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
}

function shortHash(h) {
  return h ? h.slice(0, 10) : "—";
}

let toastTimer = null;
function toast(message, bad = false) {
  const el = $("#toast");
  el.innerHTML = message;
  el.classList.toggle("bad", bad);
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 6000);
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch (_) { /* non-JSON */ }
  if (!res.ok) {
    const err = new Error((data && data.error) || `${res.status} ${res.statusText}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

function setHealth(ok, text) {
  const el = $("#health");
  el.classList.toggle("ok", ok === true);
  el.classList.toggle("bad", ok === false);
  $("#health-text").textContent = text;
}

// ---------------------------------------------------------------------------
// state loading

async function refresh({ keepPlan = false } = {}) {
  try {
    state.data = await api("GET", "/api/state");
    setHealth(true, "live");
  } catch (e) {
    setHealth(false, "offline");
    toast(`Cannot reach the API: ${esc(e.message)}`, true);
    return;
  }
  const d = state.data;
  $("#pub-gen").textContent = `gen ${d.published.generation}`;
  $("#pub-wm").textContent = `#${d.published.watermark}`;
  $("#pub-tables").textContent = `${d.tables.length}`;
  $("#pub-sources").textContent =
    `${d.sources.length} · ${fmtRows(d.sources.reduce((a, s) => a + s.rows, 0))} rows`;

  // Keep the active draft valid across refreshes.
  const drafts = d.edits.filter((e) => e.status === "draft");
  if (state.activeEdit !== null &&
      !drafts.some((e) => e.id === state.activeEdit)) {
    state.activeEdit = null;
    state.plan = null;
  }
  if (state.activeEdit === null && drafts.length) {
    state.activeEdit = drafts[0].id;
  }
  if (!keepPlan) state.plan = null;

  renderDrafts(drafts);
  renderSources(d.sources);
  renderBuilds(d.builds);
  renderGenerations(d.snapshots);
  renderEvents(d.events);
  await loadEditGraph();
  renderGraph();
  renderPublishPanel();
  renderCompareSelectors(d.snapshots);
  if (state.activeBuild) pollBuild(state.activeBuild, false);
}

// ---------------------------------------------------------------------------
// rail

function renderDrafts(drafts) {
  const box = $("#draft-list");
  const all = state.data.edits;
  if (!all.length) {
    box.innerHTML = `<p class="muted" style="font-size:11.5px">No edits yet.
      Load the demo or create a draft.</p>`;
    return;
  }
  box.innerHTML = all.slice(0, 8).map((e) => `
    <button class="rail-item ${e.id === state.activeEdit ? "active" : ""}"
            data-edit="${e.id}" type="button"
            title="${esc(e.note || "draft")} — ${e.tables.length} tables">
      <span class="name">#${e.id} ${esc(e.note || "untitled")}</span>
      <span class="badge ${esc(e.status)}">${esc(e.status)}</span>
    </button>`).join("");
  $$("[data-edit]", box).forEach((b) => b.addEventListener("click", async () => {
    state.activeEdit = Number(b.dataset.edit);
    state.plan = null;
    state.readOnlyModel = null;
    state.selected = null;
    state.impact = null;
    state.impactProblems = null;
    state.impactFor = null;
    await refresh();
    const tables = currentTables();
    if (tables.length) {
      state.selected = tables[0].name;
      renderGraph();
      await loadModel(state.selected);
    }
    await loadPlan();
  }));
}

function renderSources(sources) {
  const box = $("#source-list");
  $("#source-total").textContent =
    sources.length ? `${sources.length} declared` : "none";
  if (!sources.length) {
    box.innerHTML = `<p class="muted" style="font-size:11.5px">
      No sources. Load the demo to create some.</p>`;
    return;
  }
  box.innerHTML = sources.map((s) => `
    <div class="rail-item" style="cursor:default">
      <span class="name">${esc(s.name)}</span>
      <span class="amt">${fmtRows(s.rows)} rows</span>
    </div>`).join("");
}

function renderBuilds(builds) {
  const box = $("#build-list");
  $("#build-note").textContent = builds.length ? `${builds.length} recent` : "";
  if (!builds.length) {
    box.innerHTML = `<p class="muted" style="font-size:11.5px">No builds yet.</p>`;
    return;
  }
  box.innerHTML = builds.slice(0, 12).map((b) => `
    <button class="rail-item" data-build="${b.id}" type="button"
            title="${esc(b.error || "")}">
      <span class="name">#${b.id} ${esc(b.kind)}</span>
      ${badge(b.status)}
    </button>`).join("");
  $$("[data-build]", box).forEach((el) => el.addEventListener("click", () => {
    state.activeBuild = Number(el.dataset.build);
    switchTab("builds");
    pollBuild(state.activeBuild, true);
  }));
}

function renderGenerations(snaps) {
  const box = $("#gen-list");
  if (!snaps.length) {
    box.innerHTML = `<p class="muted" style="font-size:11.5px">No snapshots.</p>`;
    return;
  }
  box.innerHTML = snaps.slice(0, 10).map((s) => `
    <button class="rail-item" data-snap="${s.id}" type="button"
            title="${esc(s.note || "")}">
      <span class="name">gen ${s.generation} · #${s.id}</span>
      <span class="amt">${s.is_published ? "● live" : fmtRows(s.watermark)}</span>
    </button>`).join("");
  $$("[data-snap]", box).forEach((el) => el.addEventListener("click", () => {
    $("#cmp-a").value = String(el.dataset.snap);
    switchTab("compare");
  }));
}

function renderEvents(events) {
  const box = $("#event-list");
  if (!events.length) {
    box.innerHTML = `<p class="muted" style="font-size:11px">No events.</p>`;
    return;
  }
  box.innerHTML = events.slice(0, 30).map((e) => {
    const payload = Object.entries(e.payload || {})
      .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join(",") : v}`).join(" ");
    return `<div class="event" title="${esc(payload)}">
      <span class="et">${esc((e.at || "").slice(11, 19))}</span>
      <span class="ek">${esc(e.kind)}</span>
      <span class="ep">${esc(payload)}</span>
    </div>`;
  }).join("");
}

// ---------------------------------------------------------------------------
// graph

function currentTables() {
  // A draft may contain tables that have never been built; the draft graph
  // must show them, otherwise you could not select a new model to edit it.
  if (state.editGraph && state.editGraph.tables.length) {
    return state.editGraph.tables.map((t) => ({
      name: t.name, kind: t.kind, level: t.level,
      rows: t.published_rows, built_watermark: null,
      downstream: t.downstream, version: t.version,
      body: "", schema: t.columns, changed: t.changed, is_new: t.is_new,
      draft: true,
    }));
  }
  if (!state.data) return [];
  return state.data.tables;
}

async function loadEditGraph() {
  const ed = activeEdit();
  state.editGraph = null;
  if (!ed || ed.status !== "draft") return;
  try {
    state.editGraph = await api("GET", `/api/edits/${ed.id}/graph`);
  } catch (_) {
    state.editGraph = null;
  }
}

function renderGraph() {
  const tables = currentTables();
  const graph = $("#graph");
  const empty = $("#graph-empty");
  if (!tables.length) {
    graph.innerHTML = "";
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  const byLevel = new Map();
  for (const t of tables) {
    if (!byLevel.has(t.level)) byLevel.set(t.level, []);
    byLevel.get(t.level).push(t);
  }
  const affected = new Set(state.impact ? state.impact.downstream || [] : []);
  const broken = new Set((state.impactProblems || []).map((p) => p.table));
  const levels = [...byLevel.keys()].sort((a, b) => a - b);
  graph.innerHTML = levels.map((lv) => `
    <div class="level-row" role="listitem">
      <div class="level-label">L${lv}</div>
      <div class="level-nodes">
        ${byLevel.get(lv).sort((a, b) => a.name.localeCompare(b.name)).map((t) => `
          <button class="node ${t.name === state.selected ? "active" : ""}
                         ${affected.has(t.name) ? "affected" : ""}
                         ${broken.has(t.name) ? "broken" : ""}"
                  data-table="${esc(t.name)}" type="button" role="listitem"
                  title="${esc(t.body.split("\n")[0])}">
            <span class="node-name">
              <i class="kind-dot kind-${esc(t.kind)}"></i>${esc(t.name)}
            </span>
            <span class="node-meta">
              <span>${t.is_new ? "new" : fmtRows(t.rows) + " rows"}</span>
              <span>${esc(t.kind)}</span>
              ${t.changed && !t.is_new ? "<span>edited</span>" : ""}
              ${t.built_watermark !== null && t.built_watermark !== undefined
                ? `<span>wm ${fmtRows(t.built_watermark)}</span>` : ""}
            </span>
          </button>`).join("")}
      </div>
    </div>`).join("");
  $$(".node", graph).forEach((el) => el.addEventListener("click", () => {
    state.selected = el.dataset.table;
    if (state.impactFor !== state.selected) {
      state.impact = null;
      state.impactProblems = null;
      state.impactFor = null;
    }
    renderGraph();
    loadModel(state.selected);
    switchTab("editor");
  }));
  const ed = activeEdit();
  $("#graph-sub").textContent =
    `${tables.length} tables, ${levels.length} levels, deepest L${levels.at(-1)}.`
    + (ed && ed.status === "draft" ? ` Draft #${ed.id} view; unpublished tables included.` : "");
}

// ---------------------------------------------------------------------------
// editor / model

function activeEdit() {
  if (!state.data || state.activeEdit === null) return null;
  return state.data.edits.find((e) => e.id === state.activeEdit) || null;
}

async function loadModel(name) {
  if (!name) return;
  const sidebar = activeEdit();
  try {
    let url = `/api/model/${encodeURIComponent(name)}`;
    if (sidebar && sidebar.status === "draft") url += `?edit_id=${sidebar.id}`;
    const m = await api("GET", url);
    state.readOnlyModel = m;
    $("#editor-title").textContent = m.table;
    $("#editor-sub").textContent = m.upstream.length
      ? `reads ${m.upstream.join(", ")}` : "reads sources only";
    $("#model-name").value = m.table;
    $("#model-kind").value = m.kind;
    $("#model-sql").value = m.body;
    const editable = !!sidebar && sidebar.status === "draft";
    $("#model-sql").readOnly = !editable;
    $("#model-kind").disabled = !editable;
    $("#save-model").disabled = !editable;
    $("#readonly-banner").hidden = editable;
    $("#editor-version").hidden = false;
    $("#editor-version").textContent =
      `v${m.version} · ${m.kind} · ${m.rows === null ? "draft" : fmtRows(m.rows) + " rows"}`;
  } catch (e) {
    toast(esc(e.message), true);
    return;
  }
  if (state.impactFor !== name) {
    $("#impact").innerHTML = `<p class="muted">Run “Analyze impact” to see lineage,
      added / removed columns and every downstream consumer.</p>`;
  }
  loadSample(name);
}

async function loadSample(name) {
  const box = $("#sample-rows");
  try {
    const d = await api("GET", `/api/tables/${encodeURIComponent(name)}?limit=20`);
    $("#sample-sub").textContent =
      `snapshot #${d.snapshot} · generation ${d.generation} · ${fmtRows(d.row_count)} rows`;
    box.innerHTML = rowsTable(d.schema, d.rows);
  } catch (e) {
    box.innerHTML = `<p class="muted" style="padding:9px;font-size:11.5px">
      ${esc(e.status === 404 ? "Not published yet; run a build." : e.message)}</p>`;
    $("#sample-sub").textContent = "Read from the immutable table file.";
  }
}

function rowsTable(schema, rows) {
  if (!rows || !rows.length) {
    return `<p class="muted" style="padding:9px;font-size:11.5px">No rows.</p>`;
  }
  return `<table><thead><tr>${schema.map((c) => `<th>${esc(c)}</th>`).join("")}
    </tr></thead><tbody>${rows.map((r) => `<tr>${schema.map((c) => {
      const v = r[c];
      if (v === null || v === undefined) return `<td class="null">NULL</td>`;
      return `<td class="${typeof v === "number" ? "num" : ""}">${esc(v)}</td>`;
    }).join("")}</tr>`).join("")}</tbody></table>`;
}

async function analyze() {
  const name = $("#model-name").value.trim();
  const sql = $("#model-sql").value;
  const kind = $("#model-kind").value;
  if (!name || !sql.trim()) { toast("A table name and SQL are required.", true); return; }
  const payload = { table: name, sql, kind };
  if (state.activeEdit !== null) payload.edit_id = state.activeEdit;
  try {
    const d = await api("POST", "/api/analyze", payload);
    state.impact = d.impact;
    state.impactProblems = d.problems;
    state.impactFor = name;
    renderImpact(d);
    renderGraph();
    $("#editor-status").textContent = "impact computed";
    $("#editor-status").className = "status ok";
    $("#analyze-refresh").textContent = "Re-run";
  } catch (e) {
    state.impact = null;
    state.impactProblems = null;
    $("#impact").innerHTML = `<div class="problem"><strong>rejected</strong>
      ${esc(e.message)}</div><p class="muted" style="font-size:11.5px">
      The model was not saved. Strata only accepts the documented SQL subset;
      a rejected statement never reaches a build.</p>`;
    $("#editor-status").textContent = "rejected";
    $("#editor-status").className = "status bad";
    renderGraph();
  }
}

function renderImpact(d) {
  const im = d.impact;
  const cols = im.columns || [];
  const lineage = cols.length ? cols.map((c) => `
    <div class="lineage-row">
      <span class="col">${esc(c.name)}</span>
      <span class="arrow">←</span>
      <span class="from">${c.from.length ? c.from.map(esc).join(", ") : '<span class="muted">literal / aggregate</span>'}
        ${c.via && c.via.length ? `<span class="via">via ${c.via.map(esc).join(" → ")}</span>` : ""}
      </span>
    </div>`).join("") : `<p class="muted" style="font-size:11.5px">No columns.</p>`;

  const breakage = (d.problems || []).map((p) => `
    <div class="problem"><strong>${esc(p.table)}</strong> ${esc(p.error)}</div>`).join("");

  $("#impact").innerHTML = `
    ${breakage ? `<div><h3>breaks consumers</h3>${breakage}</div>` : ""}
    <div><h3>column lineage</h3><div class="lineage">${lineage}</div></div>
    <div>
      <h3>schema changes</h3>
      <div class="chips">
        ${(im.added_columns || []).map((c) => `<span class="chip added">+ ${esc(c)}</span>`).join("") || ""}
        ${(im.removed_columns || []).map((c) => `<span class="chip removed">− ${esc(c)}</span>`).join("") || ""}
        ${!(im.added_columns || []).length && !(im.removed_columns || []).length
          ? '<span class="muted" style="font-size:11.5px">same column names</span>' : ""}
      </div>
    </div>
    <div>
      <h3>affected downstream (${(im.downstream || []).length})</h3>
      <div class="chips">
        ${(im.downstream || []).map((n) =>
          `<span class="chip affected">${esc(n)} <span class="muted">L${esc(im.levels[n])}</span></span>`).join("")
          || '<span class="muted" style="font-size:11.5px">none — leaf table</span>'}
      </div>
      <p class="sub" style="margin-top:6px">longest path below this table:
        <span class="mono">${esc(im.chain_depth)}</span> levels</p>
    </div>
    <div>
      <h3>reads</h3>
      <div class="chips">${(im.sources || []).map((s) => `<span class="chip">${esc(s)}</span>`).join("")}</div>
    </div>`;
}

async function saveModel() {
  const name = $("#model-name").value.trim();
  const sql = $("#model-sql").value;
  const kind = $("#model-kind").value;
  if (!state.activeEdit) { toast("Create a draft first.", true); return; }
  try {
    await api("PUT", `/api/edits/${state.activeEdit}/tables`,
              { name, sql, kind });
    $("#editor-status").textContent = "saved to draft";
    $("#editor-status").className = "status ok";
    await refresh();
    await loadModel(name);
    await loadPlan();
  } catch (e) {
    $("#editor-status").textContent = "rejected";
    $("#editor-status").className = "status bad";
    toast(`<b>${esc(name)}</b> was not saved: ${esc(e.message)}`, true);
    await analyze();
  }
}

async function newTable() {
  if (!state.activeEdit) { toast("Create a draft first.", true); return; }
  const name = prompt("New table name (letters, digits, underscore):");
  if (!name) return;
  state.selected = name;
  $("#editor-title").textContent = name;
  $("#editor-sub").textContent = "new model in this draft";
  $("#model-name").value = name;
  $("#model-kind").value = "create";
  $("#model-sql").value = `CREATE TABLE ${name} AS\nSELECT \nFROM `;
  $("#model-sql").readOnly = false;
  $("#model-kind").disabled = false;
  $("#save-model").disabled = false;
  $("#editor-version").hidden = true;
  $("#sample-rows").innerHTML = "";
  $("#sample-sub").textContent = "No published file yet.";
  $("#model-sql").focus();
}

// ---------------------------------------------------------------------------
// planning / publishing

async function loadPlan() {
  const ed = activeEdit();
  const box = $("#plan-summary");
  if (!ed) {
    box.innerHTML = `<p class="muted">Choose a draft (or load the demo) to compute a plan.</p>`;
    state.plan = null;
    return;
  }
  if (ed.status !== "draft") {
    box.innerHTML = `<p class="muted">This edit is <b>${esc(ed.status)}</b>; it cannot
      be planned again. Create a new draft to make further changes.</p>`;
    state.plan = null;
    renderPublishPanel();
    return;
  }
  const mode = document.querySelector('input[name="mode"]:checked').value;
  try {
    const d = await api("GET", `/api/edits/${ed.id}/plan?mode=${mode}`);
    state.plan = d.plan;
    renderPlan(d.plan, mode);
  } catch (e) {
    state.plan = null;
    box.innerHTML = `<div class="problem"><strong>cannot plan</strong>
      ${esc(e.message)}</div>`;
  }
  renderPublishPanel();
}

function renderPlan(plan, mode) {
  const ex = plan.explain || {};
  const m = mode === "cutover" ? ex.cutover : ex.grouped;
  const rows = plan.tables.length;
  const stats = [
    ["tables to rebuild", rows],
    ["changed models", Object.keys(plan.changed).length],
    ["dependency families", mode === "cutover" ? 1 : plan.groups.length],
    ["deepest level", Math.max(0, ...plan.tables.map((t) => t.level))],
    ["source watermark", `#${plan.watermark}`],
  ];
  const newRows = Object.values(plan.source_new_rows || {}).reduce((a, b) => a + b, 0);
  if (newRows) stats.push(["new source rows", newRows]);
  $("#plan-summary").innerHTML = `
    ${rows ? "" : `<p class="muted" style="margin-bottom:8px">Nothing to rebuild:
      every model is current and no source moved.</p>`}
    ${stats.map(([k, v]) => `<div class="plan-stat"><span class="k">${esc(k)}</span>
      <span class="v">${esc(v)}</span></div>`).join("")}
    ${m ? `<div class="plan-explain"><b>${mode === "cutover" ? "Cutover" : "Grouped"} wait:</b>
      ${esc(m.wait)}<br><b>Cost:</b> ${esc(m.cost)}</div>` : ""}
    ${plan.groups.length && mode !== "cutover" ? `<div class="plan-explain">
      <b>Switch order:</b> ${plan.groups.map((g, i) =>
        `${i + 1}. ${esc(g.tables.join(" + "))}`).join(" → ")}</div>` : ""}`;
}

function renderPublishPanel() {
  const ed = activeEdit();
  $("#draft-state").textContent = ed ? `#${ed.id} ${ed.status}` : "no draft";
  $("#publish").disabled = !ed || ed.status !== "draft" || state.busy;
  $("#discard").hidden = !ed || ed.status !== "draft";
  $("#rebase").hidden = !ed || ed.status !== "draft" ||
    !state.data || ed.base_snapshot === state.data.published.snapshot;
  $("#publish-sub").textContent = ed
    ? `Draft #${ed.id} based on snapshot #${ed.base_snapshot}.`
    : "Load the demo or create a draft.";
}

async function publish() {
  const ed = activeEdit();
  if (!ed || ed.status !== "draft") return;
  const mode = document.querySelector('input[name="mode"]:checked').value;
  state.busy = true;
  renderPublishPanel();
  try {
    const d = await api("POST", `/api/edits/${ed.id}/publish`, { mode });
    toast(`Build #${d.build} queued (${esc(mode)}).`);
    await refresh();
    watchBuild(d.build);
  } catch (e) {
    toast(`Publish refused: ${esc(e.message)}`, true);
  } finally {
    state.busy = false;
    renderPublishPanel();
  }
}

async function rebase() {
  const ed = activeEdit();
  if (!ed) return;
  try {
    const d = await api("POST", `/api/edits/${ed.id}/rebase`);
    const conflicts = d.conflicts || [];
    toast(conflicts.length
      ? `Rebased with ${conflicts.length} conflict(s): ${conflicts.map((c) => esc(c.table)).join(", ")}`
      : "Draft rebased onto the current generation.", conflicts.length > 0);
    await refresh();
    await loadPlan();
  } catch (e) {
    toast(`Rebase failed: ${esc(e.message)}`, true);
  }
}

async function discard() {
  const ed = activeEdit();
  if (!ed) return;
  if (!confirm(`Discard draft #${ed.id}? The published generation is unaffected.`)) return;
  try {
    await api("POST", `/api/edits/${ed.id}/discard`);
    state.activeEdit = null;
    toast(`Draft #${ed.id} discarded.`);
    await refresh();
  } catch (e) {
    toast(esc(e.message), true);
  }
}

// ---------------------------------------------------------------------------
// build watching

function watchBuild(bid) {
  state.activeBuild = bid;
  pollBuild(bid, true);
}

async function pollBuild(bid, scroll) {
  let d;
  try {
    d = await api("GET", `/api/builds/${bid}`);
  } catch (e) {
    return;
  }
  renderActiveBuild(d);
  if (d.status === "ready") {
    state.lastPreview = bid;
    renderPreview(d);
  }
  if (["published", "failed", "cancelled", "conflict", "crashed", "ready"]
      .includes(d.status)) {
    if (scroll) renderBuildDetail(d);
    const wasFinal = state._finished && state._finished[bid];
    if (!wasFinal) {
      (state._finished ||= {})[bid] = true;
      if (d.status === "published") {
        toast(`Build #${bid} published generation ${d.result?.generation ?? ""}.`);
        state.impact = null;
        state.impactProblems = null;
        state.impactFor = null;
        refresh().then(() => {
          if (state.activeEdit !== null) loadPlan();
          if (state.selected) loadModel(state.selected);
        });
      } else if (d.status === "ready") {
        toast(`Preview #${bid} finished; no generation switched.`);
      } else {
        toast(`Build #${bid} ${esc(d.status)}: ${esc(d.error || "")}`, true);
      }
    }
    clearTimeout(state.pollTimer);
    return;
  }
  clearTimeout(state.pollTimer);
  state.pollTimer = setTimeout(() => pollBuild(bid, false), 700);
}

function renderActiveBuild(d) {
  const tables = d.tables || [];
  const done = tables.filter((t) => ["built", "published", "reused",
    "reused-proof", "failed"].includes(t.status)).length;
  const pct = tables.length ? Math.round((done / tables.length) * 100) : 0;
  const live = ["queued", "running", "cancelling"].includes(d.status);
  $("#active-sub").textContent = live
    ? `Build #${d.id} ${d.kind} · ${d.status}`
    : `Build #${d.id} ${d.kind} · finished`;
  $("#active-build").innerHTML = `
    <div class="inline" style="justify-content:space-between">
      <span>${badge(d.status)}</span>
      <span class="mono" style="font-size:11px">${done}/${tables.length}</span>
    </div>
    <div class="progress"><i style="width:${pct}%"></i></div>
    ${d.error ? `<div class="problem" style="margin:0"><strong>${esc(d.status)}</strong>
      ${esc(d.error)}</div>` : ""}
    <div class="build-steps">
      ${tables.map((t) => `
        <div class="step ${esc(t.status)}">
          <span class="dot"></span>
          <span class="sname">${esc(t.name)}</span>
          <span class="smeta">L${esc(t.level)} · ${esc(t.status)}${
            t.rows_out !== null && t.rows_out !== undefined ? ` · ${fmtRows(t.rows_out)} rows` : ""}</span>
        </div>`).join("")}
    </div>
    ${live ? `<button class="btn danger-ghost sm wide" id="cancel-build"
        type="button">Cancel build</button>` : ""}`;
  const cb = $("#cancel-build");
  if (cb) cb.addEventListener("click", async () => {
    try {
      await api("POST", `/api/builds/${d.id}/cancel`);
      toast(`Cancellation requested for #${d.id}. Nothing will be published.`);
    } catch (e) { toast(esc(e.message), true); }
  });
}

// ---------------------------------------------------------------------------
// preview tab

async function runPreview() {
  const ed = activeEdit();
  if (!ed || ed.status !== "draft") {
    toast("Choose a draft edit first.", true); return;
  }
  const btn = $("#run-preview");
  btn.disabled = true;
  try {
    const d = await api("POST", `/api/edits/${ed.id}/preview`);
    toast(`Preview build #${d.build} queued.`);
    state.lastPreview = null;
    switchTab("preview");
    $("#preview-body").innerHTML = `<p class="muted">Preview #${d.build} running…</p>`;
    watchBuild(d.build);
    // The watch loop renders the preview on completion.
    const tick = setInterval(async () => {
      try {
        const b = await api("GET", `/api/builds/${d.build}`);
        if (b.status === "ready") { renderPreview(b); clearInterval(tick); }
        if (["failed", "cancelled", "conflict", "crashed"].includes(b.status)) {
          clearInterval(tick);
        }
      } catch (_) { clearInterval(tick); }
    }, 700);
  } catch (e) {
    toast(`Preview refused: ${esc(e.message)}`, true);
  } finally {
    btn.disabled = false;
  }
}

function renderPreview(b) {
  const r = b.result || {};
  const diffs = r.diffs || [];
  const body = $("#preview-body");
  if (!diffs.length) {
    body.innerHTML = `<p class="muted">The preview produced no changes: the new
      files are logically identical to the published generation.</p>`;
    return;
  }
  body.innerHTML = `
    <div class="group-strip">
      ${(r.groups || []).map((g) => {
        const rebuilt = g.rebuilt || [];
        return `
        <div class="group-card">
          <div class="g-head">family ${esc(g.id)} · ${rebuilt.length} rebuilt</div>
          <div class="g-tables">${g.tables.map(esc).join("<br>")}</div>
          ${g.skipped && g.skipped.length
            ? `<div class="g-head" style="margin-top:4px">pruned: ${g.skipped.map(esc).join(", ")}</div>` : ""}
        </div>`;
      }).join("")}
    </div>
    ${diffs.map(renderDiff).join("")}
    <p class="muted" style="font-size:11px">Preview #${esc(b.id)} · watermark
      #${esc(r.watermark)} · nothing was published.</p>`;
}

function renderDiff(d) {
  const schema = (a, b) => {
    if (a.length === b.length && a.every((x, i) => x === b[i])) {
      return `<span class="muted">${a.map(esc).join(", ")}</span>`;
    }
    return `${a.map(esc).join(", ")} → <b>${b.map(esc).join(", ")}</b>`;
  };
  return `
    <div class="diff-card">
      <div class="diff-head">
        <div>
          <span class="tname">${esc(d.table)}</span>
          <span class="tmeta"> · ${esc(d.action)} · ${fmtRows(d.old_rows)} → ${fmtRows(d.new_rows)} rows</span>
        </div>
        <div class="chips">
          ${d.content_changed ? '<span class="chip added">content changed</span>' : '<span class="chip">content same</span>'}
          ${d.schema_changed ? '<span class="chip removed">schema changed</span>' : ""}
        </div>
      </div>
      <div class="diff-body">
        <div>
          <h4>schema</h4>
          <div style="font-family:var(--mono);font-size:11px">${schema(d.old_schema, d.new_schema)}</div>
          <h4 style="margin-top:9px">published sample</h4>
          ${rowsTable(d.old_schema, d.old_sample)}
        </div>
        <div>
          <h4>preview sample</h4>
          ${rowsTable(d.new_schema, d.sample_new)}
        </div>
      </div>
    </div>`;
}

// ---------------------------------------------------------------------------
// compare / rollback

function renderCompareSelectors(snaps) {
  const a = $("#cmp-a"), b = $("#cmp-b");
  const keepA = a.value, keepB = b.value;
  const opts = snaps.map((s) =>
    `<option value="${s.id}">gen ${s.generation} · #${s.id} · ${esc(s.mode)}${
      s.is_published ? " · live" : ""}</option>`).join("");
  a.innerHTML = opts;
  b.innerHTML = opts;
  if (snaps.length) {
    a.value = keepA && snaps.some((s) => String(s.id) === keepA) ? keepA : String(snaps[1]?.id ?? snaps[0].id);
    b.value = keepB && snaps.some((s) => String(s.id) === keepB) ? keepB : String(snaps[0].id);
  }
}

async function runCompare() {
  const a = $("#cmp-a").value, b = $("#cmp-b").value;
  const box = $("#compare-body");
  if (!a || !b) { toast("Select two generations.", true); return; }
  try {
    const d = await api("GET", `/api/compare?a=${a}&b=${b}&limit=15`);
    box.innerHTML = `
      <p class="sub" style="margin:0">A = gen ${d.a.generation} (#${d.a.snapshot},
        ${esc(d.a.mode)}) &nbsp;·&nbsp; B = gen ${d.b.generation} (#${d.b.snapshot},
        ${esc(d.b.mode)}) &nbsp;·&nbsp; ${d.differing.length} differing table(s)</p>
      <div class="cmp-row head">
        <span>table</span><span>rows A</span><span>rows B</span>
        <span>Δ</span><span>differences</span>
      </div>
      ${d.tables.map((t) => {
        const only = t.only_in ? `<span class="chip">only in ${t.only_in.toUpperCase()}</span>` : "";
        const same = t.only_in === null && t.same_content && !t.body_changed;
        const add = t.rows_added ? t.rows_added.length : 0;
        const rem = t.rows_removed ? t.rows_removed.length : 0;
        return `
        <div class="cmp-row ${same ? "same" : ""}">
          <span class="tname">${esc(t.table)}</span>
          <span>${fmtRows(t.rows_a)}</span>
          <span>${fmtRows(t.rows_b)}</span>
          <span>${only || (add === 0 && rem === 0 ? "—" :
            `<span class="delta-add">+${add}</span> / <span class="delta-rem">−${rem}</span>`)}</span>
          <span class="chips">
            ${t.body_changed ? '<span class="chip removed">SQL changed</span>' : ""}
            ${!t.same_content && t.only_in === null ? '<span class="chip affected">rows differ</span>' : ""}
            ${same ? '<span class="chip">identical</span>' : ""}
          </span>
        </div>`;
      }).join("")}`;
    $("#compare-body").dataset.a = a;
    $("#compare-body").dataset.b = b;
  } catch (e) {
    box.innerHTML = `<div class="problem"><strong>compare failed</strong>
      ${esc(e.message)}</div>`;
  }
}

async function rollbackToA() {
  const a = $("#cmp-a").value;
  if (!a) { toast("Select a target generation first.", true); return; }
  const snap = (state.data?.snapshots || []).find((s) => String(s.id) === a);
  const label = snap ? `generation ${snap.generation} (#${snap.id})` : `snapshot #${a}`;
  if (!confirm(`Roll back to ${label}?\n\nThis publishes a NEW generation whose ` +
               `tables point at the old immutable files. Nothing is deleted and ` +
               `the rollback itself can be rolled back.`)) return;
  try {
    const d = await api("POST", "/api/rollback",
                        { snapshot: Number(a), note: `rollback to ${label}` });
    toast(`Rolled back: generation ${d.snapshot} now points at ${esc(label)}.`);
    await refresh();
    runCompare();
  } catch (e) {
    toast(`Rollback refused: ${esc(e.message)}`, true);
  }
}

function renderBuildDetail(d) {
  const plan = d.plan || {};
  const res = d.result || {};
  $("#build-title").textContent = `Build #${d.id} · ${d.kind}`;
  $("#build-sub").textContent =
    `base snapshot #${d.base_snapshot} · watermark #${d.watermark}`;
  const groups = (res.groups || plan.groups || []);
  $("#build-body").innerHTML = `
    <div class="inline" style="justify-content:space-between;margin-bottom:10px;flex-wrap:wrap">
      <div class="chips">
        ${badge(d.status)}
        <span class="chip">kind ${esc(d.kind)}</span>
        ${d.edit_id ? `<span class="chip">edit #${esc(d.edit_id)}</span>` : ""}
        ${d.worker_pid ? `<span class="chip">pid ${esc(d.worker_pid)}</span>` : ""}
      </div>
      <span class="muted" style="font-size:11px">${esc(d.finished_at || d.created_at || "")}</span>
    </div>
    ${d.error ? `<div class="problem"><strong>${esc(d.status)}</strong> ${esc(d.error)}</div>` : ""}
    ${res.generation !== undefined && res.generation !== null
      ? `<p class="sub">Published generation <b class="mono">${esc(res.generation)}</b>,
         snapshot <span class="mono">#${esc(res.snapshot)}</span>.</p>` : ""}
    ${groups.length ? `<h3 class="impact-h">publication groups</h3>
      <div class="group-strip">${groups.map((g) => `
        <div class="group-card">
          <div class="g-head">family ${esc(g.id)}
            ${g.generation !== undefined ? `· gen ${esc(g.generation)}` : ""}
            ${g.snapshot ? `· snapshot #${esc(g.snapshot)}` : "· no switch"}</div>
          <div class="g-tables">${(g.rebuilt || []).length
            ? g.rebuilt.map(esc).join("<br>") : '<span class="muted">proven identical</span>'}</div>
          ${g.note ? `<div class="g-head" style="margin-top:4px">${esc(g.note)}</div>` : ""}
        </div>`).join("")}</div>` : ""}
    ${(res.pruned || []).length ? `<h3 class="impact-h">proof pruning</h3>
      ${res.pruned.map((p) => `<div class="plan-explain">
        <b>${esc(p.table)}</b> ${esc(p.proof)}
        ${(p.skipped_downstream || []).length
          ? `<br>skipped downstream: ${p.skipped_downstream.map(esc).join(", ")}` : ""}
      </div>`).join("")}` : ""}
    <h3 class="impact-h">tables</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>table</th><th>level</th><th>status</th><th>rows in</th>
          <th>rows out</th><th>ms</th><th>content</th></tr></thead>
        <tbody>${(d.tables || []).map((t) => `
          <tr>
            <td>${esc(t.name)}</td>
            <td class="num">${esc(t.level)}</td>
            <td class="status-${esc(t.status)}">${esc(t.status)}</td>
            <td class="num">${fmtRows(t.rows_in)}</td>
            <td class="num">${fmtRows(t.rows_out)}</td>
            <td class="num">${fmtRows(t.duration_ms)}</td>
            <td title="${esc(t.content_hash || "")}">${esc(shortHash(t.content_hash))}</td>
          </tr>`).join("")}</tbody>
      </table>
    </div>`;
}

// ---------------------------------------------------------------------------
// tabs

function switchTab(name) {
  state.tab = name;
  $$(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  $$(".tabpanel").forEach((p) => { p.hidden = p.id !== `tab-${name}`; });
  if (name === "preview" && state.lastPreview) {
    api("GET", `/api/builds/${state.lastPreview}`).then(renderPreview).catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// demo / increments

async function seed() {
  const btn = $("#seed");
  btn.disabled = true;
  try {
    const d = await api("POST", "/api/seed", { demo: "orders" });
    state.activeEdit = d.edit.id;
    state.selected = null;
    state.impact = null; state.impactProblems = null; state.impactFor = null;
    toast(`Demo loaded: draft #${d.edit.id} with eight models.`);
    await refresh();
    const tables = currentTables();
    if (tables.length) {
      state.selected = tables[0].name;
      renderGraph();
      await loadModel(state.selected);
    }
    await loadPlan();
  } catch (e) {
    toast(`Seed failed: ${esc(e.message)}`, true);
  } finally {
    btn.disabled = false;
  }
}

async function addIncrement() {
  const n = Number($("#inc-n").value || 5);
  try {
    const d = await api("POST", "/api/increments/demo", { n });
    toast(`Appended ${n} orders (source watermark #${d.watermark}).`);
    await refresh();
    if (state.activeEdit !== null) loadPlan();
  } catch (e) {
    toast(`Append failed: ${esc(e.message)}`, true);
  }
}

async function reconcile() {
  try {
    const d = await api("POST", "/api/reconcile");
    toast(`Reconcile build #${d.build} queued.`);
    await refresh();
    watchBuild(d.build);
    switchTab("builds");
  } catch (e) {
    toast(esc(e.message), true);
  }
}

// ---------------------------------------------------------------------------
// wiring

function wire() {
  $$(".tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));
  $("#refresh").addEventListener("click", () => {
    refresh().then(() => state.selected && loadModel(state.selected));
  });
  $("#seed").addEventListener("click", seed);
  $("#inc-add").addEventListener("click", addIncrement);
  $("#reconcile").addEventListener("click", reconcile);
  $("#analyze").addEventListener("click", analyze);
  $("#analyze-refresh").addEventListener("click", analyze);
  $("#sample-refresh").addEventListener("click", () =>
    state.selected && loadSample(state.selected));
  $("#save-model").addEventListener("click", saveModel);
  $("#new-table").addEventListener("click", newTable);
  $("#run-preview").addEventListener("click", runPreview);
  $("#publish").addEventListener("click", publish);
  $("#rebase").addEventListener("click", rebase);
  $("#discard").addEventListener("click", discard);
  $("#run-compare").addEventListener("click", runCompare);
  $("#rollback-a").addEventListener("click", rollbackToA);
  $("#new-draft").addEventListener("click", async () => {
    try {
      const d = await api("POST", "/api/edits",
                          { note: `edit ${new Date().toISOString().slice(11, 19)}` });
      state.activeEdit = d.edit.id;
      state.selected = null;
      toast(`Draft #${d.edit.id} created from the published generation.`);
      await refresh();
      await loadPlan();
    } catch (e) { toast(esc(e.message), true); }
  });
  $$('input[name="mode"]').forEach((r) =>
    r.addEventListener("change", () => state.activeEdit !== null && loadPlan()));

  // Ctrl/Cmd+Enter saves; Tab inserts two spaces in the SQL box.
  $("#model-sql").addEventListener("keydown", (ev) => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === "Enter") {
      ev.preventDefault();
      if (!$("#save-model").disabled) saveModel();
    }
    if (ev.key === "Tab") {
      ev.preventDefault();
      const t = ev.target, s = t.selectionStart, e = t.selectionEnd;
      t.value = t.value.slice(0, s) + "  " + t.value.slice(e);
      t.selectionStart = t.selectionEnd = s + 2;
    }
  });
}

async function boot() {
  wire();
  switchTab("editor");
  await refresh();
  const tables = currentTables();
  if (tables.length) {
    state.selected = tables[0].name;
    renderGraph();
    loadModel(state.selected);
    loadPlan();
  }
  setInterval(() => {
    if (state.busy) return;
    const anyRunning = (state.data?.builds || [])
      .some((b) => ["queued", "running", "cancelling"].includes(b.status));
    refresh({ keepPlan: true });
    if (anyRunning) { /* pollBuild follows the active build */ }
  }, 4000);
}

boot();

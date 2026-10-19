<script lang="ts">
  import { onMount, tick } from 'svelte';
  import './style.css';

  type Dataset = { id: string; name: string; file: string; rowCount: number | null; byteSize: number | null; schema: { name: string; type: string }[] | null };
  type Job = {
    id: string; status: string; datasetId: string; sql: string; submittedAt: string;
    durationMs: number | null; failure: string | null; outputFile: string | null;
    exitReason: string | null; engine: string; mode: string; image: string;
    result: { rowCount: number; columns: { name: string; type: string }[] } | null;
  };
  let datasets: Dataset[] = $state([]);
  let jobs: Job[] = $state([]);
  let selected: Job | null = $state(null);
  let detailDialog: HTMLDialogElement | null = $state(null);
  let logs = $state('');
  let name = $state('Sample events');
  let file = $state('sample.parquet');
  let datasetId = $state('');
  let sql = $state('SELECT category, COUNT(*) AS count, SUM(amount) AS total FROM input GROUP BY category ORDER BY category');
  let error = $state('');
  let connectionError = $state('');
  let busy = $state(false);

  async function api<T>(url: string, options?: RequestInit): Promise<T> {
    const response = await fetch(url, options);
    const data = await response.json();
    if (!response.ok) throw new Error(data.error ?? `Request failed: ${response.status}`);
    return data as T;
  }
  const json = (value: unknown): RequestInit => ({
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value)
  });
  async function refresh() {
    try {
      [datasets, jobs] = await Promise.all([api<Dataset[]>('/api/datasets'), api<Job[]>('/api/jobs')]);
      if (!datasetId && datasets.length) datasetId = datasets[0].id;
      if (selected) {
        const selectedId = selected.id;
        selected = jobs.find(job => job.id === selectedId) ?? null;
        if (selected) {
          const response = await fetch(`/api/jobs/${selectedId}/logs`);
          if (!response.ok) throw new Error(`Could not load logs: ${response.status}`);
          const updatedLogs = await response.text();
          if (selected?.id === selectedId) logs = updatedLogs;
        }
      }
      connectionError = '';
    } catch (cause) { connectionError = String(cause); }
  }
  async function action(work: () => Promise<void>) {
    busy = true; error = '';
    try { await work(); await refresh(); } catch (cause) { error = String(cause); }
    finally { busy = false; }
  }
  async function register() {
    await action(async () => {
      const dataset = await api<Dataset>('/api/datasets', json({ name, file }));
      datasetId = dataset.id;
    });
  }
  async function submit() {
    await action(async () => {
      const job = await api<Job>('/api/jobs', json({ datasetId, sql, engine: 'duckdb', mode: 'cpu' }));
      await openDetail(job);
    });
  }
  async function cancel(id: string) {
    await action(async () => { await api(`/api/jobs/${id}/cancel`, { method: 'POST' }); });
  }
  async function openDetail(job: Job) {
    selected = job;
    logs = 'Loading worker logs...';
    await tick();
    if (detailDialog && !detailDialog.open) detailDialog.showModal();
    await refresh();
  }
  function closeDetail() { detailDialog?.close(); }
  function datasetName(id: string): string { return datasets.find(dataset => dataset.id === id)?.name ?? 'Unknown dataset'; }
  function duration(ms: number | null): string {
    return ms === null ? 'Pending' : ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
  }
  onMount(() => {
    refresh();
    const interval = setInterval(refresh, 2000);
    return () => clearInterval(interval);
  });
</script>

<svelte:head><meta name="description" content="Local-first analytical jobs" /></svelte:head>
<header><div class="brand">LAKE<span>FORGE</span></div><div class="subtitle">Local-first compute workbench <span class="pill">DuckDB / CPU</span></div></header>
<main>
  <section class="intro"><p class="eyebrow">WORKSPACE / OVERVIEW</p><h1>Explore data.<br /><em>Watch the work.</em></h1><p>Run an isolated analytical query against registered Parquet data. Every execution has its own container and a durable history.</p></section>
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if connectionError}<p class="error" role="alert">{connectionError}</p>{/if}
  <div class="workspace-grid">
    <section class="panel"><div class="heading"><span>01 / DATASETS</span><small>{datasets.length} registered</small></div><h2>Register a Parquet file</h2><p class="hint">Place a file in <code>data/inputs</code>, then register its filename here.</p>
      <form onsubmit={(event) => { event.preventDefault(); register(); }}>
        <label>Dataset name<input bind:value={name} required /></label><label>Filename<input bind:value={file} required placeholder="sample.parquet" /></label><button disabled={busy}>Register dataset</button>
      </form>
      {#if datasets.length}<div class="dataset-list">{#each datasets as dataset}<div class="dataset"><strong>{dataset.name}</strong><code>{dataset.file}</code><small>{dataset.rowCount === null ? 'Schema not inspected' : `${dataset.rowCount} rows · ${dataset.byteSize} bytes`}</small>{#if dataset.schema}<small>{dataset.schema.map(column => `${column.name}: ${column.type}`).join(' · ')}</small>{/if}</div>{/each}</div>{/if}
    </section>
    <section class="panel"><div class="heading"><span>02 / QUERY</span><small>One container per job</small></div><h2>Submit a query</h2>
      <form onsubmit={(event) => { event.preventDefault(); submit(); }}>
        <label>Input dataset<select bind:value={datasetId} required>{#each datasets as dataset}<option value={dataset.id}>{dataset.name}</option>{/each}</select></label>
        <label>SQL / SELECT only<textarea bind:value={sql} rows="6" required spellcheck="false"></textarea></label>
        <button disabled={busy || !datasetId}>Run on DuckDB <span>↗</span></button>
      </form>
    </section>
  </div>
  <section class="history-section">
    <div class="history-heading"><div><p class="eyebrow">03 / EXECUTIONS</p><h2>Execution history</h2><p class="hint">Select a run to inspect its query, output, and worker logs.</p></div><span class="history-count">{jobs.length} runs</span></div>
    {#if jobs.length}
      <div class="history-list">
        <div class="history-labels" aria-hidden="true"><span>QUERY / DATASET</span><span>STATUS</span><span>SUBMITTED</span><span>DURATION</span></div>
        {#each jobs as job (job.id)}
          <button class="history-row" onclick={() => openDetail(job)} aria-label={`Inspect ${job.status.toLowerCase()} job ${job.id}`}>
            <span class="history-query"><strong>{job.sql}</strong><small>{datasetName(job.datasetId)} · {job.id.slice(0, 8)}</small></span>
            <span class="status" data-status={job.status}>{job.status}</span>
            <span class="history-date">{new Date(job.submittedAt).toLocaleString()}</span>
            <span class="history-duration">{duration(job.durationMs)} <span aria-hidden="true">↗</span></span>
          </button>
        {/each}
      </div>
    {:else}
      <p class="history-empty">No executions yet. Register a dataset and run your first query.</p>
    {/if}
  </section>
</main>
{#if selected}
  <dialog bind:this={detailDialog} class="detail-dialog" aria-labelledby="detail-title" onclose={() => { selected = null; logs = ''; }}>
    <div class="detail-top"><span class="eyebrow">EXECUTION / INSPECTOR</span><button type="button" class="close-button" aria-label="Close job details" onclick={closeDetail}>Close <span aria-hidden="true">×</span></button></div>
    <div class="detail-body">
      <span class="status" data-status={selected.status}>{selected.status}</span>
      <h2 id="detail-title">Execution detail</h2>
      <p class="detail-id">{selected.id}</p>
      <div class="detail-metrics"><div><small>DURATION</small><strong>{duration(selected.durationMs)}</strong></div><div><small>OUTPUT ROWS</small><strong>{selected.result?.rowCount ?? '—'}</strong></div></div>
      {#if selected.failure}<p class="error" role="alert">{selected.failure}</p>{/if}
      {#if selected.status === 'QUEUED' || selected.status === 'STARTING' || selected.status === 'RUNNING'}<button type="button" class="secondary" onclick={() => cancel(selected!.id)}>Cancel execution</button>{/if}
      <section class="detail-section"><h3>Submitted query</h3><p class="detail-caption">{datasetName(selected.datasetId)} · {selected.engine} / {selected.mode}</p><pre>{selected.sql}</pre></section>
      <section class="detail-section"><h3>Result and container</h3><dl><dt>Output</dt><dd>{selected.outputFile ?? 'Not published'}</dd><dt>Exit reason</dt><dd>{selected.exitReason ?? 'Pending'}</dd><dt>Image</dt><dd>{selected.image}</dd></dl>
        {#if selected.result?.columns?.length}<p class="detail-caption">Output columns</p><div class="output-columns">{#each selected.result.columns as column}<span>{column.name} <small>{column.type}</small></span>{/each}</div>{/if}
      </section>
      <section class="detail-section"><h3>Worker logs</h3><pre>{logs || 'Waiting for worker output'}</pre></section>
    </div>
  </dialog>
{/if}

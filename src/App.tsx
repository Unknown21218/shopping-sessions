import { useEffect, useState } from 'react';
import {
  Activity,
  Download,
  GitMerge,
  Layers,
  Play,
  RotateCcw,
  Table,
  Terminal,
} from 'lucide-react';
import type { PipelineInput, PipelineResult } from './pipeline.ts';

type TabKey = 'campaign_daily' | 'user_daily' | 'links' | 'inputs';

function formatCurrency(cents: number): string {
  return `$${(cents / 100).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function downloadCsv(filename: string, headers: string[], rows: (string | number | null)[][]) {
  const escape = (v: string | number | null) => {
    if (v === null) return '';
    const s = String(v);
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  const csv = [headers.join(','), ...rows.map((r) => r.map(escape).join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export default function App() {
  const [input, setInput] = useState<PipelineInput | null>(null);
  const [result, setResult] = useState<PipelineResult | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>('campaign_daily');
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasDiskData, setHasDiskData] = useState(false);

  const loadSample = async () => {
    setRunning(true);
    setError(null);
    try {
      const res = await fetch('/api/sample');
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to load sample');
      setInput(data.input);
      setResult(data.result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load sample');
    } finally {
      setRunning(false);
    }
  };

  useEffect(() => {
    loadSample();
    fetch('/api/data-dir-status')
      .then((r) => r.json())
      .then((d) => setHasDiskData(Boolean(d.hasManifest)))
      .catch(() => setHasDiskData(false));
  }, []);

  const runWithCurrentInputs = async () => {
    if (!input) return;
    setRunning(true);
    setError(null);
    try {
      const res = await fetch('/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(input),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Pipeline run failed');
      setResult(data.result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Pipeline run failed');
    } finally {
      setRunning(false);
    }
  };

  const runFromDisk = async () => {
    setRunning(true);
    setError(null);
    try {
      const res = await fetch('/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ useDiskDataDir: true }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to run from DATA_DIR');
      setResult(data.result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to run from DATA_DIR');
    } finally {
      setRunning(false);
    }
  };

  const stats = result?.stats;

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <header className="border-b border-slate-800 bg-slate-900/70 backdrop-blur">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-indigo-500/15 text-indigo-400 ring-1 ring-indigo-500/30">
              <Layers className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight text-white">
                Shopping Sessions Pipeline
              </h1>
              <p className="text-xs text-slate-400">
                Union-Find Identity Resolution · Event Dedup · 30m Sessionization · 7d Attribution &amp; Assists
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {hasDiskData && (
              <button
                onClick={runFromDisk}
                disabled={running}
                className="inline-flex items-center gap-1.5 rounded-lg border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-50"
              >
                <Terminal className="h-3.5 w-3.5" />
                Run DATA_DIR
              </button>
            )}
            <button
              onClick={loadSample}
              disabled={running}
              className="inline-flex items-center gap-1.5 rounded-lg border border-slate-700 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-200 hover:bg-slate-700 disabled:opacity-50"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              Reset Sample
            </button>
            <button
              onClick={runWithCurrentInputs}
              disabled={running || !input}
              className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3.5 py-1.5 text-xs font-semibold text-white shadow-sm hover:bg-indigo-500 disabled:opacity-50"
            >
              <Play className="h-3.5 w-3.5" />
              {running ? 'Running...' : 'Run Pipeline'}
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl space-y-6 px-6 py-6">
        {error && (
          <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            {error}
          </div>
        )}

        {stats && (
          <section className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Valid / Raw Events</div>
              <div className="mt-1 text-xl font-semibold text-white">
                {stats.valid_events_processed}
                <span className="ml-1 text-xs font-normal text-slate-400">
                  / {stats.raw_events_rows}
                </span>
              </div>
              <div className="mt-1 text-[11px] text-slate-400">
                {stats.deduped_duplicate_events} deduped · {stats.dropped_invalid_or_late_events} dropped
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Identity Links</div>
              <div className="mt-1 text-xl font-semibold text-white">
                {stats.resolved_link_groups}
                <span className="ml-1 text-xs font-normal text-slate-400">groups</span>
              </div>
              <div className="mt-1 text-[11px] text-slate-400">
                {stats.link_edges} edges resolved
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Sessions</div>
              <div className="mt-1 text-xl font-semibold text-white">{stats.total_sessions}</div>
              <div className="mt-1 text-[11px] text-slate-400">
                {stats.total_purchasing_sessions} purchasing
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Total Revenue</div>
              <div className="mt-1 text-xl font-semibold text-emerald-400">
                {formatCurrency(stats.total_revenue_cents)}
              </div>
              <div className="mt-1 text-[11px] text-slate-400">
                {stats.total_revenue_cents.toLocaleString()} cents
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Clicks &amp; Window</div>
              <div className="mt-1 text-xl font-semibold text-white">
                {stats.valid_clicks_processed}
                <span className="ml-1 text-xs font-normal text-slate-400">
                  / {stats.raw_clicks_rows}
                </span>
              </div>
              <div className="mt-1 text-[11px] text-slate-400">{stats.ndays} day window</div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-4">
              <div className="text-xs text-slate-400">Run Time</div>
              <div className="mt-1 flex items-baseline gap-1 text-xl font-semibold text-indigo-300">
                <Activity className="h-4 w-4 self-center" />
                {stats.execution_ms} ms
              </div>
              <div className="mt-1 text-[11px] text-slate-400">
                {stats.window_start_iso.slice(0, 10)} → {stats.window_end_iso.slice(0, 10)}
              </div>
            </div>
          </section>
        )}

        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-3">
          <div className="flex flex-wrap gap-2">
            {(
              [
                ['campaign_daily', `campaign_daily (${result?.campaign_daily.length ?? 0})`],
                ['user_daily', `user_daily (${result?.user_daily.length ?? 0})`],
                ['links', `Identity Groups (${result?.link_groups.length ?? 0})`],
                ['inputs', 'Edit CSV & Manifest Inputs'],
              ] as [TabKey, string][]
            ).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setActiveTab(key)}
                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${
                  activeTab === key
                    ? 'bg-indigo-600 text-white'
                    : 'bg-slate-900 text-slate-300 hover:bg-slate-800'
                }`}
              >
                {label}
              </button>
            ))}
          </div>

          {result && (activeTab === 'campaign_daily' || activeTab === 'user_daily') && (
            <button
              onClick={() => {
                if (activeTab === 'campaign_daily') {
                  downloadCsv(
                    'campaign_daily.csv',
                    [
                      'campaign_id',
                      'channel',
                      'day',
                      'sessions',
                      'purchasing_sessions',
                      'revenue_cents',
                      'assisted_sessions',
                    ],
                    result.campaign_daily.map((r) => [
                      r.campaign_id,
                      r.channel,
                      r.day_iso,
                      r.sessions,
                      r.purchasing_sessions,
                      r.revenue_cents,
                      r.assisted_sessions,
                    ])
                  );
                } else {
                  downloadCsv(
                    'user_daily.csv',
                    ['user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category'],
                    result.user_daily.map((r) => [
                      r.user_id,
                      r.day_iso,
                      r.sessions,
                      r.events,
                      r.revenue_cents,
                      r.top_category,
                    ])
                  );
                }
              }}
              className="inline-flex items-center gap-1.5 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs font-medium text-slate-200 hover:bg-slate-800"
            >
              <Download className="h-3.5 w-3.5" />
              Export {activeTab}.csv
            </button>
          )}
        </div>

        {activeTab === 'campaign_daily' && result && (
          <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900/50">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="border-b border-slate-800 bg-slate-900 text-xs uppercase text-slate-400">
                  <tr>
                    <th className="px-4 py-3">campaign_id</th>
                    <th className="px-4 py-3">channel</th>
                    <th className="px-4 py-3">day</th>
                    <th className="px-4 py-3 text-right">sessions</th>
                    <th className="px-4 py-3 text-right">purchasing_sessions</th>
                    <th className="px-4 py-3 text-right">revenue_cents</th>
                    <th className="px-4 py-3 text-right">assisted_sessions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {result.campaign_daily.map((row, idx) => (
                    <tr key={`${row.campaign_id}-${row.day}-${idx}`} className="hover:bg-slate-800/40">
                      <td className="px-4 py-2.5 font-mono text-xs text-indigo-300">
                        {row.campaign_id}
                      </td>
                      <td className="px-4 py-2.5 text-xs text-slate-300">{row.channel}</td>
                      <td className="px-4 py-2.5 font-mono text-xs text-slate-300">
                        {row.day_iso} <span className="text-slate-500">({row.day})</span>
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs">{row.sessions}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs">
                        {row.purchasing_sessions}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-emerald-400">
                        {row.revenue_cents.toLocaleString()}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-amber-300">
                        {row.assisted_sessions}
                      </td>
                    </tr>
                  ))}
                  {result.campaign_daily.length === 0 && (
                    <tr>
                      <td colSpan={7} className="px-4 py-8 text-center text-sm text-slate-500">
                        No campaign_daily rows generated.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'user_daily' && result && (
          <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900/50">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="border-b border-slate-800 bg-slate-900 text-xs uppercase text-slate-400">
                  <tr>
                    <th className="px-4 py-3">user_id (canonical)</th>
                    <th className="px-4 py-3">day</th>
                    <th className="px-4 py-3 text-right">sessions</th>
                    <th className="px-4 py-3 text-right">events</th>
                    <th className="px-4 py-3 text-right">revenue_cents</th>
                    <th className="px-4 py-3">top_category</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {result.user_daily.map((row, idx) => (
                    <tr key={`${row.user_id}-${row.day}-${idx}`} className="hover:bg-slate-800/40">
                      <td className="px-4 py-2.5 font-mono text-xs text-indigo-300">
                        {row.user_id}
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-slate-300">
                        {row.day_iso} <span className="text-slate-500">({row.day})</span>
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs">{row.sessions}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs">{row.events}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-emerald-400">
                        {row.revenue_cents.toLocaleString()}
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-slate-200">
                        {row.top_category ?? <span className="text-slate-500">NULL</span>}
                      </td>
                    </tr>
                  ))}
                  {result.user_daily.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-4 py-8 text-center text-sm text-slate-500">
                        No user_daily rows generated.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'links' && result && (
          <div className="grid gap-4 md:grid-cols-2">
            {result.link_groups.map((grp) => (
              <div
                key={grp.canonical_user_id}
                className="rounded-xl border border-slate-800 bg-slate-900/50 p-4"
              >
                <div className="flex items-center gap-2 text-xs font-medium text-slate-400">
                  <GitMerge className="h-4 w-4 text-indigo-400" />
                  Canonical Key (lexicographically smallest)
                </div>
                <div className="mt-1 font-mono text-sm font-semibold text-indigo-300">
                  {grp.canonical_user_id}
                </div>
                <div className="mt-3 flex flex-wrap gap-1.5">
                  {grp.aliases.map((alias) => (
                    <span
                      key={alias}
                      className="rounded border border-slate-700 bg-slate-800 px-2 py-0.5 font-mono text-xs text-slate-200"
                    >
                      {alias}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        {activeTab === 'inputs' && input && (
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="flex items-center gap-2 text-xs font-semibold text-slate-300">
                <Table className="h-4 w-4 text-indigo-400" />
                manifest.json
              </div>
              <textarea
                value={input.manifest}
                onChange={(e) => setInput({ ...input, manifest: e.target.value })}
                rows={5}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>

            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="text-xs font-semibold text-slate-300">products.csv</div>
              <textarea
                value={input.productsCsv}
                onChange={(e) => setInput({ ...input, productsCsv: e.target.value })}
                rows={5}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>

            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="text-xs font-semibold text-slate-300">campaigns.csv</div>
              <textarea
                value={input.campaignsCsv}
                onChange={(e) => setInput({ ...input, campaignsCsv: e.target.value })}
                rows={5}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>

            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="text-xs font-semibold text-slate-300">links/links_01.csv</div>
              <textarea
                value={input.linksCsvs[0]?.content ?? ''}
                onChange={(e) =>
                  setInput({
                    ...input,
                    linksCsvs: [{ name: 'links_01.csv', content: e.target.value }],
                  })
                }
                rows={5}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>

            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="text-xs font-semibold text-slate-300">clicks/clicks_01.csv</div>
              <textarea
                value={input.clicksCsvs[0]?.content ?? ''}
                onChange={(e) =>
                  setInput({
                    ...input,
                    clicksCsvs: [{ name: 'clicks_01.csv', content: e.target.value }],
                  })
                }
                rows={7}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>

            <div className="space-y-2 rounded-xl border border-slate-800 bg-slate-900/50 p-4">
              <div className="text-xs font-semibold text-slate-300">events/events_01.csv</div>
              <textarea
                value={input.eventsCsvs[0]?.content ?? ''}
                onChange={(e) =>
                  setInput({
                    ...input,
                    eventsCsvs: [{ name: 'events_01.csv', content: e.target.value }],
                  })
                }
                rows={7}
                className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-indigo-500 focus:outline-none"
              />
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

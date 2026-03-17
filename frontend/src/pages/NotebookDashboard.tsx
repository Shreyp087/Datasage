import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  BarChart3,
  BookOpen,
  CalendarClock,
  Download,
  Eye,
  Filter,
  Play,
  RefreshCw,
  Search,
  Sparkles,
} from 'lucide-react';

import { datasetsApi } from '../api/datasets';
import { notebooksApi } from '../api/notebooks';
import { useJobProgress } from '../hooks/useJobProgress';

type NotebookResult = {
  status?: string;
  result?: Record<string, any> | null;
};

type NotebookCell = {
  id?: string;
  type?: string;
  title?: string;
  analysis_type?: string;
};

type NotebookRecord = {
  id: string;
  title: string;
  description?: string | null;
  domain?: string | null;
  dataset_id?: string | null;
  is_template: boolean;
  run_count?: number;
  results?: Record<string, NotebookResult | any>;
  cells?: NotebookCell[];
  last_run_at?: string | null;
  updated_at?: string | null;
  created_at?: string | null;
};

type DatasetRecord = {
  id: string;
  name: string;
  domain: string;
  status: string;
  row_count: number | null;
  col_count: number | null;
};

type ExportFormat = 'html' | 'jupyter';

const formatDateTime = (value?: string | null) => {
  if (!value) return 'Never';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return 'Unknown';
  return d.toLocaleString();
};

const badgeColor = (status: string) => {
  switch ((status || '').toLowerCase()) {
    case 'complete':
      return 'bg-[#0f2a1f] text-[#3fb950] border-[#238636]';
    case 'failed':
      return 'bg-[#2f1b21] text-[#ff7b72] border-[#f85149]';
    default:
      return 'bg-[#1f2b40] text-[#9cc3ff] border-[#2f5f9c]';
  }
};

export default function NotebookDashboard() {
  const [notebooks, setNotebooks] = useState<NotebookRecord[]>([]);
  const [datasets, setDatasets] = useState<DatasetRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const [domainFilter, setDomainFilter] = useState('all');
  const [selectedNotebookId, setSelectedNotebookId] = useState<string | null>(null);
  const [actionNotebookId, setActionNotebookId] = useState<string | null>(null);
  const [exportLoading, setExportLoading] = useState<ExportFormat | null>(null);
  const [runningNotebookId, setRunningNotebookId] = useState<string | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);
  const progress = useJobProgress(jobId);

  const loadData = useCallback(async () => {
    try {
      const [notebooksRes, datasetsRes] = await Promise.all([
        notebooksApi.list({ is_template: false }),
        datasetsApi.list(),
      ]);
      const listedNotebooks = ((notebooksRes?.data || []) as NotebookRecord[]).filter((item) => !item.is_template);
      setNotebooks(listedNotebooks);
      setDatasets((datasetsRes?.data || []) as DatasetRecord[]);
      setError(null);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load notebook dashboard.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    const interval = window.setInterval(loadData, 12000);
    return () => clearInterval(interval);
  }, [loadData]);

  const datasetById = useMemo(() => {
    return new Map(datasets.map((dataset) => [dataset.id, dataset]));
  }, [datasets]);

  const domainOptions = useMemo(() => {
    const values = new Set<string>();
    notebooks.forEach((notebook) => {
      if (notebook.domain) values.add(notebook.domain);
    });
    return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))];
  }, [notebooks]);

  const filteredNotebooks = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return notebooks.filter((notebook) => {
      if (domainFilter !== 'all' && (notebook.domain || '').toLowerCase() !== domainFilter.toLowerCase()) {
        return false;
      }
      if (!normalizedQuery) return true;
      const datasetName = notebook.dataset_id ? datasetById.get(notebook.dataset_id)?.name || '' : '';
      return (
        (notebook.title || '').toLowerCase().includes(normalizedQuery)
        || (notebook.domain || '').toLowerCase().includes(normalizedQuery)
        || datasetName.toLowerCase().includes(normalizedQuery)
      );
    });
  }, [notebooks, domainFilter, query, datasetById]);

  useEffect(() => {
    if (!filteredNotebooks.length) {
      setSelectedNotebookId(null);
      return;
    }
    const stillExists = filteredNotebooks.some((notebook) => notebook.id === selectedNotebookId);
    if (!stillExists) {
      setSelectedNotebookId(filteredNotebooks[0].id);
    }
  }, [filteredNotebooks, selectedNotebookId]);

  const selectedNotebook = useMemo(
    () => filteredNotebooks.find((notebook) => notebook.id === selectedNotebookId) || null,
    [filteredNotebooks, selectedNotebookId]
  );

  const selectedDataset = selectedNotebook?.dataset_id ? datasetById.get(selectedNotebook.dataset_id) : null;

  const summary = useMemo(() => {
    if (!selectedNotebook) {
      return {
        totalCells: 0,
        resultCells: 0,
        chartCells: 0,
        narrativeCells: 0,
        successCells: 0,
      };
    }
    const cells = selectedNotebook.cells || [];
    const results = selectedNotebook.results || {};

    let chartCells = 0;
    let narrativeCells = 0;
    let successCells = 0;

    Object.values(results).forEach((entry: any) => {
      const payload = entry?.result || entry || {};
      const type = String(payload?.type || '').toLowerCase();
      const chartType = String(payload?.chart_type || '').toLowerCase();
      const status = String(entry?.status || '').toLowerCase();
      if (status === 'success') successCells += 1;
      if (type === 'chart' || type === 'heatmap' || Boolean(chartType)) chartCells += 1;
      if (type === 'narrative') narrativeCells += 1;
    });

    return {
      totalCells: cells.length,
      resultCells: Object.keys(results).length,
      chartCells,
      narrativeCells,
      successCells,
    };
  }, [selectedNotebook]);

  const triggerDownload = (blob: Blob, filename: string) => {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  };

  const openInNewTab = (blob: Blob) => {
    const url = window.URL.createObjectURL(blob);
    window.open(url, '_blank', 'noopener,noreferrer');
    window.setTimeout(() => window.URL.revokeObjectURL(url), 60000);
  };

  const handleExport = async (notebook: NotebookRecord, format: ExportFormat) => {
    setActionError(null);
    setStatusMessage(null);
    setActionNotebookId(notebook.id);
    setExportLoading(format);
    try {
      const response = await notebooksApi.export(notebook.id, format);
      const blob = response.data instanceof Blob ? response.data : new Blob([response.data]);
      const safeName = (notebook.title || `Notebook_${notebook.id.slice(0, 8)}`).replace(/[^a-zA-Z0-9._-]+/g, '_');
      if (format === 'html') {
        openInNewTab(blob);
        setStatusMessage('Notebook HTML opened in a new tab.');
      } else {
        triggerDownload(blob, `${safeName}.ipynb`);
        setStatusMessage('Notebook downloaded as .ipynb.');
      }
    } catch (err: any) {
      setActionError(err?.response?.data?.detail || err?.message || 'Notebook export failed.');
    } finally {
      setExportLoading(null);
      setActionNotebookId(null);
    }
  };

  const handleRunNotebook = async (notebook: NotebookRecord) => {
    setActionError(null);
    setStatusMessage(null);
    setActionNotebookId(notebook.id);
    try {
      if (!notebook.dataset_id) {
        throw new Error('This notebook is not linked to a dataset.');
      }
      const response = await notebooksApi.generateForDataset(notebook.dataset_id, {
        title: notebook.title,
        run_now: true,
        replace_existing: true,
      });
      const createdNotebookId = response?.data?.notebook?.id as string | undefined;
      const createdJobId = response?.data?.job_id as string | undefined;
      if (createdNotebookId) {
        setSelectedNotebookId(createdNotebookId);
      }
      if (!createdJobId) {
        throw new Error('Notebook run job was not created.');
      }
      setRunningNotebookId(createdNotebookId || notebook.id);
      setJobId(createdJobId);
      setStatusMessage(`Notebook run started. Job ID: ${createdJobId}`);
    } catch (err: any) {
      setActionError(err?.response?.data?.detail || err?.message || 'Failed to run notebook.');
      setRunningNotebookId(null);
      setJobId(null);
    } finally {
      setActionNotebookId(null);
    }
  };

  useEffect(() => {
    const step = String(progress.step || '').toLowerCase();
    if (!step || !jobId) return;

    if (step === 'complete') {
      setStatusMessage('Notebook run completed successfully.');
      setRunningNotebookId(null);
      setJobId(null);
      loadData();
      return;
    }
    if (step === 'failed') {
      setActionError(progress.message || 'Notebook run failed.');
      setRunningNotebookId(null);
      setJobId(null);
    }
  }, [progress.message, progress.step, jobId, loadData]);

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      <div className="rounded-2xl border border-[#2f3d4c] bg-[linear-gradient(120deg,#0f1724,#132c4d_50%,#0f1724)] p-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="space-y-2">
            <p className="text-xs tracking-[0.2em] uppercase text-[#8fb8ff]">Notebook Studio</p>
            <h2 className="text-3xl font-bold text-white flex items-center gap-3">
              <BookOpen size={28} className="text-[#8fb8ff]" />
              Notebook Content & Visualization Dashboard
            </h2>
            <p className="text-[#b7c9e8]">
              Run notebooks, inspect visualization coverage, and export reports from one workspace.
            </p>
          </div>
          <button
            onClick={loadData}
            className="px-4 py-2 rounded-md border border-[#35557f] bg-[#16263f] hover:bg-[#1f3559] text-[#cfe0ff] flex items-center gap-2 transition-colors"
          >
            <RefreshCw size={16} /> Refresh
          </button>
        </div>
      </div>

      {(error || actionError || statusMessage || jobId) && (
        <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-4 space-y-2">
          {error && <p className="text-[#ff7b72] text-sm">{error}</p>}
          {actionError && <p className="text-[#ff7b72] text-sm">{actionError}</p>}
          {statusMessage && <p className="text-[#3fb950] text-sm">{statusMessage}</p>}
          {jobId && (
            <div className="space-y-2">
              <p className="text-xs text-[#9ab2d3]">
                Notebook progress: {(progress.step || 'queued').toString()} ({Math.round(progress.pct || 0)}%)
              </p>
              <div className="h-2 w-full bg-[#0d1117] border border-[#30363d] rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-[#58a6ff] to-[#3fb950] transition-all duration-500"
                  style={{ width: `${Math.max(0, Math.min(100, progress.pct || 0))}%` }}
                />
              </div>
            </div>
          )}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <section className="lg:col-span-4 bg-[#161b22] border border-[#30363d] rounded-xl overflow-hidden">
          <div className="p-4 border-b border-[#30363d] space-y-3">
            <div className="relative">
              <Search size={16} className="absolute left-3 top-3 text-[#6b7786]" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search notebook or dataset..."
                className="w-full bg-[#0d1117] border border-[#30363d] rounded-md pl-9 pr-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-[#4a7db8]"
              />
            </div>
            <div className="relative">
              <Filter size={16} className="absolute left-3 top-3 text-[#6b7786]" />
              <select
                value={domainFilter}
                onChange={(e) => setDomainFilter(e.target.value)}
                className="w-full bg-[#0d1117] border border-[#30363d] rounded-md pl-9 pr-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-[#4a7db8]"
              >
                {domainOptions.map((domain) => (
                  <option key={domain} value={domain}>
                    {domain === 'all' ? 'All domains' : domain}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="max-h-[640px] overflow-y-auto">
            {!loading && filteredNotebooks.length === 0 && (
              <div className="p-4 text-sm text-gray-400">No notebooks match the current filters.</div>
            )}
            {filteredNotebooks.map((notebook) => {
              const linkedDataset = notebook.dataset_id ? datasetById.get(notebook.dataset_id) : null;
              const selected = notebook.id === selectedNotebookId;
              return (
                <button
                  key={notebook.id}
                  onClick={() => setSelectedNotebookId(notebook.id)}
                  className={`w-full text-left px-4 py-3 border-b border-[#222a33] transition-colors ${
                    selected ? 'bg-[#11263f]' : 'hover:bg-[#1c2330]'
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="font-semibold text-white">{notebook.title}</p>
                      <p className="text-xs text-[#8ca1bd] mt-1">
                        {linkedDataset?.name || 'No dataset linked'}
                      </p>
                    </div>
                    <span className={`text-[10px] px-2 py-1 rounded-full border ${badgeColor(linkedDataset?.status || 'unknown')}`}>
                      {(linkedDataset?.status || 'unknown').replace(/_/g, ' ')}
                    </span>
                  </div>
                  <p className="text-[11px] text-[#71839a] mt-2">
                    Domain: {notebook.domain || '-'} • Runs: {Number(notebook.run_count || 0)}
                  </p>
                </button>
              );
            })}
          </div>
        </section>

        <section className="lg:col-span-8 bg-[#161b22] border border-[#30363d] rounded-xl p-6 space-y-5">
          {!selectedNotebook && (
            <div className="text-gray-400">Select a notebook to view and manage it.</div>
          )}

          {selectedNotebook && (
            <>
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <h3 className="text-2xl font-bold text-white">{selectedNotebook.title}</h3>
                  <p className="text-sm text-[#9db2cc] mt-1">
                    {selectedNotebook.description || 'No description provided.'}
                  </p>
                  <p className="text-xs text-[#72839a] mt-2">
                    Notebook ID: {selectedNotebook.id}
                  </p>
                </div>
                <div className="text-sm text-[#9db2cc] space-y-1">
                  <div className="flex items-center gap-2">
                    <CalendarClock size={14} /> Last run: {formatDateTime(selectedNotebook.last_run_at)}
                  </div>
                  <div>Updated: {formatDateTime(selectedNotebook.updated_at)}</div>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] p-3">
                  <p className="text-[11px] uppercase tracking-wider text-[#7c93b3]">Cells</p>
                  <p className="text-2xl font-bold text-white">{summary.totalCells}</p>
                </div>
                <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] p-3">
                  <p className="text-[11px] uppercase tracking-wider text-[#7c93b3]">Executed</p>
                  <p className="text-2xl font-bold text-white">{summary.resultCells}</p>
                </div>
                <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] p-3">
                  <p className="text-[11px] uppercase tracking-wider text-[#7c93b3]">Charts</p>
                  <p className="text-2xl font-bold text-white">{summary.chartCells}</p>
                </div>
                <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] p-3">
                  <p className="text-[11px] uppercase tracking-wider text-[#7c93b3]">Narratives</p>
                  <p className="text-2xl font-bold text-white">{summary.narrativeCells}</p>
                </div>
                <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] p-3">
                  <p className="text-[11px] uppercase tracking-wider text-[#7c93b3]">Success</p>
                  <p className="text-2xl font-bold text-white">{summary.successCells}</p>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                <button
                  onClick={() => handleRunNotebook(selectedNotebook)}
                  disabled={actionNotebookId === selectedNotebook.id || !selectedNotebook.dataset_id}
                  className="px-4 py-2 rounded-md border border-[#2ea043] bg-[#0d4a38] hover:bg-[#12634b] text-white flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
                >
                  <Play size={16} />
                  {actionNotebookId === selectedNotebook.id && runningNotebookId === selectedNotebook.id ? 'Starting...' : 'Run Notebook'}
                </button>
                <button
                  onClick={() => handleExport(selectedNotebook, 'html')}
                  disabled={actionNotebookId === selectedNotebook.id || exportLoading !== null}
                  className="px-4 py-2 rounded-md border border-[#6b4d31] bg-[#3f2d1f] hover:bg-[#5a4029] text-white flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
                >
                  <Eye size={16} /> {exportLoading === 'html' && actionNotebookId === selectedNotebook.id ? 'Opening...' : 'View Notebook'}
                </button>
                <button
                  onClick={() => handleExport(selectedNotebook, 'jupyter')}
                  disabled={actionNotebookId === selectedNotebook.id || exportLoading !== null}
                  className="px-4 py-2 rounded-md border border-[#6b4d31] bg-[#3f2d1f] hover:bg-[#5a4029] text-white flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
                >
                  <Download size={16} /> {exportLoading === 'jupyter' && actionNotebookId === selectedNotebook.id ? 'Downloading...' : 'Download .ipynb'}
                </button>
                {selectedNotebook.dataset_id && (
                  <Link
                    to={`/dataset/${selectedNotebook.dataset_id}`}
                    className="px-4 py-2 rounded-md border border-[#35557f] bg-[#16263f] hover:bg-[#1f3559] text-[#d8e6ff] flex items-center gap-2"
                  >
                    <BarChart3 size={16} /> Open Dataset View
                  </Link>
                )}
              </div>

              <div className="rounded-lg border border-[#2a3440] bg-[#0f1722] overflow-hidden">
                <div className="px-4 py-3 border-b border-[#2a3440] flex items-center gap-2 text-sm text-[#c7d4e5]">
                  <Sparkles size={15} className="text-[#8fb8ff]" />
                  Notebook cells and analysis types
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-sm">
                    <thead className="bg-[#0d1117] text-[#8398b2]">
                      <tr>
                        <th className="px-4 py-2 font-medium">Cell</th>
                        <th className="px-4 py-2 font-medium">Type</th>
                        <th className="px-4 py-2 font-medium">Analysis</th>
                        <th className="px-4 py-2 font-medium">Status</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#1f2833]">
                      {(selectedNotebook.cells || []).map((cell, index) => {
                        const cellId = String(cell.id || '');
                        const output: any = (selectedNotebook.results || {})[cellId];
                        const status = String(output?.status || (output ? 'complete' : 'not_run'));
                        const statusClass = status === 'success' || status === 'complete'
                          ? 'text-[#3fb950]'
                          : status === 'error' || status === 'failed'
                            ? 'text-[#ff7b72]'
                            : 'text-[#9db2cc]';
                        return (
                          <tr key={cellId || `cell-${index}`} className="hover:bg-[#111b29]">
                            <td className="px-4 py-2 text-white">{cell.title || cellId || 'Untitled'}</td>
                            <td className="px-4 py-2 text-[#9db2cc]">{cell.type || '-'}</td>
                            <td className="px-4 py-2 text-[#9db2cc]">{cell.analysis_type || '-'}</td>
                            <td className={`px-4 py-2 capitalize ${statusClass}`}>{status.replace(/_/g, ' ')}</td>
                          </tr>
                        );
                      })}
                      {(selectedNotebook.cells || []).length === 0 && (
                        <tr>
                          <td colSpan={4} className="px-4 py-3 text-[#8398b2]">
                            This notebook has no cells configured.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </section>
      </div>
    </div>
  );
}

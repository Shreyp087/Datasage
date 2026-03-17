import React, { useEffect, useMemo, useState } from 'react';
import { useParams } from 'react-router-dom';
import * as Tabs from '@radix-ui/react-tabs';
import { BarChart2, Bot, Download, Eye, FileText, List, Play } from 'lucide-react';

import { datasetsApi } from '../api/datasets';
import { notebooksApi } from '../api/notebooks';
import AgentReportCard from '../components/reports/AgentReportCard';
import EDAReportViewer from '../components/reports/EDAReportViewer';
import { useJobProgress } from '../hooks/useJobProgress';

type DatasetRecord = {
  id: string;
  name: string;
  status: string;
  row_count: number | null;
  col_count: number | null;
  domain: string;
};

type AgentReport = {
  id: string;
  agent_name: string;
  agent_role: string;
  report_markdown: string | null;
  tokens_used: number;
  provider: string;
  model_used?: string;
};

type JobRecord = {
  id: string;
  status: string;
  progress_pct: number;
  current_step: string;
  error_message: string | null;
};

type NotebookRecord = {
  id: string;
  dataset_id: string | null;
  is_template: boolean;
  run_count?: number;
  results?: Record<string, unknown>;
  last_run_at?: string | null;
  updated_at?: string | null;
  created_at?: string | null;
};

export default function DatasetDetails() {
  const { id } = useParams();
  const [activeTab, setActiveTab] = useState('overview');
  const [dataset, setDataset] = useState<DatasetRecord | null>(null);
  const [reports, setReports] = useState<AgentReport[]>([]);
  const [job, setJob] = useState<JobRecord | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [readmeLoading, setReadmeLoading] = useState<'markdown' | 'html' | null>(null);
  const [readmeStatus, setReadmeStatus] = useState<string | null>(null);
  const [readmeError, setReadmeError] = useState<string | null>(null);
  const [notebookLaunching, setNotebookLaunching] = useState(false);
  const [notebookStatus, setNotebookStatus] = useState<string | null>(null);
  const [notebookError, setNotebookError] = useState<string | null>(null);
  const [notebookId, setNotebookId] = useState<string | null>(null);
  const [notebookJobId, setNotebookJobId] = useState<string | null>(null);
  const [notebookRunCount, setNotebookRunCount] = useState<number | null>(null);
  const [notebookResultCount, setNotebookResultCount] = useState<number | null>(null);
  const [notebookExportLoading, setNotebookExportLoading] = useState<'html' | 'jupyter' | null>(null);
  const notebookProgress = useJobProgress(notebookJobId);

  useEffect(() => {
    if (!id) return;
    const fetchData = async () => {
      try {
        const [datasetRes, reportsRes, jobRes] = await Promise.all([
          datasetsApi.get(id),
          datasetsApi.getReports(id),
          datasetsApi.getJob(id).catch(() => ({ data: null })),
        ]);
        setDataset(datasetRes.data);
        setReports(reportsRes.data || []);
        setJob(jobRes.data || null);
        setError(null);
      } catch (err: any) {
        setError(err?.message || 'Failed to load dataset details.');
      }
    };
    fetchData();
  }, [id]);

  const orderedReports = useMemo(() => {
    return [...reports].sort((a, b) => {
      const aSynth = /synthesizer/i.test(a.agent_name) ? 1 : 0;
      const bSynth = /synthesizer/i.test(b.agent_name) ? 1 : 0;
      return bSynth - aSynth;
    });
  }, [reports]);

  const defaultExpandedReportId = useMemo(() => {
    const synth = orderedReports.find((report) => /synthesizer/i.test(report.agent_name));
    return synth?.id || orderedReports[0]?.id || null;
  }, [orderedReports]);

  const safeDatasetName = useMemo(() => {
    return (dataset?.name || `dataset_${id?.slice(0, 8) || 'unknown'}`).replace(/[^a-zA-Z0-9._-]+/g, '_');
  }, [dataset?.name, id]);

  const toTimestamp = (value?: string | null) => {
    if (!value) return 0;
    const ts = Date.parse(value);
    return Number.isNaN(ts) ? 0 : ts;
  };

  useEffect(() => {
    if (!id) return;

    let cancelled = false;

    const loadLatestNotebook = async () => {
      try {
        const response = await notebooksApi.list({ is_template: false });
        const all = (response?.data || []) as NotebookRecord[];
        const datasetNotebooks = all.filter((item) => !item.is_template && item.dataset_id === id);
        if (!datasetNotebooks.length) {
          if (!cancelled) {
            setNotebookId(null);
            setNotebookRunCount(null);
            setNotebookResultCount(null);
          }
          return;
        }

        const latest = [...datasetNotebooks].sort((a, b) => {
          const bTs = toTimestamp(b.last_run_at) || toTimestamp(b.updated_at) || toTimestamp(b.created_at);
          const aTs = toTimestamp(a.last_run_at) || toTimestamp(a.updated_at) || toTimestamp(a.created_at);
          return bTs - aTs;
        })[0];

        if (cancelled) return;

        setNotebookId(latest.id);
        setNotebookRunCount(Number(latest.run_count || 0));
        const resultMap = latest.results && typeof latest.results === 'object' ? latest.results : {};
        setNotebookResultCount(Object.keys(resultMap).length);
      } catch {
        // Keep notebook actions available only after explicit run if list fails.
      }
    };

    loadLatestNotebook();

    return () => {
      cancelled = true;
    };
  }, [id]);

  const handleDownload = async (format: 'csv' | 'parquet') => {
    if (!id) return;
    const response = await datasetsApi.download(id, format);
    const blob = new Blob([response.data]);
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${dataset?.name || 'dataset'}_clean.${format}`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  };

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

  const handleReadme = async (format: 'markdown' | 'html') => {
    if (!id) return;
    setReadmeLoading(format);
    setReadmeError(null);
    setReadmeStatus(null);

    try {
      const response = await datasetsApi.getReadme(id, format);
      const blob = response.data instanceof Blob ? response.data : new Blob([response.data]);

      if (format === 'markdown') {
        triggerDownload(blob, `${safeDatasetName}_README.md`);
        setReadmeStatus('README markdown downloaded.');
      } else {
        openInNewTab(blob);
        setReadmeStatus('README HTML opened in a new tab.');
      }
    } catch (err: any) {
      setReadmeError(err?.response?.data?.detail || err?.message || 'Failed to generate README.');
    } finally {
      setReadmeLoading(null);
    }
  };

  const handleNotebookExport = async (format: 'html' | 'jupyter') => {
    if (!notebookId) return;

    setNotebookExportLoading(format);
    setNotebookError(null);

    try {
      const response = await notebooksApi.export(notebookId, format);
      const blob = response.data instanceof Blob ? response.data : new Blob([response.data]);

      if (format === 'jupyter') {
        triggerDownload(blob, `${safeDatasetName}_Notebook.ipynb`);
        setNotebookStatus('Notebook downloaded (.ipynb).');
      } else {
        openInNewTab(blob);
        setNotebookStatus('Notebook opened in a new tab (HTML view).');
      }
    } catch (err: any) {
      setNotebookError(err?.response?.data?.detail || err?.message || 'Failed to export notebook.');
    } finally {
      setNotebookExportLoading(null);
    }
  };

  const handleRunNotebook = async () => {
    if (!id) return;
    setNotebookLaunching(true);
    setNotebookError(null);
    setNotebookStatus(null);
    setNotebookRunCount(null);
    setNotebookResultCount(null);
    setNotebookId(null);
    setNotebookJobId(null);

    try {
      const generateResponse = await notebooksApi.generateForDataset(id, {
        title: `${dataset?.name || 'Dataset'} Notebook`,
        run_now: true,
        replace_existing: true,
      });
      const createdNotebookId = generateResponse?.data?.notebook?.id as string | undefined;
      if (!createdNotebookId) {
        throw new Error('Notebook generation failed.');
      }
      const createdJobId = generateResponse?.data?.job_id as string | undefined;
      if (!createdJobId) {
        throw new Error('Notebook run job was not created during generation.');
      }

      setNotebookId(createdNotebookId);
      setNotebookJobId(createdJobId);
      setNotebookStatus(`Notebook run started. Job ID: ${createdJobId}`);
    } catch (err: any) {
      setNotebookError(err?.response?.data?.detail || err?.message || 'Failed to run notebook.');
    } finally {
      setNotebookLaunching(false);
    }
  };

  useEffect(() => {
    const step = (notebookProgress.step || '').toLowerCase();
    if (!notebookId || !step) return;

    if (step === 'failed') {
      setNotebookError(notebookProgress.message || 'Notebook run failed.');
      return;
    }
    if (step !== 'complete') return;

    const fetchNotebook = async () => {
      try {
        const res = await notebooksApi.get(notebookId);
        const data = res?.data || {};
        const resultMap = data?.results && typeof data.results === 'object' ? data.results : {};
        setNotebookRunCount(Number(data?.run_count || 0));
        setNotebookResultCount(Object.keys(resultMap).length);
        setNotebookStatus('Notebook run completed successfully.');
      } catch {
        setNotebookStatus('Notebook run completed.');
      }
    };

    fetchNotebook();
  }, [notebookId, notebookProgress.message, notebookProgress.step]);

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      {error && (
        <div className="bg-[#2d1b22] border border-[#f85149] text-[#ff7b72] rounded-lg px-4 py-3 text-sm">
          {error}
        </div>
      )}

      <div className="flex justify-between items-end mb-8 border-b border-[#30363d] pb-6">
        <div>
          <h2 className="text-3xl font-bold text-white flex items-center gap-3">
            {dataset?.name || `Dataset_${id?.slice(0, 8)}`}
            <span className="px-3 py-1 bg-[#238636] bg-opacity-20 text-[#3fb950] text-sm rounded-full border border-[#238636] ml-2 font-medium capitalize">
              {dataset?.status || 'unknown'}
            </span>
          </h2>
          <p className="text-gray-400 mt-2 font-mono text-sm">
            Domain: {dataset?.domain || '-'} | Shape: ({dataset?.row_count ?? '-'}, {dataset?.col_count ?? '-'})
          </p>
          {job && (
            <p className="text-gray-400 mt-1 font-mono text-xs">
              Job: {job.status} ({Math.round(job.progress_pct || 0)}%) - {job.current_step}
            </p>
          )}
        </div>
        <div className="flex gap-2 flex-wrap justify-end">
          <button
            onClick={() => handleDownload('csv')}
            className="bg-[#21262d] hover:bg-[#30363d] text-white px-4 py-2 border border-[#30363d] rounded-md font-medium transition-colors flex items-center gap-2"
          >
            <Download size={16} /> CSV
          </button>
          <button
            onClick={() => handleDownload('parquet')}
            className="bg-[#21262d] hover:bg-[#30363d] text-white px-4 py-2 border border-[#30363d] rounded-md font-medium transition-colors flex items-center gap-2"
          >
            <Download size={16} /> Parquet
          </button>
          <button
            onClick={() => handleReadme('markdown')}
            disabled={readmeLoading !== null}
            className="bg-[#1f2b40] hover:bg-[#2d3f5f] text-white px-4 py-2 border border-[#30363d] rounded-md font-medium transition-colors flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
          >
            <FileText size={16} /> {readmeLoading === 'markdown' ? 'Generating...' : 'README .md'}
          </button>
          <button
            onClick={() => handleReadme('html')}
            disabled={readmeLoading !== null}
            className="bg-[#1f2b40] hover:bg-[#2d3f5f] text-white px-4 py-2 border border-[#30363d] rounded-md font-medium transition-colors flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
          >
            <FileText size={16} /> {readmeLoading === 'html' ? 'Generating...' : 'README HTML'}
          </button>
          <button
            onClick={handleRunNotebook}
            disabled={notebookLaunching || !id || dataset?.status !== 'complete'}
            className="bg-[#0d4a38] hover:bg-[#12634b] text-white px-4 py-2 border border-[#2ea043] rounded-md font-medium transition-colors flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
          >
            <Play size={16} /> {notebookLaunching ? 'Starting...' : 'Run Notebook'}
          </button>
          <button
            onClick={() => handleNotebookExport('html')}
            disabled={!notebookId || notebookExportLoading !== null}
            className="bg-[#3f2d1f] hover:bg-[#5a4029] text-white px-4 py-2 border border-[#6b4d31] rounded-md font-medium transition-colors flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
            title={notebookId ? 'Open notebook as HTML in a new tab' : 'Run notebook first'}
          >
            <Eye size={16} /> {notebookExportLoading === 'html' ? 'Opening...' : 'View Notebook'}
          </button>
          <button
            onClick={() => handleNotebookExport('jupyter')}
            disabled={!notebookId || notebookExportLoading !== null}
            className="bg-[#3f2d1f] hover:bg-[#5a4029] text-white px-4 py-2 border border-[#6b4d31] rounded-md font-medium transition-colors flex items-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed"
            title={notebookId ? 'Download notebook as .ipynb' : 'Run notebook first'}
          >
            <Download size={16} /> {notebookExportLoading === 'jupyter' ? 'Downloading...' : 'Download Notebook'}
          </button>
        </div>
      </div>

      {(readmeStatus || readmeError || notebookStatus || notebookError || notebookJobId) && (
        <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-4 space-y-3">
          {readmeStatus && <p className="text-[#3fb950] text-sm">{readmeStatus}</p>}
          {readmeError && <p className="text-[#f85149] text-sm">{readmeError}</p>}
          {notebookStatus && <p className="text-[#58a6ff] text-sm">{notebookStatus}</p>}
          {notebookError && <p className="text-[#f85149] text-sm">{notebookError}</p>}

          {notebookJobId && (
            <div className="space-y-2">
              <div className="text-xs text-gray-400">
                Notebook progress: {(notebookProgress.step || 'queued').toString()} ({Math.round(notebookProgress.pct || 0)}%)
              </div>
              <div className="w-full h-2 bg-[#0d1117] rounded-full border border-[#30363d] overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-[#58a6ff] to-[#3fb950] transition-all duration-500"
                  style={{ width: `${Math.max(0, Math.min(100, notebookProgress.pct || 0))}%` }}
                />
              </div>
            </div>
          )}

          {(notebookRunCount !== null || notebookResultCount !== null) && (
            <div className="text-xs text-gray-300 font-mono">
              Notebook ID: {notebookId ?? '-'} | Run count: {notebookRunCount ?? '-'} | Result cells: {notebookResultCount ?? '-'}
            </div>
          )}
          {!notebookId && (
            <div className="text-xs text-gray-500">
              No notebook found for this dataset yet. Click <strong>Run Notebook</strong> first.
            </div>
          )}
        </div>
      )}

      <Tabs.Root value={activeTab} onValueChange={setActiveTab}>
        <Tabs.List className="flex border-b border-[#30363d] mb-6">
          <Tabs.Trigger
            value="overview"
            className={`px-4 py-3 font-medium text-sm flex items-center gap-2 border-b-2 transition-colors ${
              activeTab === 'overview'
                ? 'border-[#f78166] text-white'
                : 'border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-600'
            }`}
          >
            <List size={16} /> Job Overview
          </Tabs.Trigger>
          <Tabs.Trigger
            value="eda"
            className={`px-4 py-3 font-medium text-sm flex items-center gap-2 border-b-2 transition-colors ${
              activeTab === 'eda'
                ? 'border-[#f78166] text-white'
                : 'border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-600'
            }`}
          >
            <BarChart2 size={16} /> EDA Report
          </Tabs.Trigger>
          <Tabs.Trigger
            value="agents"
            className={`px-4 py-3 font-medium text-sm flex items-center gap-2 border-b-2 transition-colors ${
              activeTab === 'agents'
                ? 'border-[#f78166] text-white'
                : 'border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-600'
            }`}
          >
            <Bot size={16} /> Agent Reports
          </Tabs.Trigger>
        </Tabs.List>

        <Tabs.Content value="overview" className="outline-none">
          <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
            <h3 className="text-xl font-bold text-white mb-4">Processing Status</h3>
            {!job && <p className="text-gray-400">No processing job found for this dataset yet.</p>}
            {job && (
              <div className="space-y-4">
                <div className="flex gap-3">
                  <span className="text-gray-400">Job ID:</span>
                  <span className="text-white font-mono">{job.id}</span>
                </div>
                <div className="flex gap-3">
                  <span className="text-gray-400">Step:</span>
                  <span className="text-white capitalize">{job.current_step}</span>
                </div>
                <div className="flex gap-3">
                  <span className="text-gray-400">Progress:</span>
                  <span className="text-white">{Math.round(job.progress_pct || 0)}%</span>
                </div>
                <div className="w-full h-3 bg-[#0d1117] rounded-full border border-[#30363d] overflow-hidden">
                  <div
                    className="h-full bg-gradient-to-r from-[#e94560] to-[#4ade80] transition-all duration-500"
                    style={{ width: `${Math.max(0, Math.min(100, job.progress_pct || 0))}%` }}
                  />
                </div>
                {job.error_message && <div className="text-[#f85149]">{job.error_message}</div>}
              </div>
            )}
          </div>
        </Tabs.Content>

        <Tabs.Content value="eda" className="outline-none">
          <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
            {id ? <EDAReportViewer datasetId={id} /> : <p className="text-gray-400">Invalid dataset id.</p>}
          </div>
        </Tabs.Content>

        <Tabs.Content value="agents" className="outline-none">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="col-span-1 bg-[#161b22] border border-[#30363d] rounded-xl p-6">
              <h3 className="text-lg font-bold text-white mb-4">Agent Execution</h3>
              <div className="space-y-4">
                {reports.length === 0 && <p className="text-gray-400">No agent reports yet.</p>}
                {orderedReports.map((report) => (
                  <div
                    key={report.id}
                    className="flex items-center justify-between p-3 rounded-lg bg-[#0d1117] border border-[#30363d]"
                  >
                    <div>
                      <div className="font-medium text-gray-200">{report.agent_name}</div>
                      <div className="text-xs text-gray-500 uppercase">{report.agent_role}</div>
                    </div>
                    <span className="text-xs px-2 py-1 rounded-md bg-[#1f2b40] text-[#9cc3ff]">
                      {report.tokens_used?.toLocaleString() || 0} tokens
                    </span>
                  </div>
                ))}
              </div>
            </div>

            <div className="col-span-2 bg-[#161b22] border border-[#30363d] rounded-xl p-6">
              <h1 className="text-white text-2xl font-bold border-b border-[#30363d] pb-2">
                Multi-Agent Reports
              </h1>
              <p className="text-sm text-gray-400 mt-3 mb-4">
                The synthesized report is expanded by default. Expand other agent reports for detailed evidence.
              </p>
              {orderedReports
                .filter((report) => report.report_markdown)
                .map((report) => (
                  <AgentReportCard
                    key={report.id}
                    report={report}
                    defaultExpanded={report.id === defaultExpandedReportId}
                  />
                ))}
              {orderedReports.length === 0 && (
                <div className="text-gray-400 text-sm py-4">
                  Reports will appear here once agent processing completes.
                </div>
              )}
            </div>
          </div>
        </Tabs.Content>
      </Tabs.Root>
    </div>
  );
}

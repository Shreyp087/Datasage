import React, { useEffect, useMemo, useState } from 'react';
import { useParams } from 'react-router-dom';
import * as Tabs from '@radix-ui/react-tabs';
import { BarChart2, Bot, Download, List } from 'lucide-react';

import { datasetsApi } from '../api/datasets';
import AgentReportCard from '../components/reports/AgentReportCard';
import EDAReportViewer from '../components/reports/EDAReportViewer';

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

export default function DatasetDetails() {
  const { id } = useParams();
  const [activeTab, setActiveTab] = useState('overview');
  const [dataset, setDataset] = useState<DatasetRecord | null>(null);
  const [reports, setReports] = useState<AgentReport[]>([]);
  const [job, setJob] = useState<JobRecord | null>(null);
  const [error, setError] = useState<string | null>(null);

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
        <div className="flex gap-2">
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
        </div>
      </div>

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

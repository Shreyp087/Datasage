import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Activity, AlertTriangle, ArrowRight, CheckCircle, Database, Play, Server } from 'lucide-react';

import { datasetsApi } from '../api/datasets';
import { statsApi } from '../api/stats';

type DatasetItem = {
  id: string;
  name: string;
  domain: string;
  row_count: number | null;
  status: string;
};

type StatsOverview = {
  total_datasets: number;
  storage_used_mb: number;
  processing_count: number;
};

const statusText = (status: string) => status.replace(/_/g, ' ');

const statusIcon = (status: string) => {
  switch (status) {
    case 'complete':
      return <CheckCircle size={16} className="text-[#3fb950]" />;
    case 'agents_running':
    case 'eda_running':
    case 'preprocessing':
    case 'queued':
      return <Activity size={16} className="text-[#1f6feb] animate-pulse" />;
    case 'failed':
      return <AlertTriangle size={16} className="text-[#f85149]" />;
    default:
      return <Play size={16} className="text-[#e3b341]" />;
  }
};

export default function Dashboard() {
  const [datasets, setDatasets] = useState<DatasetItem[]>([]);
  const [stats, setStats] = useState<StatsOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [datasetsRes, statsRes] = await Promise.all([
          datasetsApi.list(),
          statsApi.overview(),
        ]);
        setDatasets((datasetsRes.data || []) as DatasetItem[]);
        setStats((statsRes.data || null) as StatsOverview | null);
        setError(null);
      } catch (err: any) {
        setError(err?.message || 'Failed to load dashboard data.');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  const statsView = stats || {
    total_datasets: 0,
    storage_used_mb: 0,
    processing_count: 0,
  };

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-3xl font-bold text-white">Platform Overview</h2>
        <Link
          to="/upload"
          className="bg-[#1f6feb] hover:bg-blue-600 text-white px-5 py-2 rounded-md font-medium transition-colors flex items-center gap-2"
        >
          New Upload <ArrowRight size={16} />
        </Link>
      </div>

      {error && (
        <div className="bg-[#2d1b22] border border-[#f85149] text-[#ff7b72] rounded-lg px-4 py-3 text-sm">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 flex items-center gap-5">
          <div className="w-12 h-12 rounded-full bg-[#21262d] flex items-center justify-center">
            <Database className="text-[#58a6ff]" />
          </div>
          <div>
            <div className="text-sm text-gray-400 font-medium tracking-wide">TOTAL DATASETS</div>
            <div className="text-3xl font-bold font-mono text-white">{statsView.total_datasets}</div>
          </div>
        </div>
        <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 flex items-center gap-5">
          <div className="w-12 h-12 rounded-full bg-[#21262d] flex items-center justify-center">
            <Server className="text-[#3fb950]" />
          </div>
          <div>
            <div className="text-sm text-gray-400 font-medium tracking-wide">STORAGE USED</div>
            <div className="text-3xl font-bold font-mono text-white">{statsView.storage_used_mb} MB</div>
          </div>
        </div>
        <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 flex items-center gap-5">
          <div className="w-12 h-12 rounded-full bg-[#21262d] flex items-center justify-center">
            <Activity className="text-[#e3b341]" />
          </div>
          <div>
            <div className="text-sm text-gray-400 font-medium tracking-wide">ACTIVE JOBS</div>
            <div className="text-3xl font-bold font-mono text-white">{statsView.processing_count}</div>
          </div>
        </div>
      </div>

      <div className="bg-[#161b22] border border-[#30363d] rounded-xl overflow-hidden">
        <div className="p-5 border-b border-[#30363d]">
          <h3 className="text-xl font-bold text-white font-mono">Recent Datasets</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-[#0d1117] text-gray-400 text-sm tracking-wider">
                <th className="p-4 font-medium">NAME</th>
                <th className="p-4 font-medium">DOMAIN</th>
                <th className="p-4 font-medium">ROWS</th>
                <th className="p-4 font-medium">STATUS</th>
                <th className="p-4 font-medium">ACTIONS</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#30363d]">
              {!loading && datasets.length === 0 && (
                <tr>
                  <td colSpan={5} className="p-6 text-gray-400">
                    No datasets uploaded yet.
                  </td>
                </tr>
              )}
              {datasets.map((dataset) => (
                <tr key={dataset.id} className="hover:bg-[#21262d] transition-colors group">
                  <td className="p-4 font-mono text-white">{dataset.name}</td>
                  <td className="p-4">
                    <span className="px-2.5 py-1 bg-[#1f6feb] bg-opacity-20 text-[#58a6ff] rounded-full text-xs font-medium uppercase">
                      {dataset.domain}
                    </span>
                  </td>
                  <td className="p-4 font-mono text-gray-300">
                    {dataset.row_count !== null ? dataset.row_count.toLocaleString() : '-'}
                  </td>
                  <td className="p-4 flex items-center gap-2 tracking-wide text-sm capitalize">
                    {statusIcon(dataset.status)} {statusText(dataset.status)}
                  </td>
                  <td className="p-4">
                    <Link
                      to={`/dataset/${dataset.id}`}
                      className="text-[#58a6ff] hover:text-white font-medium text-sm transition-colors"
                    >
                      View Report →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

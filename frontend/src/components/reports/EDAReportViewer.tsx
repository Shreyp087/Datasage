import { useEffect, useMemo, useState } from 'react';

import { datasetsApi } from '../../api/datasets';

type EDAReportResponse = {
  dataset_id: string;
  html_report: string;
  quality_score?: number;
  generated_at?: string;
};

type EDAReportViewerProps = {
  datasetId: string;
};

export default function EDAReportViewer({ datasetId }: EDAReportViewerProps) {
  const [reportUrl, setReportUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reportMeta, setReportMeta] = useState<EDAReportResponse | null>(null);

  useEffect(() => {
    if (!datasetId) return;

    let objectUrl: string | null = null;

    const fetchReport = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await datasetsApi.getEDA(datasetId);
        const data = res.data as EDAReportResponse;
        setReportMeta(data);
        const blob = new Blob([data.html_report || ''], { type: 'text/html' });
        objectUrl = URL.createObjectURL(blob);
        setReportUrl(objectUrl);
      } catch (err: any) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load EDA report.');
      } finally {
        setLoading(false);
      }
    };

    fetchReport();

    return () => {
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
    };
  }, [datasetId]);

  const qualityLabel = useMemo(() => {
    const score = Number(reportMeta?.quality_score ?? 0);
    if (!reportMeta || Number.isNaN(score)) return null;
    if (score >= 91) return `✨ ${score.toFixed(0)}/100 — Excellent`;
    if (score >= 71) return `🟢 ${score.toFixed(0)}/100 — Good`;
    if (score >= 41) return `🟡 ${score.toFixed(0)}/100 — Fair`;
    return `🔴 ${score.toFixed(0)}/100 — Poor`;
  }, [reportMeta]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <div className="text-center">
          <div className="text-3xl mb-3">⚙️</div>
          <div>Loading EDA Report...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-[#2d1b22] border border-[#f85149] text-[#ff7b72] rounded-lg px-4 py-3 text-sm">
        {error}
      </div>
    );
  }

  if (!reportUrl) {
    return (
      <div className="text-gray-400 text-sm">
        EDA report is not available yet. Processing may still be in progress.
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-4 gap-3 flex-wrap">
        <div>
          <h3 className="text-lg font-semibold text-white">📊 Exploratory Data Analysis</h3>
          <div className="text-xs text-gray-400 mt-1">
            {qualityLabel ? `Quality: ${qualityLabel}` : null}
            {reportMeta?.generated_at ? ` · Generated: ${new Date(reportMeta.generated_at).toLocaleString()}` : null}
          </div>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => window.open(reportUrl, '_blank', 'noopener,noreferrer')}
            className="px-3 py-1.5 text-sm bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition-colors"
          >
            🔗 Open Full Page
          </button>
          <a
            href={reportUrl}
            download={`eda_report_${datasetId}.html`}
            className="px-3 py-1.5 text-sm bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition-colors"
          >
            ⬇️ Download Report
          </a>
        </div>
      </div>

      <iframe
        src={reportUrl}
        className="w-full rounded-xl border border-gray-700 bg-white"
        style={{ height: '80vh', minHeight: '600px' }}
        title="EDA Report"
      />
    </div>
  );
}

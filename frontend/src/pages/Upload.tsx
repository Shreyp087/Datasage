import React, { useCallback, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { useDropzone } from 'react-dropzone';
import { CheckCircle, File, UploadCloud, X } from 'lucide-react';

import api from '../lib/api';
import { useJobProgress } from '../hooks/useJobProgress';

const DOMAINS = ['finance', 'healthcare', 'education', 'ecommerce', 'ai_incidents', 'general', 'other'];

type UploadResponse = {
  dataset_id: string;
  job_id: string;
  filename: string;
  status: string;
  message: string;
};

export default function Upload() {
  const [files, setFiles] = useState<File[]>([]);
  const [domain, setDomain] = useState('general');
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [jobId, setJobId] = useState<string | null>(null);
  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const jobProgress = useJobProgress(jobId);
  const pipelineProgress = useMemo(() => {
    if (!jobId) return null;
    return jobProgress;
  }, [jobId, jobProgress]);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    setFiles((prev) => [...prev, ...acceptedFiles]);
    setError(null);
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  const removeFile = (name: string) => {
    setFiles(files.filter((file) => file.name !== name));
  };

  const handleUpload = async () => {
    if (files.length === 0) return;
    setUploading(true);
    setUploadProgress(5);
    setError(null);

    const file = files[0];
    const formData = new FormData();
    formData.append('file', file);
    formData.append('domain', domain);

    try {
      const response = await api.post<UploadResponse>('upload/file', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        onUploadProgress: (progressEvent) => {
          if (progressEvent.total) {
            const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
            setUploadProgress(percentCompleted);
          }
        },
      });

      setJobId(response.data.job_id);
      setDatasetId(response.data.dataset_id);
      setUploading(false);
      setUploadProgress(100);
    } catch (err: any) {
      setUploading(false);
      setUploadProgress(0);
      setError(err?.response?.data?.detail || err?.message || 'Upload failed.');
    }
  };

  const progressLabel = pipelineProgress
    ? `${pipelineProgress.step || 'processing'}${pipelineProgress.message ? ` - ${pipelineProgress.message}` : ''}`
    : uploading
      ? 'Uploading file...'
      : 'Idle';

  const progressValue = pipelineProgress ? pipelineProgress.pct : uploadProgress;
  const isComplete = pipelineProgress?.step === 'complete';

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <h2 className="text-3xl font-bold text-white mb-8">Data Ingestion Studio</h2>

      <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6 space-y-6">
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">Dataset Domain</label>
          <select
            value={domain}
            onChange={(event) => setDomain(event.target.value)}
            className="w-full bg-[#0d1117] border border-[#30363d] text-white rounded-md p-2 focus:ring-2 focus:ring-[#1f6feb] outline-none capitalize"
          >
            {DOMAINS.map((entry) => (
              <option key={entry} value={entry}>
                {entry}
              </option>
            ))}
          </select>
        </div>

        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-xl p-12 text-center cursor-pointer transition-colors ${
            isDragActive
              ? 'border-[#58a6ff] bg-[#1f6feb] bg-opacity-10'
              : 'border-[#30363d] hover:border-gray-500 hover:bg-[#21262d]'
          }`}
        >
          <input {...getInputProps()} />
          <UploadCloud className="mx-auto h-12 w-12 text-gray-400 mb-4" />
          <p className="text-lg text-white font-medium">Drag and drop files here, or click to select</p>
          <p className="text-sm text-gray-500 mt-2">Supports CSV, JSON, Parquet, Excel, TSV, ZIP. Max size: 10GB.</p>
        </div>

        {files.length > 0 && (
          <div className="space-y-3">
            <h4 className="text-white font-medium">Selected Files ({files.length})</h4>
            {files.map((file) => (
              <div
                key={file.name}
                className="flex items-center justify-between bg-[#21262d] p-3 rounded-lg border border-[#30363d]"
              >
                <div className="flex items-center gap-3">
                  <File size={18} className="text-[#58a6ff]" />
                  <span className="text-gray-200 font-mono text-sm">{file.name}</span>
                  <span className="text-gray-500 text-xs">{(file.size / (1024 * 1024)).toFixed(2)} MB</span>
                </div>
                <button onClick={() => removeFile(file.name)} className="text-gray-500 hover:text-[#f85149] p-1">
                  <X size={16} />
                </button>
              </div>
            ))}
          </div>
        )}

        {(uploading || jobId || progressValue > 0) && (
          <div className="space-y-2 pt-4">
            <div className="flex justify-between text-sm">
              <span className="text-gray-300 capitalize">{progressLabel}</span>
              <span className="text-[#58a6ff]">{Math.max(0, Math.min(100, Math.round(progressValue)))}%</span>
            </div>
            <div className="w-full bg-[#0d1117] rounded-full h-2.5 border border-[#30363d] overflow-hidden">
              <div
                className="bg-[#1f6feb] h-2.5 rounded-full transition-all duration-200"
                style={{ width: `${Math.max(0, Math.min(100, progressValue))}%` }}
              />
            </div>
          </div>
        )}

        {error && <div className="text-[#f85149] text-sm">{error}</div>}

        {isComplete && datasetId && (
          <div className="bg-[#0d2d1d] border border-[#238636] text-[#3fb950] rounded-lg px-4 py-3 text-sm">
            Processing completed.
            <Link to={`/dataset/${datasetId}`} className="ml-2 underline text-white">
              Open dataset report
            </Link>
          </div>
        )}

        <div className="pt-6 border-t border-[#30363d] flex justify-end">
          <button
            disabled={files.length === 0 || uploading}
            onClick={handleUpload}
            className={`px-6 py-2 rounded-md font-medium flex items-center gap-2 ${
              files.length === 0 || uploading
                ? 'bg-[#21262d] text-gray-500 cursor-not-allowed'
                : 'bg-[#238636] hover:bg-[#2ea043] text-white'
            }`}
          >
            {uploading ? (
              <>
                <UploadCloud size={18} /> Uploading...
              </>
            ) : isComplete ? (
              <>
                <CheckCircle size={18} /> Upload Complete
              </>
            ) : (
              <>
                <UploadCloud size={18} /> Start Processing Job
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

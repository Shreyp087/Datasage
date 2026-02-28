import { useEffect, useState } from 'react';

type Progress = {
  pct: number;
  step: string;
  message: string;
};

const initialProgress: Progress = { pct: 0, step: '', message: '' };

export const useJobProgress = (jobId: string | null) => {
  const [progress, setProgress] = useState<Progress>(initialProgress);

  useEffect(() => {
    if (!jobId) return;

    let polling: number | null = null;
    const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${wsProtocol}://${window.location.hostname}:8000/api/v1/ws/jobs/${jobId}`);

    const startPollingFallback = () => {
      if (polling !== null) return;
      polling = window.setInterval(async () => {
        try {
          const response = await fetch(`http://${window.location.hostname}:8000/api/v1/jobs/${jobId}/progress`);
          if (!response.ok) return;
          const data = await response.json();
          setProgress({
            pct: Number(data.pct || 0),
            step: String(data.step || ''),
            message: String(data.message || ''),
          });
          if (data.step === 'complete' || data.step === 'failed') {
            if (polling !== null) {
              clearInterval(polling);
              polling = null;
            }
          }
        } catch {
          // Keep retrying while backend is reachable.
        }
      }, 3000);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setProgress({
          pct: Number(data.pct || 0),
          step: String(data.step || ''),
          message: String(data.message || ''),
        });
      } catch {
        // Ignore malformed frames
      }
    };

    ws.onerror = () => {
      startPollingFallback();
    };

    ws.onclose = () => {
      startPollingFallback();
    };

    return () => {
      ws.close();
      if (polling !== null) {
        clearInterval(polling);
      }
    };
  }, [jobId]);

  return progress;
};

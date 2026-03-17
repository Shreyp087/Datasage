import axiosClient from './client';

type NotebookClonePayload = {
  dataset_id: string;
  title?: string;
  description?: string;
};

type NotebookExportFormat = 'html' | 'pdf' | 'jupyter';
type NotebookGeneratePayload = {
  title?: string;
  description?: string;
  run_now?: boolean;
  replace_existing?: boolean;
};

export const notebooksApi = {
  list: (params?: { domain?: string; is_template?: boolean; tags?: string }) =>
    axiosClient.get('/api/v1/notebooks/', { params }),
  listTemplates: (domain?: string) =>
    axiosClient.get('/api/v1/notebooks/templates', {
      params: domain ? { domain } : undefined,
    }),
  cloneFromTemplate: (templateId: string, payload: NotebookClonePayload) =>
    axiosClient.post(`/api/v1/notebooks/from-template/${templateId}`, payload),
  generateForDataset: (datasetId: string, payload?: NotebookGeneratePayload) =>
    axiosClient.post(`/api/v1/notebooks/generate/${datasetId}`, payload || {}),
  run: (notebookId: string) => axiosClient.post(`/api/v1/notebooks/${notebookId}/run`),
  get: (notebookId: string) => axiosClient.get(`/api/v1/notebooks/${notebookId}`),
  export: (notebookId: string, format: NotebookExportFormat) =>
    axiosClient.post(
      `/api/v1/notebooks/${notebookId}/export`,
      { format },
      { responseType: 'blob' }
    ),
};

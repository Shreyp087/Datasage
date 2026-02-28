import axiosClient from './client';

export const datasetsApi = {
  list: () => axiosClient.get('/api/v1/datasets/'),
  get: (id: string) => axiosClient.get(`/api/v1/datasets/${id}`),
  delete: (id: string) => axiosClient.delete(`/api/v1/datasets/${id}`),
  getReports: (id: string) => axiosClient.get(`/api/v1/datasets/${id}/reports`),
  getEDA: (id: string) => axiosClient.get(`/api/v1/datasets/${id}/eda`),
  getJob: (id: string) => axiosClient.get(`/api/v1/datasets/${id}/job`),
  download: (id: string, format: string) =>
    axiosClient.get(`/api/v1/datasets/${id}/download?format=${format}`, {
      responseType: 'blob',
    }),
};

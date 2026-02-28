import axiosClient from './client';

export const statsApi = {
  overview: () => axiosClient.get('/api/v1/stats/overview'),
};

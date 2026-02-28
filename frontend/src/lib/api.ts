import axios, { AxiosInstance } from 'axios';

const DEFAULT_API_BASE_URL = 'http://localhost:8000/api/v1';
const rawApiBaseUrl = (import.meta.env.VITE_API_URL || DEFAULT_API_BASE_URL).replace(/\/+$/, '');
const normalizedApiBaseUrl = rawApiBaseUrl.endsWith('/api/v1')
  ? rawApiBaseUrl
  : `${rawApiBaseUrl}/api/v1`;

const api: AxiosInstance = axios.create({
  baseURL: normalizedApiBaseUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to attach tokens if auth was fully implemented
api.interceptors.request.use(
  (config) => {
    if (typeof config.url === 'string') {
      config.url = config.url
        .replace(/^\/?api\/v1\//, '')
        .replace(/^\/+/, '');
    }
    // const token = localStorage.getItem('token');
    // if (token) config.headers.Authorization = `Bearer ${token}`;
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for global error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Handle unauthorized
    } else if (error.response?.status === 429) {
      console.error('Rate limit exceeded. Please slow down.');
    }
    return Promise.reject(error);
  }
);

export default api;

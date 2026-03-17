import axiosClient from './client';

export type MergeJoinType = 'inner' | 'left' | 'right' | 'outer';
export type MergeStrategy = 'exact' | 'casefold' | 'numeric' | 'date' | 'id_strip' | 'slug';

export interface MergeSignal {
  type: string;
  description: string;
  score_pts?: number;
  weight?: number;
  [key: string]: unknown;
}

export interface MergeCandidate {
  left_col: string;
  right_col: string;
  join_type: MergeJoinType;
  strategy: MergeStrategy;
  confidence: number;
  label: string;
  match_count: number;
  left_total: number;
  right_total: number;
  left_match_pct: number;
  right_match_pct: number;
  merged_rows?: number;
  est_output_rows?: number;
  sample_matches: Array<Record<string, unknown>>;
  sample_nulls?: unknown[];
  sample_no_match?: unknown[];
  signals: MergeSignal[];
}

export interface DetectMergeRequest {
  left_dataset_id: string;
  right_dataset_id: string;
  top_n?: number;
  sample_rows?: number;
}

export interface DetectMergeResponse {
  left_dataset_id: string;
  right_dataset_id: string;
  left_name: string;
  right_name: string;
  left_rows: number;
  right_rows: number;
  left_cols: string[];
  right_cols: string[];
  candidates: MergeCandidate[];
  detected_at: string;
}

export interface PreviewMergeRequest {
  left_dataset_id: string;
  right_dataset_id: string;
  left_col: string;
  right_col: string;
  strategy: MergeStrategy;
  join_type: MergeJoinType;
  preview_rows?: number;
}

export interface PreviewMergeResponse {
  left_col: string;
  right_col: string;
  join_type: MergeJoinType;
  strategy: MergeStrategy;
  total_rows: number;
  matched_rows: number;
  left_only: number;
  right_only: number;
  col_conflicts: string[];
  warnings: string[];
  columns: string[];
  preview: Array<Record<string, string>>;
}

export interface ApplyMergeRequest {
  left_dataset_id: string;
  right_dataset_id: string;
  left_col: string;
  right_col: string;
  strategy: MergeStrategy;
  join_type: MergeJoinType;
  output_name?: string;
  left_suffix?: string;
  right_suffix?: string;
}

export interface ApplyMergeResponse {
  merge_id: string;
  output_dataset_id: string;
  output_name: string;
  merged_rows: number;
  matched_rows: number;
  left_only_rows: number;
  right_only_rows: number;
  col_conflicts: string[];
  warnings: string[];
  created_at: string;
}

export interface MergeHistoryItem {
  merge_id: string;
  left_dataset_id: string;
  right_dataset_id: string;
  output_dataset_id: string | null;
  left_col: string;
  right_col: string;
  strategy: MergeStrategy;
  join_type: MergeJoinType;
  merged_rows: number;
  matched_rows: number;
  left_only_rows: number;
  right_only_rows: number;
  warnings: string[];
  created_at: string | null;
}

export const mergeApi = {
  detect: (payload: DetectMergeRequest) =>
    axiosClient.post<DetectMergeResponse>('/api/v1/merge/detect', payload),
  preview: (payload: PreviewMergeRequest) =>
    axiosClient.post<PreviewMergeResponse>('/api/v1/merge/preview', payload),
  apply: (payload: ApplyMergeRequest) =>
    axiosClient.post<ApplyMergeResponse>('/api/v1/merge/apply', payload),
  history: (params?: { limit?: number; offset?: number }) =>
    axiosClient.get<MergeHistoryItem[]>('/api/v1/merge/history', { params }),
  get: (mergeId: string) =>
    axiosClient.get<MergeHistoryItem>(`/api/v1/merge/${mergeId}`),
};

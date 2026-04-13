import type {
  JobUploadResponse,
  FeedbackQuestion,
  UserFeedback,
  FeedbackSubmitResponse,
} from '../types';

export async function uploadFiles(
  sapFile: File,
  esjcFile: File,
): Promise<JobUploadResponse> {
  const formData = new FormData();
  formData.append('sap_file', sapFile);
  formData.append('esjc_file', esjcFile);

  const response = await fetch('/jobs/upload', {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => 'Unknown error');
    throw new Error(detail);
  }

  return response.json();
}

export async function fetchQuestions(jobId: string): Promise<FeedbackQuestion[]> {
  const response = await fetch(`/jobs/${jobId}/questions`);
  if (!response.ok) {
    throw new Error(`Failed to fetch questions: ${response.status}`);
  }
  const data: { questions: FeedbackQuestion[] } = await response.json();
  return data.questions;
}

export async function submitFeedback(
  jobId: string,
  answers: UserFeedback[],
): Promise<FeedbackSubmitResponse> {
  const response = await fetch(`/jobs/${jobId}/feedback`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ answers }),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => 'Unknown error');
    throw new Error(detail);
  }

  return response.json();
}

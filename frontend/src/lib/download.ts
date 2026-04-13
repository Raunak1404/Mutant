interface PyWebViewApi {
  save_job_result: (jobId: string) => Promise<{ ok: boolean; path?: string; error?: string }>;
}

declare global {
  interface Window {
    pywebview?: { api: PyWebViewApi };
  }
}

function parseFilename(contentDisposition: string | null, fallback: string): string {
  if (!contentDisposition) return fallback;
  const match = /filename="?([^"]+)"?/i.exec(contentDisposition);
  return match?.[1] ?? fallback;
}

export async function downloadJobResult(
  jobId: string,
): Promise<{ success: boolean; message: string }> {
  const url = `/jobs/${jobId}/download`;

  // Desktop app path (pywebview)
  if (window.pywebview?.api?.save_job_result) {
    const result = await window.pywebview.api.save_job_result(jobId);
    if (result.ok) {
      return { success: true, message: `Saved to ${result.path}` };
    }
    throw new Error(result.error ?? 'Download failed');
  }

  // Browser path
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(await response.text());
  }

  const blob = await response.blob();
  const filename = parseFilename(
    response.headers.get('Content-Disposition'),
    `Mutant_Output_${jobId.slice(0, 8)}.zip`,
  );
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = objectUrl;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);

  return { success: true, message: 'Download started' };
}

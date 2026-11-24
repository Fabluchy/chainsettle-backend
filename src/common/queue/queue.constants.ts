/**
 * Names and payload contracts for the BullMQ queues that carry background work
 * (webhook delivery, email sending, exports) out of the request/cron path.
 *
 * Job data must stay JSON-serialisable and self-contained: a job picked up
 * after a restart has no access to request-scoped state.
 */
export enum QueueName {
  WEBHOOKS = 'webhooks',
  EMAILS = 'emails',
  EXPORTS = 'exports',
}

export const QUEUE_NAMES: readonly QueueName[] = [
  QueueName.WEBHOOKS,
  QueueName.EMAILS,
  QueueName.EXPORTS,
];

export const WEBHOOK_DELIVERY_JOB = 'webhook.deliver';
export const EMAIL_SEND_JOB = 'email.send';
export const EXPORT_GENERATE_JOB = 'export.generate';

export interface WebhookDeliveryJobData {
  endpointId: string;
  eventType: string;
  payload: Record<string, unknown>;
}

export interface EmailJobData {
  from?: string;
  to: string;
  subject: string;
  text: string;
  html?: string;
  headers?: Record<string, string>;
}

export type ExportFormat = 'csv' | 'pdf';

export interface ExportJobData {
  stellarAddress: string;
  isAdmin: boolean;
  format: ExportFormat;
}

/** Shape returned by the export worker and persisted on the job. */
export interface ExportJobResult {
  format: ExportFormat;
  filename: string;
  contentType: string;
  bytes: number;
}

/** Serialised export artifact kept in Redis until it expires or is downloaded. */
export interface ExportArtifact {
  filename: string;
  contentType: string;
  /** base64-encoded file contents */
  data: string;
}

export const EXPORT_RESULT_KEY_PREFIX = 'chainsettle:export:job:';
export const EXPORT_RESULT_TTL_SECONDS = 60 * 60;

export function exportResultKey(jobId: string): string {
  return `${EXPORT_RESULT_KEY_PREFIX}${jobId}`;
}

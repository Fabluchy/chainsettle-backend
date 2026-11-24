import { ConfigService } from '@nestjs/config';
import { QueueName, QUEUE_NAMES } from './queue.constants';

/** Concurrency + retry policy for one queue. */
export interface QueueRuntimeConfig {
  name: QueueName;
  concurrency: number;
  /** Total attempts (first try included) before a job is parked as failed. */
  attempts: number;
  /** Base delay for exponential back-off, in milliseconds. */
  backoffMs: number;
}

interface QueueDefaults {
  concurrency: number;
  attempts: number;
  backoffMs: number;
}

const ENV_KEYS: Record<
  QueueName,
  { concurrency: string; attempts: string; backoffMs: string }
> = {
  [QueueName.WEBHOOKS]: {
    concurrency: 'QUEUE_WEBHOOKS_CONCURRENCY',
    attempts: 'QUEUE_WEBHOOKS_ATTEMPTS',
    backoffMs: 'QUEUE_WEBHOOKS_BACKOFF_MS',
  },
  [QueueName.EMAILS]: {
    concurrency: 'QUEUE_EMAILS_CONCURRENCY',
    attempts: 'QUEUE_EMAILS_ATTEMPTS',
    backoffMs: 'QUEUE_EMAILS_BACKOFF_MS',
  },
  [QueueName.EXPORTS]: {
    concurrency: 'QUEUE_EXPORTS_CONCURRENCY',
    attempts: 'QUEUE_EXPORTS_ATTEMPTS',
    backoffMs: 'QUEUE_EXPORTS_BACKOFF_MS',
  },
};

export const QUEUE_DEFAULTS: Record<QueueName, QueueDefaults> = {
  [QueueName.WEBHOOKS]: { concurrency: 5, attempts: 5, backoffMs: 5_000 },
  [QueueName.EMAILS]: { concurrency: 3, attempts: 5, backoffMs: 10_000 },
  [QueueName.EXPORTS]: { concurrency: 2, attempts: 3, backoffMs: 5_000 },
};

function positiveInteger(
  value: number | string | undefined,
  fallback: number,
): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

/** Resolves the runtime options for every queue from config/env. */
export function resolveQueueConfig(
  config: ConfigService,
): Record<QueueName, QueueRuntimeConfig> {
  return QUEUE_NAMES.reduce(
    (acc, name) => {
      const keys = ENV_KEYS[name];
      const defaults = QUEUE_DEFAULTS[name];
      acc[name] = {
        name,
        concurrency: positiveInteger(
          config.get<number | string>(keys.concurrency),
          defaults.concurrency,
        ),
        attempts: positiveInteger(
          config.get<number | string>(keys.attempts),
          defaults.attempts,
        ),
        backoffMs: positiveInteger(
          config.get<number | string>(keys.backoffMs),
          defaults.backoffMs,
        ),
      };
      return acc;
    },
    {} as Record<QueueName, QueueRuntimeConfig>,
  );
}

/**
 * The queue runtime connects to Redis, so it is skipped while the OpenAPI
 * schema is generated (the SDK script sets SDK_GENERATE=1 and boots the app
 * without any infrastructure).
 */
export function isQueueRuntimeEnabled(): boolean {
  return process.env.SDK_GENERATE !== '1';
}

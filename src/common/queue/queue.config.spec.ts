import { ConfigService } from '@nestjs/config';
import { QueueName } from './queue.constants';
import { QUEUE_DEFAULTS, resolveQueueConfig } from './queue.config';

function configWith(values: Record<string, unknown>): ConfigService {
  return {
    get: (key: string, fallback?: unknown) =>
      key in values ? values[key] : fallback,
  } as unknown as ConfigService;
}

describe('resolveQueueConfig', () => {
  it('falls back to defaults when nothing is configured', () => {
    const config = resolveQueueConfig(configWith({}));

    expect(config[QueueName.WEBHOOKS]).toEqual({
      name: QueueName.WEBHOOKS,
      ...QUEUE_DEFAULTS[QueueName.WEBHOOKS],
    });
    expect(config[QueueName.EMAILS].concurrency).toBe(
      QUEUE_DEFAULTS[QueueName.EMAILS].concurrency,
    );
    expect(config[QueueName.EXPORTS].attempts).toBe(
      QUEUE_DEFAULTS[QueueName.EXPORTS].attempts,
    );
  });

  it('reads concurrency, attempts and back-off per queue', () => {
    const config = resolveQueueConfig(
      configWith({
        QUEUE_WEBHOOKS_CONCURRENCY: '12',
        QUEUE_WEBHOOKS_ATTEMPTS: '9',
        QUEUE_WEBHOOKS_BACKOFF_MS: '2500',
        QUEUE_EXPORTS_CONCURRENCY: 4,
        QUEUE_EXPORTS_ATTEMPTS: 2,
        QUEUE_EXPORTS_BACKOFF_MS: 1000,
      }),
    );

    expect(config[QueueName.WEBHOOKS]).toEqual({
      name: QueueName.WEBHOOKS,
      concurrency: 12,
      attempts: 9,
      backoffMs: 2500,
    });
    expect(config[QueueName.EXPORTS]).toEqual({
      name: QueueName.EXPORTS,
      concurrency: 4,
      attempts: 2,
      backoffMs: 1000,
    });
    // Untouched queue keeps its defaults
    expect(config[QueueName.EMAILS].attempts).toBe(
      QUEUE_DEFAULTS[QueueName.EMAILS].attempts,
    );
  });

  it('ignores non-numeric and non-positive values', () => {
    const config = resolveQueueConfig(
      configWith({
        QUEUE_EMAILS_CONCURRENCY: 'many',
        QUEUE_EMAILS_ATTEMPTS: '0',
        QUEUE_EMAILS_BACKOFF_MS: '-10',
      }),
    );

    expect(config[QueueName.EMAILS]).toMatchObject({
      concurrency: QUEUE_DEFAULTS[QueueName.EMAILS].concurrency,
      attempts: QUEUE_DEFAULTS[QueueName.EMAILS].attempts,
      backoffMs: QUEUE_DEFAULTS[QueueName.EMAILS].backoffMs,
    });
  });
});

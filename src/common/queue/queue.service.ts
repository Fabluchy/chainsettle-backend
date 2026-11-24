import {
  BadRequestException,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Job, JobsOptions, Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import { MetricsService } from '../metrics/metrics.service';
import { QueueName, QUEUE_NAMES } from './queue.constants';
import {
  isQueueRuntimeEnabled,
  QueueRuntimeConfig,
  resolveQueueConfig,
} from './queue.config';

export type QueueJobHandler = (job: Job) => Promise<unknown>;

export interface QueueStats {
  name: QueueName;
  available: boolean;
  paused: boolean;
  workerRegistered: boolean;
  concurrency: number;
  retryPolicy: { attempts: number; backoffMs: number };
  counts: Record<string, number>;
}

const DEPTH_STATES = ['waiting', 'active', 'delayed', 'failed'] as const;

/**
 * Owns the BullMQ queues and their workers.
 *
 * Jobs live in Redis, so queued work survives process restarts; a job is only
 * acknowledged once a worker completes it, and failures are retried with the
 * per-queue exponential back-off configured by QUEUE_*_ATTEMPTS /
 * QUEUE_*_BACKOFF_MS. Depth and throughput are exported as Prometheus metrics
 * and surfaced to admins through GET /admin/queues.
 *
 * Producers are created for every queue on boot. A worker is only started when
 * a feature module registers a handler for its queue.
 */
@Injectable()
export class QueueService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(QueueService.name);
  private readonly queues = new Map<QueueName, Queue>();
  private readonly workers = new Map<QueueName, Worker>();
  private readonly connections: Redis[] = [];
  private readonly prefix: string;
  private redisUrl: string;
  private configs: Record<QueueName, QueueRuntimeConfig> | null = null;
  private metricsTimer?: NodeJS.Timeout;

  constructor(
    private readonly config: ConfigService,
    private readonly metrics: MetricsService,
  ) {
    this.prefix = this.config.get<string>('QUEUE_PREFIX', 'chainsettle');
    this.redisUrl = this.config.get<string>(
      'REDIS_URL',
      'redis://localhost:6379',
    );
  }

  async onModuleInit(): Promise<void> {
    if (!isQueueRuntimeEnabled()) {
      this.logger.warn('Queue runtime disabled (SDK_GENERATE=1)');
      return;
    }

    this.configs = resolveQueueConfig(this.config);
    const producer = this.createConnection(1);

    for (const name of QUEUE_NAMES) {
      const cfg = this.configs[name];
      const queue = new Queue(name, {
        connection: producer,
        prefix: this.prefix,
        defaultJobOptions: {
          attempts: cfg.attempts,
          backoff: { type: 'exponential', delay: cfg.backoffMs },
          removeOnComplete: { age: 60 * 60, count: 1_000 },
          removeOnFail: { age: 7 * 24 * 60 * 60, count: 5_000 },
        },
      });
      queue.on('error', (err: Error) =>
        this.logger.error(`[queue:${name}] ${err.message}`),
      );
      this.queues.set(name, queue);
    }

    const intervalMs = Number(
      this.config.get<number | string>('QUEUE_METRICS_INTERVAL_MS', 15_000),
    );
    if (Number.isFinite(intervalMs) && intervalMs > 0) {
      this.metricsTimer = setInterval(() => {
        void this.refreshQueueMetrics();
      }, intervalMs);
      this.metricsTimer.unref?.();
    }

    await this.refreshQueueMetrics();
    this.logger.log(`BullMQ queues ready: ${QUEUE_NAMES.join(', ')}`);
  }

  async onModuleDestroy(): Promise<void> {
    if (this.metricsTimer) clearInterval(this.metricsTimer);
    await Promise.allSettled([...this.workers.values()].map((w) => w.close()));
    await Promise.allSettled([...this.queues.values()].map((q) => q.close()));
    await Promise.allSettled(this.connections.map((c) => c.quit()));
  }

  /**
   * Starts the worker that drains `name` with the configured concurrency.
   * Called once per queue from the owning feature module.
   */
  registerHandler(name: QueueName, handler: QueueJobHandler): void {
    if (this.workers.has(name)) return;
    const cfg = this.configs?.[name];
    const queue = this.queues.get(name);
    if (!cfg || !queue) {
      this.logger.debug(`Queue ${name} unavailable — worker not started`);
      return;
    }

    const worker = new Worker(name, async (job) => handler(job), {
      connection: this.createConnection(null),
      prefix: this.prefix,
      concurrency: cfg.concurrency,
    });

    worker.on('completed', (job) => {
      this.metrics.recordQueueJob(name, job.name, 'completed');
      this.logger.debug(
        `[queue:${name}] completed ${job.name} (${job.id}) in ${job.finishedOn && job.processedOn ? job.finishedOn - job.processedOn : 0}ms`,
      );
    });

    worker.on('failed', (job, err) => {
      const maxAttempts = job?.opts?.attempts ?? cfg.attempts;
      const finalFailure = (job?.attemptsMade ?? maxAttempts) >= maxAttempts;
      this.metrics.recordQueueJob(
        name,
        job?.name ?? 'unknown',
        finalFailure ? 'failed' : 'retried',
      );
      this.logger.warn(
        `[queue:${name}] ${job?.name ?? 'unknown'} failed ` +
          `(attempt ${job?.attemptsMade ?? 1}/${maxAttempts}): ${err.message}`,
      );
    });

    worker.on('error', (err: Error) =>
      this.logger.error(`[queue:${name}] worker error: ${err.message}`),
    );

    this.workers.set(name, worker);
    this.logger.log(
      `Worker for queue '${name}' started (concurrency=${cfg.concurrency}, attempts=${cfg.attempts})`,
    );
  }

  /**
   * Enqueues a job. Returns the job id, or null when the queue runtime is
   * disabled (callers then fall back to in-process work). A Redis failure is
   * thrown so producers can decide whether to fall back.
   */
  async enqueue<T extends object>(
    name: QueueName,
    jobName: string,
    data: T,
    options?: JobsOptions,
  ): Promise<string | null> {
    const queue = this.queues.get(name);
    if (!queue) return null;

    const job = await queue.add(jobName, data, options);
    this.metrics.recordQueueJob(name, jobName, 'enqueued');
    return job.id ?? null;
  }

  async getJob(name: QueueName, jobId: string): Promise<Job | undefined> {
    const queue = this.queues.get(name);
    if (!queue) return undefined;
    return queue.getJob(jobId);
  }

  /** Depth, retry policy and worker state for every queue (admin view). */
  async getStats(): Promise<QueueStats[]> {
    const stats: QueueStats[] = [];

    for (const name of QUEUE_NAMES) {
      const cfg = this.configs?.[name];
      const queue = this.queues.get(name);
      const retryPolicy = {
        attempts: cfg?.attempts ?? 0,
        backoffMs: cfg?.backoffMs ?? 0,
      };

      if (!queue || !cfg) {
        stats.push({
          name,
          available: false,
          paused: false,
          workerRegistered: false,
          concurrency: cfg?.concurrency ?? 0,
          retryPolicy,
          counts: {},
        });
        continue;
      }

      const counts = await queue.getJobCounts(
        'waiting',
        'active',
        'delayed',
        'completed',
        'failed',
        'paused',
      );

      stats.push({
        name,
        available: true,
        paused: await queue.isPaused(),
        workerRegistered: this.workers.has(name),
        concurrency: cfg.concurrency,
        retryPolicy,
        counts,
      });
    }

    return stats;
  }

  async pause(name: string): Promise<{ queue: QueueName; paused: true }> {
    const queue = this.requireQueue(name);
    await queue.pause();
    this.logger.log(`Queue '${name}' paused`);
    return { queue: name as QueueName, paused: true };
  }

  async resume(name: string): Promise<{ queue: QueueName; paused: false }> {
    const queue = this.requireQueue(name);
    await queue.resume();
    this.logger.log(`Queue '${name}' resumed`);
    return { queue: name as QueueName, paused: false };
  }

  /** Pushes current queue depths into Prometheus gauges. */
  async refreshQueueMetrics(): Promise<void> {
    for (const [name, queue] of this.queues) {
      try {
        const counts = await queue.getJobCounts(...DEPTH_STATES);
        for (const [state, count] of Object.entries(counts)) {
          this.metrics.setQueueDepth(name, state, Number(count));
        }
      } catch (err) {
        this.logger.warn(
          `Failed to refresh queue metrics for '${name}': ${(err as Error).message}`,
        );
      }
    }
  }

  private requireQueue(name: string): Queue {
    if (!isQueueName(name)) {
      throw new BadRequestException(
        `Unknown queue '${name}'. Expected one of: ${QUEUE_NAMES.join(', ')}`,
      );
    }
    const queue = this.queues.get(name);
    if (!queue) {
      throw new ServiceUnavailableException(
        'Queue runtime is unavailable (not connected to Redis)',
      );
    }
    return queue;
  }

  private createConnection(maxRetriesPerRequest: number | null): Redis {
    const connection = new Redis(this.redisUrl, {
      maxRetriesPerRequest,
      enableReadyCheck: false,
    });
    connection.on('error', (err) => {
      this.logger.error(`Queue Redis connection error: ${err.message}`);
    });
    this.connections.push(connection);
    return connection;
  }
}

export function isQueueName(name: string): name is QueueName {
  return (QUEUE_NAMES as readonly string[]).includes(name);
}

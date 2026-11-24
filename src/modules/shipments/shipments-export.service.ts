import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { QueueService } from '../../common/queue/queue.service';
import { RedisService } from '../../common/redis/redis.service';
import {
  EXPORT_GENERATE_JOB,
  ExportArtifact,
  ExportFormat,
  ExportJobData,
  ExportJobResult,
  exportResultKey,
} from '../../common/queue/queue.constants';
import { QueueName } from '../../common/queue/queue.constants';

/** Identity of the caller a queued export belongs to. */
export interface ExportRequester {
  stellarAddress: string;
  isAdmin: boolean;
}

export interface ExportJobStatus {
  jobId: string;
  status: 'queued' | 'processing' | 'completed' | 'failed';
  format: ExportFormat | null;
  createdAt: string | null;
  finishedAt: string | null;
  result: ExportJobResult | null;
  error: string | null;
  downloadUrl: string | null;
}

/**
 * Queued export jobs (#661). The request only enqueues a job that carries the
 * caller's identity; the worker re-runs the query and builds the CSV/PDF, so
 * the job survives a restart. The finished file is cached in Redis for an hour
 * and downloaded through the job endpoints.
 */
@Injectable()
export class ShipmentsExportService {
  private readonly logger = new Logger(ShipmentsExportService.name);

  constructor(
    private readonly queue: QueueService,
    private readonly redis: RedisService,
  ) {}

  async enqueue(requester: ExportRequester, format: ExportFormat) {
    const data: ExportJobData = {
      stellarAddress: requester.stellarAddress,
      isAdmin: requester.isAdmin,
      format,
    };

    const jobId = await this.queue.enqueue(
      QueueName.EXPORTS,
      EXPORT_GENERATE_JOB,
      data,
    );

    if (!jobId) {
      throw new ServiceUnavailableException(
        'Export queue is unavailable — try again shortly',
      );
    }

    this.logger.log(
      `Queued ${format} export job ${jobId} for ${requester.stellarAddress}`,
    );

    return {
      jobId,
      status: 'queued' as const,
      format,
      statusUrl: `/api/v1/shipments/export-jobs/${jobId}`,
    };
  }

  async getStatus(
    jobId: string,
    requester: ExportRequester,
  ): Promise<ExportJobStatus> {
    const job = await this.findOwnedJob(jobId, requester);
    const state = await job.getState();
    const data = job.data as ExportJobData;

    const status: ExportJobStatus['status'] =
      state === 'completed'
        ? 'completed'
        : state === 'failed'
          ? 'failed'
          : state === 'active'
            ? 'processing'
            : 'queued';

    return {
      jobId: String(job.id),
      status,
      format: data?.format ?? null,
      createdAt: job.timestamp ? new Date(job.timestamp).toISOString() : null,
      finishedAt: job.finishedOn
        ? new Date(job.finishedOn).toISOString()
        : null,
      result:
        status === 'completed'
          ? ((job.returnvalue as ExportJobResult | null) ?? null)
          : null,
      error:
        status === 'failed' ? (job.failedReason ?? 'Export job failed') : null,
      downloadUrl:
        status === 'completed'
          ? `/api/v1/shipments/export-jobs/${job.id}/download`
          : null,
    };
  }

  async download(jobId: string, requester: ExportRequester) {
    const job = await this.findOwnedJob(jobId, requester);
    const state = await job.getState();
    if (state !== 'completed') {
      throw new BadRequestException(
        `Export job ${jobId} is not ready (state: ${state})`,
      );
    }

    const raw = await this.redis.get(exportResultKey(String(job.id)));
    if (!raw) {
      throw new NotFoundException(
        'Export result has expired — request a new export',
      );
    }

    const artifact = JSON.parse(raw) as ExportArtifact;
    return {
      filename: artifact.filename,
      contentType: artifact.contentType,
      buffer: Buffer.from(artifact.data, 'base64'),
    };
  }

  private async findOwnedJob(jobId: string, requester: ExportRequester) {
    const job = await this.queue.getJob(QueueName.EXPORTS, jobId);
    if (!job) throw new NotFoundException('Export job not found');

    const data = job.data as ExportJobData;
    const owned =
      data?.isAdmin ||
      (data?.stellarAddress &&
        data.stellarAddress === requester.stellarAddress);
    if (!owned) throw new NotFoundException('Export job not found');

    return job;
  }
}

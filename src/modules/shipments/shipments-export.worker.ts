import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Job } from 'bullmq';
import { QueueService } from '../../common/queue/queue.service';
import { RedisService } from '../../common/redis/redis.service';
import {
  EXPORT_RESULT_TTL_SECONDS,
  ExportArtifact,
  ExportJobData,
  ExportJobResult,
  exportResultKey,
  QueueName,
} from '../../common/queue/queue.constants';
import { ShipmentsService } from './shipments.service';

/**
 * Builds exports off the request path (#661). The job carries the caller's
 * identity, so it can be re-run from scratch after a restart; the finished
 * file is written to Redis and described in the job's return value.
 */
@Injectable()
export class ShipmentsExportWorker implements OnModuleInit {
  private readonly logger = new Logger(ShipmentsExportWorker.name);

  constructor(
    private readonly queue: QueueService,
    private readonly shipments: ShipmentsService,
    private readonly redis: RedisService,
  ) {}

  onModuleInit(): void {
    this.queue.registerHandler(QueueName.EXPORTS, (job) => this.handle(job));
  }

  private async handle(job: Job): Promise<ExportJobResult> {
    const { stellarAddress, isAdmin, format } = (job.data ??
      {}) as ExportJobData;

    if (!stellarAddress || (format !== 'csv' && format !== 'pdf')) {
      throw new Error(
        `Malformed export job ${job.id}: stellarAddress and format are required`,
      );
    }

    const shipments = await this.shipments.exportForUser(
      stellarAddress,
      Boolean(isAdmin),
    );
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

    const artifact =
      format === 'pdf'
        ? {
            filename: `chainsettle-export-${timestamp}.pdf`,
            contentType: 'application/pdf',
            buffer: await this.shipments.buildPdf(shipments),
          }
        : {
            filename: `chainsettle-export-${timestamp}.csv`,
            contentType: 'text/csv',
            buffer: Buffer.from(this.shipments.buildCsv(shipments), 'utf-8'),
          };

    const payload: ExportArtifact = {
      filename: artifact.filename,
      contentType: artifact.contentType,
      data: artifact.buffer.toString('base64'),
    };

    await this.redis.set(
      exportResultKey(String(job.id)),
      JSON.stringify(payload),
      EXPORT_RESULT_TTL_SECONDS,
    );

    this.logger.log(
      `Export job ${job.id} finished: ${artifact.filename} (${artifact.buffer.byteLength} bytes, ${shipments.length} shipment(s))`,
    );

    return {
      format,
      filename: artifact.filename,
      contentType: artifact.contentType,
      bytes: artifact.buffer.byteLength,
    };
  }
}

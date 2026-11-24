import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Job } from 'bullmq';
import { QueueService } from '../../common/queue/queue.service';
import {
  QueueName,
  WebhookDeliveryJobData,
} from '../../common/queue/queue.constants';
import { WebhooksService } from './webhooks.service';

/**
 * Drains the webhooks queue: one job per endpoint per event (#661).
 *
 * A job that throws is retried by BullMQ with the queue's back-off policy. A
 * delivery that fails at the HTTP level is recorded by WebhooksService and
 * handed to its existing scheduled retry policy, exactly as before.
 */
@Injectable()
export class WebhookDeliveryWorker implements OnModuleInit {
  private readonly logger = new Logger(WebhookDeliveryWorker.name);

  constructor(
    private readonly queue: QueueService,
    private readonly webhooks: WebhooksService,
  ) {}

  onModuleInit(): void {
    this.queue.registerHandler(QueueName.WEBHOOKS, (job) => this.handle(job));
  }

  private async handle(job: Job): Promise<void> {
    const data = (job.data ?? {}) as WebhookDeliveryJobData;
    if (!data.endpointId || !data.eventType) {
      this.logger.warn(
        `[queue:webhooks] Dropping malformed job ${job.id}: missing endpointId/eventType`,
      );
      return;
    }

    await this.webhooks.deliverToEndpoint(
      data.endpointId,
      data.eventType,
      data.payload ?? {},
    );
  }
}

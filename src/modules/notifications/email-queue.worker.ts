import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Job } from 'bullmq';
import { QueueService } from '../../common/queue/queue.service';
import { EmailJobData, QueueName } from '../../common/queue/queue.constants';
import { NotificationsService } from './notifications.service';

/**
 * Sends queued emails (#661). Emails are rendered before enqueueing, so a job
 * that runs after a restart still has everything it needs. Failures throw and
 * are retried by BullMQ with exponential back-off.
 */
@Injectable()
export class EmailQueueWorker implements OnModuleInit {
  private readonly logger = new Logger(EmailQueueWorker.name);

  constructor(
    private readonly queue: QueueService,
    private readonly notifications: NotificationsService,
  ) {}

  onModuleInit(): void {
    this.queue.registerHandler(QueueName.EMAILS, (job) => this.handle(job));
  }

  private async handle(job: Job): Promise<void> {
    const mail = (job.data ?? {}) as EmailJobData;
    if (!mail.to || !mail.subject) {
      this.logger.warn(
        `[queue:emails] Dropping malformed job ${job.id}: missing to/subject`,
      );
      return;
    }

    await this.notifications.deliverQueuedEmail(mail);
  }
}

import { Global, Module } from '@nestjs/common';
import { QueueService } from './queue.service';

/**
 * Global so feature modules can enqueue work and register their queue workers
 * without importing each other.
 */
@Global()
@Module({
  providers: [QueueService],
  exports: [QueueService],
})
export class QueueModule {}

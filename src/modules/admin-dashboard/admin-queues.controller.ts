import {
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  UseGuards,
} from '@nestjs/common';
import {
  ApiBearerAuth,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { UserRole } from '@prisma/client';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';
import { Roles } from '../../common/decorators/roles.decorator';
import { QueueService, QueueStats } from '../../common/queue/queue.service';
import { QUEUE_NAMES } from '../../common/queue/queue.constants';

/**
 * Admin view of the BullMQ queues (#661): depth per state, configured
 * concurrency and retry policy, plus pause/resume controls for draining before
 * a deploy or shedding load during an incident.
 */
@ApiTags('admin')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('admin/queues')
export class AdminQueuesController {
  constructor(private readonly queues: QueueService) {}

  @Get()
  @Roles(UserRole.ADMIN)
  @ApiOperation({
    summary: '[Admin] List background queues with depth and retry policy',
    description:
      'Reports waiting, active, delayed, completed, failed and paused counts ' +
      'for every BullMQ queue (webhooks, emails, exports), along with the ' +
      'configured concurrency and retry policy.',
  })
  @ApiResponse({ status: 200, description: 'Queue statistics' })
  @ApiResponse({ status: 401, description: 'Unauthorized' })
  @ApiResponse({ status: 403, description: 'Admin access required' })
  async list(): Promise<QueueStats[]> {
    return this.queues.getStats();
  }

  @Post(':name/pause')
  @Roles(UserRole.ADMIN)
  @HttpCode(HttpStatus.OK)
  @ApiParam({ name: 'name', enum: QUEUE_NAMES })
  @ApiOperation({
    summary: '[Admin] Pause a background queue',
    description:
      'Paused queues stop handing out jobs; jobs already in Redis stay there ' +
      'until the queue is resumed.',
  })
  @ApiResponse({ status: 200, description: 'Queue paused' })
  @ApiResponse({ status: 400, description: 'Unknown queue' })
  @ApiResponse({ status: 503, description: 'Queue runtime unavailable' })
  pause(@Param('name') name: string) {
    return this.queues.pause(name);
  }

  @Post(':name/resume')
  @Roles(UserRole.ADMIN)
  @HttpCode(HttpStatus.OK)
  @ApiParam({ name: 'name', enum: QUEUE_NAMES })
  @ApiOperation({ summary: '[Admin] Resume a paused background queue' })
  @ApiResponse({ status: 200, description: 'Queue resumed' })
  @ApiResponse({ status: 400, description: 'Unknown queue' })
  @ApiResponse({ status: 503, description: 'Queue runtime unavailable' })
  resume(@Param('name') name: string) {
    return this.queues.resume(name);
  }
}

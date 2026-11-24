import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsIn, IsOptional } from 'class-validator';
import { ExportFormat } from '../../../common/queue/queue.constants';

export class CreateExportJobDto {
  @ApiPropertyOptional({
    enum: ['csv', 'pdf'],
    default: 'csv',
    description: 'Export file format (default: csv)',
  })
  @IsOptional()
  @IsIn(['csv', 'pdf'])
  format?: ExportFormat;
}

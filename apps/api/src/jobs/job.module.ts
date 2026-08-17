import { Module } from '@nestjs/common';
import { JobsController } from './job.controller';
import { JobsService } from './job.service';


@Module({
  imports: [],
  controllers: [JobsController],
  providers: [JobsService],
})
export class JobsModule {}

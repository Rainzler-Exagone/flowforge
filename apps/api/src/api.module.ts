import { Module } from '@nestjs/common';
import { JobsModule } from './jobs/job.module';


@Module({
  imports: [JobsModule],
  controllers: [],
  providers: [],
})
export class ApiModule {}

import { Module } from '@nestjs/common';
import { ImageWorkerService } from './image-worker.service';

@Module({
  imports: [],
  controllers: [],
  providers: [ImageWorkerService],
})
export class ImageWorkerModule {}

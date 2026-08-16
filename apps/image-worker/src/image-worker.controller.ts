import { Controller, Get } from '@nestjs/common';
import { ImageWorkerService } from './image-worker.service';

@Controller()
export class ImageWorkerController {
  constructor(private readonly imageWorkerService: ImageWorkerService) {}

  @Get()
  getHello(): string {
    return this.imageWorkerService.getHello();
  }
}

import { Injectable } from '@nestjs/common';

@Injectable()
export class ImageWorkerService {
  getHello(): string {
    return 'Hello World!';
  }
}

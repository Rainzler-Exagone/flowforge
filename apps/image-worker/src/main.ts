import { NestFactory } from '@nestjs/core';
import { ImageWorkerModule } from './image-worker.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(ImageWorkerModule);
}
bootstrap();

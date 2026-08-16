import { NestFactory } from '@nestjs/core';
import { ImageWorkerModule } from './image-worker.module';

async function bootstrap() {
  const app = await NestFactory.create(ImageWorkerModule);
  await app.listen(process.env.port ?? 3000);
}
bootstrap();

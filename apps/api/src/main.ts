import { NestFactory } from '@nestjs/core';
import { Logger } from 'pino-nestjs';

import { ApiModule } from './api.module';

async function bootstrap() {
  const app = await NestFactory.create(ApiModule, {
    bufferLogs: true,
  });

  app.useLogger(app.get(Logger));


  await app.listen(process.env.PORT ?? 3000);
}

bootstrap();
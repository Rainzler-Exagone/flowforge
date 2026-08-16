import { NestFactory } from '@nestjs/core';
import { ApiModule } from './api.module';
import { testDatabaseConnection } from './db';

async function bootstrap() {
    await testDatabaseConnection();
  
  const app = await NestFactory.create(ApiModule);
  await app.listen(process.env.port ?? 3000,'0.0.0.0');
}
bootstrap();

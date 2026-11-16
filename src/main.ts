import 'reflect-metadata';
import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import type { AppConfig } from './config';
import { APP_CONFIG } from './infrastructure/infrastructure.service';

const app = await NestFactory.create(AppModule, {
  logger: new ConsoleLogger({ json: true }),
});
app.enableShutdownHooks();
const config = app.get<AppConfig>(APP_CONFIG);
await app.listen(config.port, config.host);

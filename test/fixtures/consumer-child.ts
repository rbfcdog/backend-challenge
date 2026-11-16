import 'reflect-metadata';
import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { InfrastructureService } from '../../src/infrastructure/infrastructure.service';
import { WorkerService } from '../../src/workers/worker.service';

// Automatic workers are disabled by the parent. All SQL/SQS operations remain real.
const app = await NestFactory.create(AppModule, {
  logger: new ConsoleLogger({ json: true }),
});
app.enableShutdownHooks();
await app.init();
if (process.env.TEST_BLOCK_ACK === 'true') {
  app.get(InfrastructureService).sqs.middlewareStack.add((next, context) => async args => {
    if (context.commandName === 'DeleteMessageCommand') {
      // This middleware runs before the actual HTTP dispatch. The use case has
      // committed, but no DeleteMessage has reached SQS. Only the parent kills us.
      console.log('AFTER_COMMIT_BEFORE_ACK');
      await Promise.withResolvers<never>().promise;
    }
    return next(args);
  }, { step: 'finalizeRequest', name: 'testCrashBeforeAck', priority: 'high' });
}
console.log('CONSUMER_READY');
try {
  await app.get(WorkerService).consumeOnce();
} finally {
  await app.close();
}

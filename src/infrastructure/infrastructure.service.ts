import { GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Inject, Injectable } from '@nestjs/common';
import type { OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import type { AppConfig } from '../config';
import { databaseConfig } from './database/orm.config';

export const APP_CONFIG = Symbol('APP_CONFIG');

@Injectable()
export class InfrastructureService implements OnModuleInit, OnApplicationShutdown {
  private orm: MikroORM | undefined;
  readonly sqs: SQSClient;

  constructor(@Inject(APP_CONFIG) private readonly config: AppConfig) {
    this.sqs = new SQSClient({
      region: config.awsRegion,
      ...(config.sqsEndpoint ? { endpoint: config.sqsEndpoint } : {}),
      maxAttempts: 1,
      requestHandler: {
        connectionTimeout: 3000,
        requestTimeout: 10_000,
        throwOnRequestTimeout: true,
      },
    });
  }

  get db(): MikroORM {
    if (!this.orm) throw new Error('PostgreSQL not initialized');
    return this.orm;
  }

  async onModuleInit(): Promise<void> {
    this.orm = await MikroORM.init(databaseConfig(this.config.databaseUrl));
  }

  async readiness(): Promise<{ postgres: boolean; sqs: boolean }> {
    const [postgres, sqs] = await Promise.allSettled([
      this.orm
        ? this.orm.em.getConnection().execute('select 1')
        : Promise.reject(new Error('PostgreSQL not initialized')),
      Promise.all([this.config.sqsQueueUrl, this.config.sqsDlqUrl, this.config.sqsEventsQueueUrl].map(QueueUrl =>
        this.sqs.send(
          new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }),
          { requestTimeout: 3000 },
        ),
      )),
    ]);
    return { postgres: postgres.status === 'fulfilled', sqs: sqs.status === 'fulfilled' };
  }

  async onApplicationShutdown(): Promise<void> {
    this.sqs.destroy();
    await this.orm?.close(true);
  }
}

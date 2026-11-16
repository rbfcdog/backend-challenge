import { Module } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { ProcessorService } from './application/processor.service';
import { Metrics } from './metrics';
import { WalletsController, WageringController, ProviderTransactionsController } from './http/financial.controller';
import { MetricsController } from './http/metrics.controller';
import { FinancialExceptionFilter } from './http/financial-exception.filter';
import { ProviderIdentityPort, LocalProviderIdentity } from './http/provider-identity.port';
import { WorkerService } from './workers/worker.service';
import { loadConfig } from './config';
import { HealthController } from './health/health.controller';
import { APP_CONFIG, InfrastructureService } from './infrastructure/infrastructure.service';

@Module({
  controllers: [HealthController, WalletsController, WageringController, ProviderTransactionsController, MetricsController],
  providers: [
    { provide: APP_CONFIG, useFactory: () => loadConfig() },
    InfrastructureService,
    Metrics,
    ProcessorService,
    WorkerService,
    { provide: ProviderIdentityPort, useClass: LocalProviderIdentity },
    { provide: APP_FILTER, useClass: FinancialExceptionFilter },
  ],
})
export class AppModule {}

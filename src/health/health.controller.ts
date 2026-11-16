import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { InfrastructureService } from '../infrastructure/infrastructure.service';

@Controller('health')
export class HealthController {
  constructor(@Inject(InfrastructureService) private readonly infrastructure: InfrastructureService) {}

  @Get('live')
  live() {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready() {
    const dependencies = await this.infrastructure.readiness();
    if (!dependencies.postgres || !dependencies.sqs) {
      throw new ServiceUnavailableException({ status: 'unavailable', dependencies });
    }
    return { status: 'ok', dependencies };
  }
}

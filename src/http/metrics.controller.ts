import { Controller, Get, Header, Inject, Req, Res } from '@nestjs/common';
import { Metrics } from '../metrics';
import { correlationId } from './http-context';
import type { HttpRequest, HttpResponse } from './http-context';

@Controller('metrics')
export class MetricsController {
  constructor(@Inject(Metrics) private readonly metrics: Metrics) {}

  @Get()
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  get(@Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse): string {
    correlationId(request, response);
    return this.metrics.render();
  }
}

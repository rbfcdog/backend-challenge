import { Catch, HttpException, Inject, Logger } from '@nestjs/common';
import type { ArgumentsHost, ExceptionFilter } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import { AppError, isTransientInfrastructure } from '../application/contracts';
import { correlationId } from './http-context';
import type { HttpRequest, HttpResponse } from './http-context';

const httpErrors: Readonly<Record<number, readonly [string, string]>> = {
  400: ['INVALID_REQUEST', 'Invalid request'],
  401: ['UNAUTHORIZED', 'Authentication required'],
  403: ['FORBIDDEN', 'Access denied'],
  404: ['NOT_FOUND', 'Resource not found'],
  405: ['METHOD_NOT_ALLOWED', 'Method not allowed'],
  408: ['REQUEST_TIMEOUT', 'Request timed out'],
  409: ['CONFLICT', 'Request conflicts with existing state'],
  413: ['PAYLOAD_TOO_LARGE', 'Request payload is too large'],
  415: ['UNSUPPORTED_MEDIA_TYPE', 'Unsupported media type'],
  422: ['BUSINESS_RULE_REJECTED', 'Transaction rejected'],
  429: ['TOO_MANY_REQUESTS', 'Too many requests'],
  500: ['INTERNAL_ERROR', 'Internal server error'],
  502: ['BAD_GATEWAY', 'Upstream service unavailable'],
  503: ['INFRASTRUCTURE_UNAVAILABLE', 'Service temporarily unavailable'],
  504: ['GATEWAY_TIMEOUT', 'Upstream service timed out'],
};


function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : undefined;
}


export interface FinancialHttpError {
  readonly statusCode: number;
  readonly code: string;
  readonly message: string;
}

/** Never returns driver messages, SQL, request bodies, or exception stacks. */
export function mapFinancialError(exception: unknown): FinancialHttpError {
  if (exception instanceof AppError) {
    return { statusCode: exception.status, code: exception.code, message: exception.message };
  }
  if (exception instanceof HttpException) {
    const statusCode = exception.getStatus();
    const [fallbackCode, message] = httpErrors[statusCode] ?? ['HTTP_ERROR', 'Request failed'];
    const explicitCode = record(exception.getResponse())?.['code'];
    const code = typeof explicitCode === 'string' && /^[A-Z][A-Z0-9_]{0,95}$/.test(explicitCode)
      ? explicitCode
      : fallbackCode;
    return { statusCode, code, message };
  }
  // Body-parser errors are not HttpException instances and may contain the body.
  const error = record(exception);
  if (error?.['type'] === 'entity.parse.failed' && error['status'] === 400) {
    return { statusCode: 400, code: 'INVALID_JSON', message: 'Malformed JSON request body' };
  }
  if (error?.['type'] === 'entity.too.large' && error['status'] === 413) {
    return { statusCode: 413, code: 'PAYLOAD_TOO_LARGE', message: 'Request payload is too large' };
  }
  return isTransientInfrastructure(exception)
    ? { statusCode: 503, code: 'INFRASTRUCTURE_UNAVAILABLE', message: 'Service temporarily unavailable' }
    : { statusCode: 500, code: 'INTERNAL_ERROR', message: 'Internal server error' };
}

@Catch()
export class FinancialExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(FinancialExceptionFilter.name);

  constructor(@Inject(HttpAdapterHost) private readonly adapterHost: HttpAdapterHost) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const response = http.getResponse<HttpResponse>();
    const request = http.getRequest<HttpRequest>();
    const mapped = mapFinancialError(exception);
    let id: string;
    try {
      id = correlationId(request, response);
    } catch {
      // The helper has stored a safe replacement before rejecting an invalid header.
      id = correlationId(request, response);
    }
    const log = { event: 'http_request_failed', correlationId: id, messageId: null,
      transactionId: request.financialContext?.transactionId ?? null,
      walletId: request.financialContext?.walletId ?? null, providerId: request.financialContext?.providerId ?? null,
      code: mapped.code, statusCode: mapped.statusCode };
    if (mapped.statusCode >= 500) this.logger.error(log);
    else this.logger.warn(log);
    this.adapterHost.httpAdapter.reply(response, { ...mapped, correlationId: id }, mapped.statusCode);
  }
}

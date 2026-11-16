import { AppError } from '../application/contracts';

const correlationKey = Symbol('httpCorrelationId');

export interface HttpRequest {
  readonly headers: Record<string, unknown>;
  [correlationKey]?: string;
  financialContext?: { walletId?: string; transactionId?: string; providerId?: string };
}

export interface HttpResponse {
  status(code: number): unknown;
  setHeader(name: string, value: string): unknown;
}

/** A bounded, printable transport identifier, never an authentication credential. */
export function correlationId(request: HttpRequest, response: HttpResponse): string {
  const existing = request[correlationKey];
  if (existing !== undefined) {
    response.setHeader('X-Correlation-Id', existing);
    return existing;
  }

  const supplied = request.headers['x-correlation-id'];
  const valid = typeof supplied === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(supplied);
  const id = valid ? supplied : crypto.randomUUID();
  request[correlationKey] = id;
  response.setHeader('X-Correlation-Id', id);
  if (supplied !== undefined && !valid) {
    throw new AppError(400, 'INVALID_CORRELATION_ID', 'X-Correlation-Id must be 1–128 ASCII identifier characters');
  }
  return id;
}

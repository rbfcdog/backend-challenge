import 'reflect-metadata';
import { expect, test } from 'bun:test';
import { HttpException } from '@nestjs/common';
import { AppError } from '../application/contracts';
import type { ProcessResult } from '../application/contracts';
import type { ProcessorService } from '../application/processor.service';
import { WageringController } from './financial.controller';
import { mapFinancialError } from './financial-exception.filter';
import { correlationId } from './http-context';
import type { HttpRequest, HttpResponse } from './http-context';
import { LocalProviderIdentity, ProviderIdentityPort } from './provider-identity.port';

test('HTTP boundaries retain correlation, sanitize errors, authorize providers and map every processing state', async () => {
  const headers: Record<string, string> = {};
  let status = 0;
  const response: HttpResponse = {
    status(code) { status = code; },
    setHeader(name, value) { headers[name] = value; },
  };
  const request: HttpRequest = { headers: { 'x-correlation-id': 'provider:request-1' } };
  expect(correlationId(request, response)).toBe('provider:request-1');
  expect(headers['X-Correlation-Id']).toBe('provider:request-1');
  const generated = correlationId({ headers: {} }, response);
  expect(generated).toMatch(/^[0-9a-f-]{36}$/);
  const invalidRequest: HttpRequest = { headers: { 'x-correlation-id': 'unsafe\r\nheader' } };
  expect(() => correlationId(invalidRequest, response)).toThrow(AppError);
  expect(correlationId(invalidRequest, response)).toBe(headers['X-Correlation-Id']!);
  expect(headers['X-Correlation-Id']).toMatch(/^[0-9a-f-]{36}$/);

  expect(mapFinancialError(new AppError(409, 'IDEMPOTENCY_CONFLICT')).code).toBe('IDEMPOTENCY_CONFLICT');
  const forbidden = mapFinancialError(new HttpException({ code: 'ACCESS_DENIED', message: 'private details' }, 403));
  expect(forbidden.statusCode).toBe(403);
  expect(forbidden.code).toBe('ACCESS_DENIED');
  expect(JSON.stringify(forbidden)).not.toContain('private details');
  expect(mapFinancialError({ cause: { code: '40P01', message: 'private SQL' } }).statusCode).toBe(503);
  expect(mapFinancialError({ code: '08006' }).statusCode).toBe(503);
  const unexpected = mapFinancialError({ code: '23505', message: 'private SQL' });
  expect(unexpected.statusCode).toBe(500);
  expect(JSON.stringify(unexpected)).not.toContain('private SQL');
  expect(mapFinancialError({ type: 'entity.parse.failed', status: 400, body: 'private financial payload' }).statusCode).toBe(400);
  const cyclic: Record<string, unknown> = {};
  cyclic['cause'] = cyclic;
  expect(mapFinancialError(cyclic).statusCode).toBe(500);

  let authorized = '';
  let processed = 0;
  let resultStatus = 'PROCESSED';
  class TestIdentity extends ProviderIdentityPort {
    override async authorize(providerId: string): Promise<void> { authorized = providerId; }
  }
  const processor = {
    async process(input: { providerId: string }, key: string, ctx: { correlationId: string }) {
      expect(authorized).toBe(input.providerId);
      processed++;
      return {
        transactionId: '0192f298-345e-7e38-af88-e43f851a819d',
        status: resultStatus,
        balance: { amount: '975.00', currency: 'BRL' },
        idempotentReplay: false,
      } as ProcessResult;
    },
  } as unknown as ProcessorService;
  const controller = new WageringController(processor, new TestIdentity());
  const body = {
    providerId: 'provider-a', externalTransactionId: 'transaction-123',
    playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1', walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
    roundId: 'round-987', gameId: 'fortune-chimp', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
  };
  const cases: readonly (readonly [string, number])[] = [
    ['PROCESSED', 200], ['PENDING', 202], ['PENDING_REFERENCE', 202], ['REJECTED', 422], ['FAILED', 503],
  ];
  for (const [state, expected] of cases) {
    resultStatus = state;
    await controller.submit(body, 'provider-a:transaction-123', request, response);
    expect(status).toBe(expected);
  }
  expect(processed).toBe(cases.length);
  await expect(controller.submit(body, undefined, request, response)).rejects.toMatchObject({ code: 'IDEMPOTENCY_KEY_REQUIRED' });
  expect(processed).toBe(cases.length);
  await expect(new LocalProviderIdentity().authorize('', request)).rejects.toBeInstanceOf(AppError);
});

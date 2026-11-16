import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res } from '@nestjs/common';
import { AppError, parseWager, parseWallet, text, uuid } from '../application/contracts';
import { ProcessorService } from '../application/processor.service';
import { correlationId } from './http-context';
import type { HttpRequest, HttpResponse } from './http-context';
import { ProviderIdentityPort } from './provider-identity.port';

@Controller('wallets')
export class WalletsController {
  constructor(@Inject(ProcessorService) private readonly processor: ProcessorService) {}

  @Post()
  @HttpCode(201)
  create(@Body() body: unknown, @Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse) {
    const ctx = { correlationId: correlationId(request, response) };
    return this.processor.createWallet(parseWallet(body), ctx);
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: unknown, @Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse) {
    correlationId(request, response);
    const id = uuid(walletId, 'walletId');
    request.financialContext = { walletId: id };
    return this.processor.getWallet(id);
  }

  @Get(':walletId/ledger')
  ledger(
    @Param('walletId') walletId: unknown,
    @Query('cursor') cursor: unknown,
    @Query('limit') limit: unknown,
    @Req() request: HttpRequest,
    @Res({ passthrough: true }) response: HttpResponse,
  ) {
    correlationId(request, response);
    // Reject arrays/objects at the transport boundary; semantic pagination belongs to the processor.
    if ((cursor !== undefined && typeof cursor !== 'string') || (limit !== undefined && typeof limit !== 'string')) {
      throw new AppError(400, 'INVALID_PAGINATION', 'cursor and limit must be single string values');
    }
    const id = uuid(walletId, 'walletId');
    request.financialContext = { walletId: id };
    return this.processor.getLedger(id, cursor, limit);
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId') walletId: unknown, @Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse) {
    const ctx = { correlationId: correlationId(request, response) };
    const id = uuid(walletId, 'walletId');
    request.financialContext = { walletId: id };
    return this.processor.reconcile(id, ctx);
  }
}

@Controller('wagering/transactions')
export class WageringController {
  constructor(
    @Inject(ProcessorService) private readonly processor: ProcessorService,
    @Inject(ProviderIdentityPort) private readonly identity: ProviderIdentityPort,
  ) {}

  @Post()
  @HttpCode(200)
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') key: unknown,
    @Req() request: HttpRequest,
    @Res({ passthrough: true }) response: HttpResponse,
  ) {
    const ctx = { correlationId: correlationId(request, response) };
    if (key === undefined) throw new AppError(400, 'IDEMPOTENCY_KEY_REQUIRED', 'Idempotency-Key is required');
    const idempotencyKey = text(key, 'Idempotency-Key');
    const input = parseWager(body);
    request.financialContext = { walletId: input.walletId, providerId: input.providerId };
    await this.identity.authorize(input.providerId, request);
    const result = await this.processor.process(input, idempotencyKey, ctx);
    switch (result.status) {
      case 'PROCESSED': response.status(200); break;
      case 'PENDING':
      case 'PENDING_REFERENCE': response.status(202); break;
      case 'REJECTED': response.status(422); break;
      case 'FAILED': response.status(503); break;
      default: throw new AppError(500, 'INVALID_TRANSACTION_STATUS');
    }
    return result;
  }

  @Get(':transactionId')
  get(@Param('transactionId') transactionId: unknown, @Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse) {
    correlationId(request, response);
    const id = uuid(transactionId, 'transactionId');
    request.financialContext = { transactionId: id };
    return this.processor.getTransaction(id);
  }
}

@Controller('providers')
export class ProviderTransactionsController {
  constructor(
    @Inject(ProcessorService) private readonly processor: ProcessorService,
    @Inject(ProviderIdentityPort) private readonly identity: ProviderIdentityPort,
  ) {}

  @Get(':providerId/wagering/transactions/:externalTransactionId')
  async get(
    @Param('providerId') providerId: unknown,
    @Param('externalTransactionId') externalTransactionId: unknown,
    @Req() request: HttpRequest,
    @Res({ passthrough: true }) response: HttpResponse,
  ) {
    correlationId(request, response);
    const provider = text(providerId, 'providerId');
    const externalId = text(externalTransactionId, 'externalTransactionId');
    request.financialContext = { providerId: provider };
    await this.identity.authorize(provider, request);
    return this.processor.getProviderTransaction(provider, externalId);
  }
}

import { Injectable } from '@nestjs/common';
import { text } from '../application/contracts';

export abstract class ProviderIdentityPort {
  abstract authorize(providerId: string, context: unknown): Promise<void>;
}

/**
 * Local challenge only: validates the provider identifier but DOES NOT authenticate
 * or authorize callers. Replace the ProviderIdentityPort binding with an external
 * IdP-backed implementation before exposing any financial endpoint in production.
 * That implementation must bind the authenticated principal to providerId; wallet
 * administration/read access also requires a separate deployment access policy.
 */
@Injectable()
export class LocalProviderIdentity extends ProviderIdentityPort {
  override async authorize(providerId: string, _context: unknown): Promise<void> {
    text(providerId, 'providerId');
  }
}

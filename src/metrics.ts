import { Injectable } from '@nestjs/common';

@Injectable()
export class Metrics {
  private readonly values = new Map<string, number>();

  constructor() {
    for (const status of ['PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED']) {
      this.increment('wagering_transactions_total', { status }, 0);
    }
    for (const name of ['wagering_duplicates_total', 'wagering_dlq_total', 'wagering_lock_conflicts_total', 'wagering_outbox_lag_seconds', 'wagering_reconciliation_mismatches_total']) {
      this.set(name, 0);
    }
    this.observe('wagering_processing_seconds', 0, false);
  }

  increment(name: string, labels: Record<string, string> = {}, value = 1): void {
    const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));
    const key = entries.length ? `${name}{${entries.map(([key, label]) => `${key}=${JSON.stringify(label)}`).join(',')}}` : name;
    this.values.set(key, (this.values.get(key) ?? 0) + value);
  }

  set(name: string, value: number): void {
    this.values.set(name, value);
  }

  observe(name: string, seconds: number, record = true): void {
    this.increment(`${name}_sum`, {}, record ? seconds : 0);
    this.increment(`${name}_count`, {}, record ? 1 : 0);
  }

  render(): string {
    return [...this.values].sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key} ${value}`).join('\n') + '\n';
  }
}

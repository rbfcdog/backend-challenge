/** Stable business failure with a machine-readable code, independent of transport. */
export class DomainError extends Error {
  constructor(public readonly code: string, message = code) {
    super(message);
    this.name = new.target.name;
  }
}

/** A caller attempted a transition forbidden by the transaction lifecycle. */
export class InvalidTransactionStateError extends Error {
  constructor(state: string, transition: string) {
    super(`Cannot ${transition} a transaction in ${state}`);
    this.name = "InvalidTransactionStateError";
  }
}

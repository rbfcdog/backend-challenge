import { DomainError } from "./errors";

export interface MoneyProps {
  readonly amount: string;
  readonly currency: string;
}

const currencies = new Set<string>(Intl.supportedValuesOf("currency"));

function assertCurrency(currency: string): void {
  if (
    typeof currency !== "string" ||
    !/^[A-Z]{3}$/.test(currency) ||
    !currencies.has(currency)
  ) {
    throw new TypeError("Currency must be a supported uppercase ISO-4217 code");
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(left: string, right: string) {
    super("CURRENCY_MISMATCH", `Cannot operate on different currencies: ${left} and ${right}`);
  }
}

/** Exact, immutable two-decimal money; persisted and arithmetic values may be signed. */
export class Money {
  readonly #minorUnits: bigint;
  readonly currency: string;

  private constructor(minorUnits: bigint, currency: string) {
    this.#minorUnits = minorUnits;
    this.currency = currency;
    Object.freeze(this);
  }

  /** Parses non-negative contract input without rounding or number conversion. */
  static from(props: MoneyProps): Money {
    if (props === null || typeof props !== "object" || Array.isArray(props)) {
      throw new TypeError("Money must be an object with amount and currency");
    }
    // Anchor the true end of input; "$" alone also permits a final newline.
    if (typeof props.amount !== "string" || !/^[0-9]+\.[0-9]{2}(?![\s\S])/.test(props.amount)) {
      throw new TypeError("Amount must be a non-negative decimal string with exactly two decimal places");
    }
    assertCurrency(props.currency);
    return new Money(BigInt(props.amount.replace(".", "")), props.currency);
  }

  /** Reconstructs exact persisted values, including signed reconciliation results. */
  static rehydrate(props: MoneyProps): Money {
    if (props === null || typeof props !== "object" || Array.isArray(props)) {
      throw new TypeError("Money must be an object with amount and currency");
    }
    if (typeof props.amount !== "string" || !/^-?[0-9]+\.[0-9]{2}(?![\s\S])/.test(props.amount)) {
      throw new TypeError("Persisted amount must be a decimal string with exactly two decimal places");
    }
    assertCurrency(props.currency);
    return new Money(BigInt(props.amount.replace(".", "")), props.currency);
  }

  static zero(currency: string): Money {
    assertCurrency(currency);
    return new Money(0n, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.#minorUnits + other.#minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.#minorUnits - other.#minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.#minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.#minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.#minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.#minorUnits < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.#minorUnits < other.#minorUnits;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.#minorUnits === other.#minorUnits;
  }

  /** Produces a stable JSON-safe DTO, including signed arithmetic results. */
  toJSON(): MoneyProps {
    return { amount: this.formatAmount(), currency: this.currency };
  }

  toString(): string {
    return `${this.formatAmount()} ${this.currency}`;
  }

  private formatAmount(): string {
    const absolute = this.#minorUnits < 0n ? -this.#minorUnits : this.#minorUnits;
    const sign = this.#minorUnits < 0n ? "-" : "";
    return `${sign}${absolute / 100n}.${(absolute % 100n).toString().padStart(2, "0")}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}

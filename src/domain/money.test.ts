import { expect, test } from "bun:test";
import { CurrencyMismatchError, Money, type MoneyProps } from "./money";

test("adds and subtracts exact amounts beyond Number.MAX_SAFE_INTEGER", () => {
  const large = Money.from({ amount: "9007199254740993.99", currency: "BRL" });
  const cent = Money.from({ amount: "0.01", currency: "BRL" });
  const sum = large.add(cent);

  expect(sum.toJSON()).toEqual({ amount: "9007199254740994.00", currency: "BRL" });
  expect(sum.subtract(cent).equals(large)).toBe(true);
  expect(large.toJSON().amount).toBe("9007199254740993.99");
  expect(cent.toJSON().amount).toBe("0.01");
  expect(large.add(Money.zero("BRL"))).not.toBe(large);
});

test("rejects invalid amounts and runtime contract types without coercion", () => {
  const invalidAmounts: readonly unknown[] = [
    "", "1", "1.0", "1.000", "1.001", ".01", "1.", "-1.00", "-0.00", "+1.00",
    "1e2", "1e2.00", " 1.00", "1.00 ", "1.00\n", "NaN", "Infinity", "1,00",
    1, 1n, Number.NaN, Number.POSITIVE_INFINITY, null, undefined, true,
    {}, [], new String("1.00"), { toString: () => "1.00" },
  ];
  for (const amount of invalidAmounts) {
    expect(() => Money.from({ amount, currency: "BRL" } as MoneyProps)).toThrow(TypeError);
  }
  for (const props of [null, undefined, false, 1, "1.00", [], {}, { amount: "1.00" }]) {
    expect(() => Money.from(props as unknown as MoneyProps)).toThrow(TypeError);
  }
});

test("validates currency codes in both factories", () => {
  for (const currency of ["", "brl", "BR", "BRLL", " BRL", "BRL ", "ZZZ", "123", null, undefined, 1, {}]) {
    expect(() => Money.from({ amount: "1.00", currency } as MoneyProps)).toThrow(TypeError);
    expect(() => Money.zero(currency as string)).toThrow(TypeError);
  }
  expect(Money.zero("USD").toJSON()).toEqual({ amount: "0.00", currency: "USD" });
  expect(Money.from({ amount: "1.00", currency: "EUR" }).currency).toBe("EUR");
});

test("rejects all cross-currency arithmetic and comparisons with a domain error", () => {
  const brl = Money.from({ amount: "1.00", currency: "BRL" });
  const usd = Money.from({ amount: "1.00", currency: "USD" });
  const operations = [
    () => brl.add(usd),
    () => brl.subtract(usd),
    () => brl.isLessThan(usd),
    () => brl.equals(usd),
  ];
  for (const operation of operations) {
    expect(operation).toThrow(CurrencyMismatchError);
  }
});

test("supports signed ledger arithmetic while preserving zero and comparison semantics", () => {
  const zero = Money.zero("BRL");
  const positive = Money.from({ amount: "0.01", currency: "BRL" });
  const negative = zero.subtract(positive);

  expect(positive.isPositive()).toBe(true);
  expect(positive.isNegative()).toBe(false);
  expect(negative.isNegative()).toBe(true);
  expect(negative.isPositive()).toBe(false);
  expect(negative.isZero()).toBe(false);
  expect(negative.toJSON().amount).toBe("-0.01");
  expect(negative.isLessThan(zero)).toBe(true);
  expect(zero.isLessThan(positive)).toBe(true);
  expect(positive.isLessThan(positive)).toBe(false);
  expect(negative.negate().equals(positive)).toBe(true);
  expect(positive.negate().equals(negative)).toBe(true);
  expect(negative.add(positive).equals(zero)).toBe(true);
  expect(zero.isZero()).toBe(true);
  expect(zero.isNegative()).toBe(false);
  expect(zero.isPositive()).toBe(false);
  expect(zero.negate().toJSON().amount).toBe("0.00");
  expect(positive.equals(zero)).toBe(false);
});

test("freezes every instance and serializes stable two-decimal JSON without bigint leakage", () => {
  const money = Money.from({ amount: "00025.00", currency: "BRL" });
  const zero = Money.zero("BRL");
  const instances = [money, zero, money.add(zero), zero.subtract(money), money.negate()];
  for (const instance of instances) {
    expect(Object.isFrozen(instance)).toBe(true);
    expect(Reflect.set(instance, "currency", "USD")).toBe(false);
  }
  expect(JSON.stringify(money)).toBe('{"amount":"25.00","currency":"BRL"}');
  expect(money.toString()).toBe("25.00 BRL");
  expect(money.negate().toString()).toBe("-25.00 BRL");
  expect(JSON.stringify(money.negate())).toBe('{"amount":"-25.00","currency":"BRL"}');
  expect(Money.from(money.toJSON()).equals(money)).toBe(true);
  const serialized = money.toJSON();
  expect(Reflect.set(serialized, "amount", "99.00")).toBe(true);
  expect(money.toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
});

test("rehydrates signed persisted values exactly without weakening non-negative input contracts", () => {
  for (const amount of ["-9007199254740993.99", "-0.01", "0.00", "9007199254740993.99"]) {
    const restored = Money.rehydrate({ amount, currency: "BRL" });
    expect(restored.toJSON()).toEqual({ amount, currency: "BRL" });
    expect(Money.rehydrate(restored.toJSON()).equals(restored)).toBe(true);
    expect(Object.isFrozen(restored)).toBe(true);
  }
  expect(Money.rehydrate({ amount: "-0.00", currency: "BRL" }).toJSON().amount).toBe("0.00");
  expect(() => Money.from({ amount: "-0.01", currency: "BRL" })).toThrow(TypeError);
  expect(Money.rehydrate({ amount: "-9007199254740993.99", currency: "BRL" })
    .add(Money.from({ amount: "9007199254740994.00", currency: "BRL" })).toJSON().amount).toBe("0.01");
});

test("rehydration still rejects inexact representations, coercion and unsupported currencies", () => {
  for (const amount of ["-1", "-1.0", "-1.001", "-1e2", "--1.00", "+1.00", "1.00\n", 1, null]) {
    expect(() => Money.rehydrate({ amount, currency: "BRL" } as MoneyProps)).toThrow(TypeError);
  }
  for (const value of [null, undefined, [], {}, "1.00"]) {
    expect(() => Money.rehydrate(value as unknown as MoneyProps)).toThrow(TypeError);
  }
  for (const currency of ["ZZZ", "brl", "", "BRL\n"]) {
    expect(() => Money.rehydrate({ amount: "-1.00", currency })).toThrow(TypeError);
  }
});

test("currency mismatch exposes a stable business failure code", () => {
  const mismatch = new CurrencyMismatchError("BRL", "USD");
  expect(mismatch.code).toBe("CURRENCY_MISMATCH");
  expect(mismatch.name).toBe("CurrencyMismatchError");
});

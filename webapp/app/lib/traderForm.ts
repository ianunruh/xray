import {
  type MMConfig,
  type NoiseConfig,
  type Trader,
  type TraderConfig,
  TraderType,
} from "../../src/gen/trader/v1/service_pb";
import { moneyToPrice, priceToNumber } from "./format";

export type MMFormFields = {
  symbol: string;
  accountId: string;
  initialDeposit: number | "";
  initialShares: number | "";
  spread: number | "";
  quantity: number | "";
  levels: number | "";
  levelSpacing: number | "";
  maxPosition: number | "";
  requoteIntervalMs: number | "";
  priceMoveThreshold: number | "";
  maxSkew: number | "";
};

export type NoiseFormFields = {
  symbol: string;
  accountId: string;
  initialDeposit: number | "";
  initialShares: number | "";
  randomInitialShares: boolean;
  orderIntervalMs: number | "";
  minQuantity: number | "";
  maxQuantity: number | "";
  priceJitter: number | "";
  marketOrderPct: number | "";
  maxPosition: number | "";
  buyBias: number | "";
  orderTimeoutMs: number | "";
};

export type FormState = {
  id?: string;
  name: string;
  type: TraderType;
  mm: MMFormFields;
  noise: NoiseFormFields;
};

export function emptyForm(): FormState {
  return {
    name: "",
    type: TraderType.MM,
    mm: {
      symbol: "AAPL",
      accountId: "",
      initialDeposit: 10_000_000,
      initialShares: 10_000,
      spread: 3.0,
      quantity: 20,
      levels: 3,
      levelSpacing: 1.0,
      maxPosition: 30_000,
      requoteIntervalMs: 30_000,
      priceMoveThreshold: 2.0,
      maxSkew: 1.0,
    },
    noise: {
      symbol: "AAPL",
      accountId: "",
      initialDeposit: 500_000,
      initialShares: 500,
      randomInitialShares: true,
      orderIntervalMs: 3_000,
      minQuantity: 1,
      maxQuantity: 10,
      priceJitter: 20.0,
      marketOrderPct: 0.5,
      maxPosition: 1000,
      buyBias: 0.5,
      orderTimeoutMs: 300_000,
    },
  };
}

// withName updates the form's name. The active config's accountId follows the
// name by default, staying in sync until the user edits the accountId directly
// (i.e. while it is still blank or equal to the previous name).
export function withName(form: FormState, name: string): FormState {
  const follow = (accountId: string) =>
    accountId === "" || accountId === form.name ? name : accountId;
  if (form.type === TraderType.MM) {
    return { ...form, name, mm: { ...form.mm, accountId: follow(form.mm.accountId) } };
  }
  return { ...form, name, noise: { ...form.noise, accountId: follow(form.noise.accountId) } };
}

export function mmFromProto(c: MMConfig): MMFormFields {
  return {
    symbol: c.symbol,
    accountId: c.accountId,
    initialDeposit: priceToNumber(c.initialDeposit),
    initialShares: Number(c.initialShares),
    spread: priceToNumber(c.spread),
    quantity: Number(c.quantity),
    levels: c.levels,
    levelSpacing: priceToNumber(c.levelSpacing),
    maxPosition: Number(c.maxPosition),
    requoteIntervalMs: Number(c.requoteIntervalMs),
    priceMoveThreshold: priceToNumber(c.priceMoveThreshold),
    maxSkew: priceToNumber(c.maxSkew),
  };
}

export function noiseFromProto(c: NoiseConfig): NoiseFormFields {
  return {
    symbol: c.symbol,
    accountId: c.accountId,
    initialDeposit: priceToNumber(c.initialDeposit),
    initialShares: Number(c.initialShares),
    randomInitialShares: c.randomInitialShares,
    orderIntervalMs: Number(c.orderIntervalMs),
    minQuantity: Number(c.minQuantity),
    maxQuantity: Number(c.maxQuantity),
    priceJitter: priceToNumber(c.priceJitter),
    marketOrderPct: c.marketOrderPct,
    maxPosition: Number(c.maxPosition),
    buyBias: c.buyBias,
    orderTimeoutMs: Number(c.orderTimeoutMs),
  };
}

export function formFromTrader(t: Trader): FormState {
  const f = emptyForm();
  f.id = t.id;
  f.name = t.name;
  f.type = t.type;
  if (t.config?.config.case === "mm") {
    f.mm = mmFromProto(t.config.config.value);
  } else if (t.config?.config.case === "noise") {
    f.noise = noiseFromProto(t.config.config.value);
  }
  return f;
}

// num pulls a NumberInput value into a number, treating "" as 0. The form
// types let users blank out a field; the engine config validators enforce
// "is this positive?" server-side.
function num(v: number | ""): number {
  return v === "" ? 0 : Number(v);
}

export function buildConfig(f: FormState): TraderConfig {
  if (f.type === TraderType.MM) {
    const mm: MMConfig = {
      $typeName: "trader.v1.MMConfig",
      symbol: f.mm.symbol.trim(),
      accountId: f.mm.accountId.trim(),
      initialDeposit: moneyToPrice(num(f.mm.initialDeposit)),
      initialShares: BigInt(num(f.mm.initialShares)),
      spread: moneyToPrice(num(f.mm.spread)),
      quantity: BigInt(num(f.mm.quantity)),
      levels: num(f.mm.levels),
      levelSpacing: moneyToPrice(num(f.mm.levelSpacing)),
      maxPosition: BigInt(num(f.mm.maxPosition)),
      requoteIntervalMs: BigInt(num(f.mm.requoteIntervalMs)),
      priceMoveThreshold: moneyToPrice(num(f.mm.priceMoveThreshold)),
      maxSkew: moneyToPrice(num(f.mm.maxSkew)),
    };
    return {
      $typeName: "trader.v1.TraderConfig",
      config: { case: "mm", value: mm },
    };
  }
  const noise: NoiseConfig = {
    $typeName: "trader.v1.NoiseConfig",
    symbol: f.noise.symbol.trim(),
    accountId: f.noise.accountId.trim(),
    initialDeposit: moneyToPrice(num(f.noise.initialDeposit)),
    initialShares: BigInt(num(f.noise.initialShares)),
    randomInitialShares: f.noise.randomInitialShares,
    orderIntervalMs: BigInt(num(f.noise.orderIntervalMs)),
    minQuantity: BigInt(num(f.noise.minQuantity)),
    maxQuantity: BigInt(num(f.noise.maxQuantity)),
    priceJitter: moneyToPrice(num(f.noise.priceJitter)),
    marketOrderPct: num(f.noise.marketOrderPct),
    maxPosition: BigInt(num(f.noise.maxPosition)),
    buyBias: num(f.noise.buyBias),
    orderTimeoutMs: BigInt(num(f.noise.orderTimeoutMs)),
  };
  return {
    $typeName: "trader.v1.TraderConfig",
    config: { case: "noise", value: noise },
  };
}

export function typeLabel(t: TraderType): string {
  switch (t) {
    case TraderType.MM:
      return "mm";
    case TraderType.NOISE:
      return "noise";
    default:
      return "unknown";
  }
}

export function symbolOf(t: Trader): string {
  if (t.config?.config.case === "mm") return t.config.config.value.symbol;
  if (t.config?.config.case === "noise") return t.config.config.value.symbol;
  return "";
}

export function accountOf(t: Trader): string {
  if (t.config?.config.case === "mm") return t.config.config.value.accountId;
  if (t.config?.config.case === "noise") return t.config.config.value.accountId;
  return "";
}

// uniqueIncrement returns the next "<base>-<n>" string that isn't in `taken`.
// A trailing "-<digits>" on base is treated as the starting suffix; otherwise
// counting begins at 2. Empty base returns empty (the duplicate flow leaves
// blank fields blank so the user notices they're required).
export function uniqueIncrement(base: string, taken: Iterable<string>): string {
  const trimmed = base.trim();
  if (!trimmed) return "";
  const set = new Set(taken);
  const m = trimmed.match(/^(.*?)-(\d+)$/);
  const root = m ? m[1] : trimmed;
  let n = m ? Number(m[2]) + 1 : 2;
  while (set.has(`${root}-${n}`)) n++;
  return `${root}-${n}`;
}

// duplicateForm returns a deep-copied form pre-filled from `source` with the
// name and the active config's accountId bumped to avoid collisions with
// already-used names/accountIds across all traders.
export function duplicateForm(
  source: FormState,
  takenNames: Iterable<string>,
  takenAccountIds: Iterable<string>,
): FormState {
  const copy: FormState = {
    name: uniqueIncrement(source.name, takenNames),
    type: source.type,
    mm: { ...source.mm },
    noise: { ...source.noise },
  };
  if (copy.type === TraderType.MM) {
    copy.mm.accountId = uniqueIncrement(source.mm.accountId, takenAccountIds);
  } else {
    copy.noise.accountId = uniqueIncrement(
      source.noise.accountId,
      takenAccountIds,
    );
  }
  return copy;
}

export function depositOf(t: Trader): bigint {
  if (t.config?.config.case === "mm") return t.config.config.value.initialDeposit;
  if (t.config?.config.case === "noise")
    return t.config.config.value.initialDeposit;
  return 0n;
}

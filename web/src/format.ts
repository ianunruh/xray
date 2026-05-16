const PRICE_SCALE = 10000n;

function addCommas(s: string): string {
  return s.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function formatPrice(price: bigint): string {
  const negative = price < 0n;
  const abs = negative ? -price : price;
  const whole = abs / PRICE_SCALE;
  const frac = abs % PRICE_SCALE;
  const fracStr = frac.toString().padStart(4, "0").replace(/0{1,2}$/, "");
  return `${negative ? "-" : ""}${addCommas(whole.toString())}.${fracStr}`;
}

export function formatMoney(price: bigint): string {
  return `$${formatPrice(price)}`;
}

export function formatQuantity(qty: bigint): string {
  return addCommas(qty.toString());
}

export function priceToNumber(price: bigint): number {
  return Number(price) / 10000;
}

export function moneyToPrice(value: number): bigint {
  return BigInt(Math.round(value * 10000));
}

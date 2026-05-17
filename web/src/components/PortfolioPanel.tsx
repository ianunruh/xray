import { Fragment, useEffect, useState } from "react";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Card,
  Collapse,
  Group,
  Menu,
  Modal,
  NumberInput,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import type { OrderPrefill } from "./OrderForm";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { formatMoney, formatPrice, formatQuantity, moneyToPrice, priceToNumber } from "../format";
import { PositionSide, Side } from "../gen/orderbook/v1/events_pb";
import { OrderStatus } from "../gen/portfolio/v1/service_pb";
import { usePortfolio } from "../hooks/usePortfolio";
import { useMarginCalls } from "../hooks/useMarginCalls";
import { useMarginSnapshot } from "../hooks/useMarginSnapshot";
import { portfolioClient, sagaClient } from "../client";

function timestampToMillis(ts: Timestamp | undefined): number | null {
  if (!ts) return null;
  return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
}

// formatRemaining renders a coarse "in 27s" / "in 3m12s" / "expired"
// string for a future or past instant.
function formatRemaining(deadlineMs: number, nowMs: number): string {
  const diffSec = Math.round((deadlineMs - nowMs) / 1000);
  if (diffSec <= 0) return "expired";
  if (diffSec < 60) return `in ${diffSec}s`;
  const mins = Math.floor(diffSec / 60);
  const secs = diffSec % 60;
  return `in ${mins}m${secs.toString().padStart(2, "0")}s`;
}

// GraceCountdown self-ticks every second to refresh the rendered
// remaining time. Renders nothing when deadline is null.
function GraceCountdown({ deadline }: { deadline: Timestamp | undefined }) {
  const deadlineMs = timestampToMillis(deadline);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (deadlineMs === null) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [deadlineMs]);
  if (deadlineMs === null) return null;
  return <>{formatRemaining(deadlineMs, now)}</>;
}

function sideName(side: Side): string {
  switch (side) {
    case Side.BUY:
      return "Buy";
    case Side.SELL:
      return "Sell";
    default:
      return "?";
  }
}

function orderStatusName(status: OrderStatus): string {
  switch (status) {
    case OrderStatus.STARTED:
      return "Started";
    case OrderStatus.CASH_HELD:
      return "Cash Held";
    case OrderStatus.ORDER_PLACED:
      return "Order Placed";
    case OrderStatus.COMPLETED:
      return "Completed";
    case OrderStatus.FAILED:
      return "Failed";
    default:
      return "?";
  }
}

function orderStatusColor(status: OrderStatus): string | undefined {
  switch (status) {
    case OrderStatus.COMPLETED:
      return "green";
    case OrderStatus.FAILED:
      return "red";
    default:
      return undefined;
  }
}

function DepositModal({
  accountId,
  opened,
  onClose,
}: {
  accountId: string;
  opened: boolean;
  onClose: () => void;
}) {
  const [amount, setAmount] = useState<number | string>("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit() {
    const val = Number(amount);
    if (!val || val <= 0) return;
    setLoading(true);
    try {
      await portfolioClient.deposit({
        accountId,
        amount: moneyToPrice(val),
      });
      notifications.show({
        title: "Deposit successful",
        message: `Deposited $${val} into ${accountId}`,
        color: "green",
      });
      setAmount("");
      onClose();
    } catch (e: unknown) {
      notifications.show({
        title: "Deposit failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Deposit">
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Account: {accountId}
        </Text>
        <NumberInput
          label="Amount"
          placeholder="0.00"
          min={0}
          decimalScale={4}
          value={amount}
          onChange={setAmount}
          autoFocus
        />
        <Button onClick={handleSubmit} loading={loading}>
          Deposit
        </Button>
      </Stack>
    </Modal>
  );
}

function WithdrawModal({
  accountId,
  opened,
  onClose,
}: {
  accountId: string;
  opened: boolean;
  onClose: () => void;
}) {
  const [amount, setAmount] = useState<number | string>("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit() {
    const val = Number(amount);
    if (!val || val <= 0) return;
    setLoading(true);
    try {
      await portfolioClient.withdraw({
        accountId,
        amount: moneyToPrice(val),
      });
      notifications.show({
        title: "Withdrawal successful",
        message: `Withdrew $${val} from ${accountId}`,
        color: "green",
      });
      setAmount("");
      onClose();
    } catch (e: unknown) {
      notifications.show({
        title: "Withdrawal failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Withdraw">
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Account: {accountId}
        </Text>
        <NumberInput
          label="Amount"
          placeholder="0.00"
          min={0}
          decimalScale={4}
          value={amount}
          onChange={setAmount}
          autoFocus
        />
        <Button onClick={handleSubmit} loading={loading} color="red">
          Withdraw
        </Button>
      </Stack>
    </Modal>
  );
}

function CreditSharesModal({
  accountId,
  opened,
  onClose,
  symbols,
}: {
  accountId: string;
  opened: boolean;
  onClose: () => void;
  symbols: string[];
}) {
  const [symbol, setSymbol] = useState("");
  const [quantity, setQuantity] = useState<number | string>("");
  const [costPerShare, setCostPerShare] = useState<number | string>("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit() {
    if (!symbol) return;
    const qty = Number(quantity);
    const cost = Number(costPerShare);
    if (!qty || qty <= 0 || !cost || cost <= 0) return;
    setLoading(true);
    try {
      await portfolioClient.creditShares({
        accountId,
        symbol,
        quantity: BigInt(qty),
        costPerShare: moneyToPrice(cost),
      });
      notifications.show({
        title: "Shares credited",
        message: `Credited ${qty} ${symbol} to ${accountId}`,
        color: "green",
      });
      setSymbol("");
      setQuantity("");
      setCostPerShare("");
      onClose();
    } catch (e: unknown) {
      notifications.show({
        title: "Credit shares failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Credit Shares">
      <Stack gap="sm">
        <Text size="sm" c="dimmed">
          Account: {accountId}
        </Text>
        <TextInput
          label="Symbol"
          placeholder="AAPL"
          value={symbol}
          onChange={(e) => setSymbol(e.currentTarget.value.toUpperCase())}
          list="credit-symbols"
          autoFocus
        />
        <datalist id="credit-symbols">
          {symbols.map((s) => (
            <option key={s} value={s} />
          ))}
        </datalist>
        <NumberInput
          label="Quantity"
          placeholder="0"
          min={1}
          allowDecimal={false}
          value={quantity}
          onChange={setQuantity}
        />
        <NumberInput
          label="Cost per Share"
          placeholder="0.00"
          min={0}
          decimalScale={4}
          value={costPerShare}
          onChange={setCostPerShare}
        />
        <Button onClick={handleSubmit} loading={loading}>
          Credit Shares
        </Button>
      </Stack>
    </Modal>
  );
}

export function PortfolioPanel({
  accountId,
  symbols,
  onJumpToAggregate,
  onPrefillOrder,
}: {
  accountId: string;
  symbols?: string[];
  onJumpToAggregate?: (aggregateId: string) => void;
  onPrefillOrder?: (p: OrderPrefill) => void;
}) {
  const portfolio = usePortfolio(accountId);
  const margin = useMarginSnapshot(accountId);
  const marginCalls = useMarginCalls(accountId);
  const [depositOpened, depositHandlers] = useDisclosure(false);
  const [withdrawOpened, withdrawHandlers] = useDisclosure(false);
  const [creditOpened, creditHandlers] = useDisclosure(false);
  const [cancellingId, setCancellingId] = useState<string | null>(null);
  const [expandedCalls, setExpandedCalls] = useState<Set<string>>(new Set());
  const toggleCallExpanded = (callId: string) => {
    setExpandedCalls((prev) => {
      const next = new Set(prev);
      next.has(callId) ? next.delete(callId) : next.add(callId);
      return next;
    });
  };

  async function handleCancel(sagaId: string, symbol: string) {
    setCancellingId(sagaId);
    try {
      await sagaClient.cancel({ sagaId });
      notifications.show({
        title: "Order cancelled",
        message: `Cancelled order for ${symbol}`,
        color: "green",
      });
    } catch (e: unknown) {
      notifications.show({
        title: "Cancel failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setCancellingId(null);
    }
  }

  if (!portfolio) {
    return (
      <Card withBorder>
        <Text c="dimmed">Loading portfolio...</Text>
      </Card>
    );
  }

  // Use the server's aggregate field — summing holdings.realizedPnl
  // misses short-side P&L since closed shorts have no holdings row.
  const totalRealizedPnl = portfolio.totalRealizedPnl;

  const activeOrders = portfolio.pendingOrders.filter(
    (o) =>
      o.status !== OrderStatus.COMPLETED && o.status !== OrderStatus.FAILED,
  );
  const recentOrders = portfolio.pendingOrders.filter(
    (o) =>
      o.status === OrderStatus.COMPLETED || o.status === OrderStatus.FAILED,
  );

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between">
          <Title order={5}>Portfolio: {portfolio.accountId}</Title>
          <Group gap="xs">
            <Button size="xs" variant="light" onClick={depositHandlers.open}>
              Deposit
            </Button>
            <Button
              size="xs"
              variant="light"
              color="red"
              onClick={withdrawHandlers.open}
            >
              Withdraw
            </Button>
            <Button
              size="xs"
              variant="light"
              color="violet"
              onClick={creditHandlers.open}
            >
              Credit Shares
            </Button>
          </Group>
        </Group>

        {margin?.marginCall && (
          <Alert color="red" title="Margin call active">
            Equity {formatMoney(margin.equity)} below maintenance
            requirement {formatMoney(margin.maintenanceRequirement)}.{" "}
            {margin.marginCallGraceExpiresAt ? (
              <>
                Auto-liquidation{" "}
                <GraceCountdown deadline={margin.marginCallGraceExpiresAt} />.
              </>
            ) : (
              <>Auto-liquidation in progress.</>
            )}
          </Alert>
        )}

        <Group gap="xl">
          <div>
            <Text size="xs" c="dimmed">
              Cash Available
            </Text>
            <Text fw={700}>{formatMoney(portfolio.cashBalance)}</Text>
          </div>
          <div>
            <Text size="xs" c="dimmed">
              Cash Held
            </Text>
            <Text fw={700}>{formatMoney(portfolio.cashHeld)}</Text>
          </div>
          <div>
            <Text size="xs" c="dimmed">
              Realized P&L
            </Text>
            <Text fw={700} c={totalRealizedPnl >= 0n ? "green" : "red"}>
              {formatMoney(totalRealizedPnl)}
            </Text>
          </div>
          {margin && (
            <>
              <div>
                <Text size="xs" c="dimmed">
                  Buying Power
                </Text>
                <Text fw={700}>{formatMoney(margin.buyingPower)}</Text>
              </div>
              <div>
                <Text size="xs" c="dimmed">
                  Equity
                </Text>
                <Text fw={700}>{formatMoney(margin.equity)}</Text>
              </div>
              <div>
                <Text size="xs" c="dimmed">
                  Maint. Req.
                </Text>
                <Text fw={700}>
                  {formatMoney(margin.maintenanceRequirement)}
                </Text>
              </div>
              <div>
                <Text size="xs" c="dimmed">
                  Margin Excess
                </Text>
                <Text
                  fw={700}
                  c={margin.marginExcess >= 0n ? "green" : "red"}
                >
                  {formatMoney(margin.marginExcess)}
                </Text>
              </div>
              {margin.marginLoan > 0n && (
                <div>
                  <Text size="xs" c="dimmed">
                    Margin Loan
                  </Text>
                  <Text fw={700} c="orange">
                    {formatMoney(margin.marginLoan)}
                  </Text>
                </div>
              )}
            </>
          )}
        </Group>

        {margin && margin.missingMarks.length > 0 && (
          <Text size="xs" c="orange">
            Missing marks: {margin.missingMarks.join(", ")} — equity
            understated for these symbols.
          </Text>
        )}

        {margin && margin.positions.some(
          (p) => p.side === PositionSide.SHORT,
        ) && (
          <>
            <Title order={6}>Short Positions</Title>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Symbol</Table.Th>
                  <Table.Th ta="right">Qty Owed</Table.Th>
                  <Table.Th ta="right">Avg Open</Table.Th>
                  <Table.Th ta="right">Mark</Table.Th>
                  <Table.Th ta="right">Liability</Table.Th>
                  <Table.Th ta="right">Unrealized P&L</Table.Th>
                  <Table.Th />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {margin.positions
                  .filter((p) => p.side === PositionSide.SHORT)
                  .map((p) => (
                    <Table.Tr key={p.symbol}>
                      <Table.Td>
                        <Group gap={4}>
                          {p.symbol}
                          <Badge size="xs" color="red" variant="light">
                            SHORT
                          </Badge>
                        </Group>
                      </Table.Td>
                      <Table.Td ta="right">
                        {formatQuantity(p.quantity)}
                      </Table.Td>
                      <Table.Td ta="right">{formatMoney(p.avgPrice)}</Table.Td>
                      <Table.Td ta="right">
                        {p.markMissing ? (
                          <Text size="xs" c="dimmed">
                            —
                          </Text>
                        ) : (
                          formatMoney(p.markPrice)
                        )}
                      </Table.Td>
                      <Table.Td ta="right">
                        {p.markMissing ? "—" : formatMoney(p.marketValue)}
                      </Table.Td>
                      <Table.Td
                        ta="right"
                        c={p.unrealizedPnl >= 0n ? "green" : "red"}
                      >
                        {p.markMissing ? "—" : formatMoney(p.unrealizedPnl)}
                      </Table.Td>
                      <Table.Td>
                        <Group justify="flex-end">
                          {onPrefillOrder && p.quantity > 0n && (
                            <Menu shadow="md" position="bottom-end" withinPortal>
                              <Menu.Target>
                                <ActionIcon size="xs" variant="subtle" color="gray" title="Position actions">
                                  ⋯
                                </ActionIcon>
                              </Menu.Target>
                              <Menu.Dropdown>
                                <Menu.Item
                                  onClick={() =>
                                    onPrefillOrder({
                                      symbol: p.symbol,
                                      action: "COVER",
                                      quantity: Number(p.quantity),
                                      orderType: "MARKET",
                                      nonce: Date.now(),
                                    })
                                  }
                                >
                                  Close Position
                                </Menu.Item>
                                <Menu.Item
                                  // Short profits when price drops. 50% gain on
                                  // avg-open notional ⇒ cover at avg * 0.5.
                                  onClick={() =>
                                    onPrefillOrder({
                                      symbol: p.symbol,
                                      action: "COVER",
                                      quantity: Number(p.quantity),
                                      orderType: "LIMIT",
                                      price: priceToNumber(p.avgPrice) * 0.5,
                                      nonce: Date.now(),
                                    })
                                  }
                                >
                                  Close at 50% profit
                                </Menu.Item>
                              </Menu.Dropdown>
                            </Menu>
                          )}
                        </Group>
                      </Table.Td>
                    </Table.Tr>
                  ))}
              </Table.Tbody>
            </Table>
          </>
        )}

        {portfolio.holdings.length > 0 && (
          <>
            <Title order={6}>Holdings</Title>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Symbol</Table.Th>
                  <Table.Th ta="right">Qty</Table.Th>
                  <Table.Th ta="right">Avg Cost</Table.Th>
                  <Table.Th ta="right">Total Cost</Table.Th>
                  <Table.Th ta="right">Held</Table.Th>
                  <Table.Th ta="right">Realized P&L</Table.Th>
                  <Table.Th />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {portfolio.holdings.map((h) => (
                  <Table.Tr key={h.symbol}>
                    <Table.Td>{h.symbol}</Table.Td>
                    <Table.Td ta="right">{formatQuantity(h.quantity)}</Table.Td>
                    <Table.Td ta="right">{formatMoney(h.averageCost)}</Table.Td>
                    <Table.Td ta="right">{formatMoney(h.totalCost)}</Table.Td>
                    <Table.Td ta="right">
                      {formatQuantity(h.sharesHeld)}
                    </Table.Td>
                    <Table.Td
                      ta="right"
                      c={h.realizedPnl >= 0n ? "green" : "red"}
                    >
                      {formatMoney(h.realizedPnl)}
                    </Table.Td>
                    <Table.Td>
                      <Group justify="flex-end">
                        {onPrefillOrder && h.quantity > 0n && (
                          <Menu shadow="md" position="bottom-end" withinPortal>
                            <Menu.Target>
                              <ActionIcon size="xs" variant="subtle" color="gray" title="Position actions">
                                ⋯
                              </ActionIcon>
                            </Menu.Target>
                            <Menu.Dropdown>
                              <Menu.Item
                                onClick={() =>
                                  onPrefillOrder({
                                    symbol: h.symbol,
                                    action: "SELL",
                                    quantity: Number(h.quantity),
                                    orderType: "MARKET",
                                    nonce: Date.now(),
                                  })
                                }
                              >
                                Close Position
                              </Menu.Item>
                              <Menu.Item
                                // Long profits when price climbs. 50% gain on
                                // avg cost ⇒ sell at avgCost * 1.5.
                                onClick={() =>
                                  onPrefillOrder({
                                    symbol: h.symbol,
                                    action: "SELL",
                                    quantity: Number(h.quantity),
                                    orderType: "LIMIT",
                                    price: priceToNumber(h.averageCost) * 1.5,
                                    nonce: Date.now(),
                                  })
                                }
                              >
                                Close at 50% profit
                              </Menu.Item>
                            </Menu.Dropdown>
                          </Menu>
                        )}
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </>
        )}

        {activeOrders.length > 0 && (
          <>
            <Title order={6}>Pending Orders</Title>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Symbol</Table.Th>
                  <Table.Th>Side</Table.Th>
                  <Table.Th ta="right">Price</Table.Th>
                  <Table.Th ta="right">Qty</Table.Th>
                  <Table.Th ta="right">Filled</Table.Th>
                  <Table.Th>Status</Table.Th>
                  <Table.Th />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {activeOrders
                  .sort((a, b) => Number(a.price - b.price))
                  .map((o) => (
                    <Table.Tr key={o.sagaId}>
                      <Table.Td>{o.symbol}</Table.Td>
                      <Table.Td c={o.side === Side.BUY ? "green" : "red"}>
                        {sideName(o.side)}
                      </Table.Td>
                      <Table.Td ta="right">{formatPrice(o.price)}</Table.Td>
                      <Table.Td ta="right">
                        {formatQuantity(o.quantity)}
                      </Table.Td>
                      <Table.Td ta="right">
                        {formatQuantity(o.filledQuantity)}
                      </Table.Td>
                      <Table.Td>{orderStatusName(o.status)}</Table.Td>
                      <Table.Td>
                        <Group justify="flex-end">
                          <Menu shadow="md" position="bottom-end" withinPortal>
                            <Menu.Target>
                              <ActionIcon size="xs" variant="subtle" color="gray" title="Order actions">
                                ⋯
                              </ActionIcon>
                            </Menu.Target>
                            <Menu.Dropdown>
                              <Menu.Item
                                color="red"
                                disabled={cancellingId === o.sagaId}
                                onClick={() => handleCancel(o.sagaId, o.symbol)}
                              >
                                Cancel
                              </Menu.Item>
                              {onJumpToAggregate && (
                                <Menu.Item
                                  onClick={() => onJumpToAggregate(`order-saga:${o.sagaId}`)}
                                >
                                  View Event Log
                                </Menu.Item>
                              )}
                            </Menu.Dropdown>
                          </Menu>
                        </Group>
                      </Table.Td>
                    </Table.Tr>
                  ))}
              </Table.Tbody>
            </Table>
          </>
        )}

        {recentOrders.length > 0 && (
          <>
            <Title order={6}>Recent Orders</Title>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Symbol</Table.Th>
                  <Table.Th>Side</Table.Th>
                  <Table.Th ta="right">Price</Table.Th>
                  <Table.Th ta="right">Qty</Table.Th>
                  <Table.Th ta="right">Filled</Table.Th>
                  <Table.Th>Status</Table.Th>
                  <Table.Th>Reason</Table.Th>
                  <Table.Th />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {recentOrders.map((o) => (
                  <Table.Tr key={o.sagaId}>
                    <Table.Td>{o.symbol}</Table.Td>
                    <Table.Td c={o.side === Side.BUY ? "green" : "red"}>
                      {sideName(o.side)}
                    </Table.Td>
                    <Table.Td ta="right">{formatPrice(o.price)}</Table.Td>
                    <Table.Td ta="right">
                      {formatQuantity(o.quantity)}
                    </Table.Td>
                    <Table.Td ta="right">
                      {formatQuantity(o.filledQuantity)}
                    </Table.Td>
                    <Table.Td c={orderStatusColor(o.status)}>
                      {orderStatusName(o.status)}
                    </Table.Td>
                    <Table.Td>
                      <Text size="xs" c="dimmed">
                        {o.failReason}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      <Group justify="flex-end">
                        {onJumpToAggregate && (
                          <Menu shadow="md" position="bottom-end" withinPortal>
                            <Menu.Target>
                              <ActionIcon size="xs" variant="subtle" color="gray" title="Order actions">
                                ⋯
                              </ActionIcon>
                            </Menu.Target>
                            <Menu.Dropdown>
                              <Menu.Item
                                onClick={() => onJumpToAggregate(`order-saga:${o.sagaId}`)}
                              >
                                View Event Log
                              </Menu.Item>
                            </Menu.Dropdown>
                          </Menu>
                        )}
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </>
        )}

        {marginCalls.length > 0 && (
          <>
            <Title order={6}>Margin Calls</Title>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th w={28} />
                  <Table.Th>Issued</Table.Th>
                  <Table.Th>Grace expires</Table.Th>
                  <Table.Th>Trigger</Table.Th>
                  <Table.Th>Status</Table.Th>
                  <Table.Th />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {marginCalls.map((c) => {
                  const isActive = !c.coveredAt;
                  const issued = c.issuedAt
                    ? new Date(Number(c.issuedAt.seconds) * 1000).toLocaleString()
                    : "";
                  const graceMs = timestampToMillis(c.graceExpiresAt);
                  const graceLabel = graceMs
                    ? new Date(graceMs).toLocaleTimeString()
                    : "—";
                  const expanded = expandedCalls.has(c.callId);
                  return (
                    <Fragment key={c.callId}>
                      <Table.Tr>
                        <Table.Td>
                          <ActionIcon
                            size="xs"
                            variant="subtle"
                            color="gray"
                            onClick={() => toggleCallExpanded(c.callId)}
                            title={expanded ? "Hide details" : "Show details"}
                          >
                            {expanded ? "▾" : "▸"}
                          </ActionIcon>
                        </Table.Td>
                        <Table.Td>
                          <Text size="xs">{issued}</Text>
                        </Table.Td>
                        <Table.Td>
                          <Group gap={6} wrap="nowrap">
                            <Text size="xs">{graceLabel}</Text>
                            {isActive && c.graceExpiresAt && (
                              <Text size="xs" c="dimmed">
                                (
                                <GraceCountdown deadline={c.graceExpiresAt} />)
                              </Text>
                            )}
                          </Group>
                        </Table.Td>
                        <Table.Td>{c.triggerSymbol}</Table.Td>
                        <Table.Td>
                          {isActive ? (
                            <Badge size="xs" color="red" variant="filled">
                              ACTIVE
                            </Badge>
                          ) : (
                            <Badge size="xs" color="gray" variant="light">
                              COVERED
                            </Badge>
                          )}
                        </Table.Td>
                        <Table.Td>
                          <Group justify="flex-end">
                            {c.liquidationSagaIds.length === 0 ? (
                              <Text size="xs" c="dimmed">
                                —
                              </Text>
                            ) : onJumpToAggregate ? (
                              <Menu shadow="md" position="bottom-end" withinPortal>
                                <Menu.Target>
                                  <ActionIcon size="xs" variant="subtle" color="gray" title="Call actions">
                                    ⋯
                                  </ActionIcon>
                                </Menu.Target>
                                <Menu.Dropdown>
                                  {c.liquidationSagaIds.map((sid) => (
                                    <Menu.Item
                                      key={sid}
                                      onClick={() => onJumpToAggregate(`order-saga:${sid}`)}
                                    >
                                      Go to liquidation: {sid}
                                    </Menu.Item>
                                  ))}
                                </Menu.Dropdown>
                              </Menu>
                            ) : (
                              <Text size="xs" c="dimmed">
                                {c.liquidationSagaIds.length}
                              </Text>
                            )}
                          </Group>
                        </Table.Td>
                      </Table.Tr>
                      <Table.Tr>
                        <Table.Td colSpan={6} p={0} style={{ borderTop: "none" }}>
                          <Collapse in={expanded}>
                            <Group gap="xl" p="xs" pl={40}>
                              <div>
                                <Text size="xs" c="dimmed">
                                  Mark
                                </Text>
                                <Text size="sm">{formatMoney(c.markPrice)}</Text>
                              </div>
                              <div>
                                <Text size="xs" c="dimmed">
                                  Equity at issue
                                </Text>
                                <Text size="sm">{formatMoney(c.equityAtIssue)}</Text>
                              </div>
                              <div>
                                <Text size="xs" c="dimmed">
                                  Maint. req.
                                </Text>
                                <Text size="sm">
                                  {formatMoney(c.maintenanceRequirementAtIssue)}
                                </Text>
                              </div>
                            </Group>
                          </Collapse>
                        </Table.Td>
                      </Table.Tr>
                    </Fragment>
                  );
                })}
              </Table.Tbody>
            </Table>
          </>
        )}
      </Stack>

      <DepositModal
        accountId={accountId}
        opened={depositOpened}
        onClose={depositHandlers.close}
      />
      <WithdrawModal
        accountId={accountId}
        opened={withdrawOpened}
        onClose={withdrawHandlers.close}
      />
      <CreditSharesModal
        accountId={accountId}
        opened={creditOpened}
        onClose={creditHandlers.close}
        symbols={symbols ?? []}
      />
    </Card>
  );
}

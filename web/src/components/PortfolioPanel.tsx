import { useCallback, useEffect, useState } from "react";
import { Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import { portfolioClient } from "../client";
import { formatMoney, formatPrice, formatQuantity } from "../format";
import type { GetPortfolioResponse } from "../gen/portfolio/v1/service_pb";
import { Side } from "../gen/orderbook/v1/events_pb";
import { PendingOrderStatus } from "../gen/portfolio/v1/service_pb";

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

function pendingStatusName(status: PendingOrderStatus): string {
  switch (status) {
    case PendingOrderStatus.STARTED:
      return "Started";
    case PendingOrderStatus.CASH_HELD:
      return "Cash Held";
    case PendingOrderStatus.ORDER_PLACED:
      return "Order Placed";
    default:
      return "?";
  }
}

export function PortfolioPanel({ accountId }: { accountId: string }) {
  const [portfolio, setPortfolio] = useState<GetPortfolioResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchPortfolio = useCallback(async () => {
    try {
      const resp = await portfolioClient.getPortfolio({ accountId });
      setPortfolio(resp);
      setError(null);
    } catch (err) {
      setError((err as Error).message);
    }
  }, [accountId]);

  useEffect(() => {
    fetchPortfolio();
    const id = setInterval(fetchPortfolio, 5000);
    return () => clearInterval(id);
  }, [fetchPortfolio]);

  if (error) {
    return (
      <Card withBorder>
        <Text c="red">Portfolio error: {error}</Text>
      </Card>
    );
  }

  if (!portfolio) {
    return (
      <Card withBorder>
        <Text c="dimmed">Loading portfolio...</Text>
      </Card>
    );
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Title order={5}>Portfolio: {portfolio.accountId}</Title>

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
        </Group>

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
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </>
        )}

        {portfolio.pendingOrders.length > 0 && (
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
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {[...portfolio.pendingOrders].sort((a, b) => Number(a.price - b.price)).map((o) => (
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
                    <Table.Td>{pendingStatusName(o.status)}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </>
        )}
      </Stack>
    </Card>
  );
}

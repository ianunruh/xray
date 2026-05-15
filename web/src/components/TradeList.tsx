import { useCallback, useState } from "react";
import { Table, Text, Title } from "@mantine/core";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { orderBookClient } from "../client";
import { formatPrice, formatQuantity } from "../format";
import type { Trade } from "../gen/orderbook/v1/service_pb";
import { useStream } from "../hooks/useStream";

const MAX_TRADES = 100;

export function TradeList({ symbol }: { symbol: string }) {
  const [trades, setTrades] = useState<Trade[]>([]);

  const onTrade = useCallback((trade: Trade) => {
    setTrades((prev) => [trade, ...prev].slice(0, MAX_TRADES));
  }, []);

  useStream(
    (signal) => orderBookClient.streamTrades({ symbol }, { signal }),
    onTrade,
    [symbol],
  );

  return (
    <>
      <Title order={6}>Recent Trades</Title>
      <Table>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Time</Table.Th>
            <Table.Th ta="right">Price</Table.Th>
            <Table.Th ta="right">Qty</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {trades.map((t) => (
            <Table.Tr key={t.tradeId}>
              <Table.Td>
                <Text size="xs" ff="monospace">
                  {t.executedAt ? timestampDate(t.executedAt).toLocaleTimeString() : ""}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" ff="monospace">
                  {formatPrice(t.price)}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" ff="monospace">
                  {formatQuantity(t.quantity)}
                </Text>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </>
  );
}

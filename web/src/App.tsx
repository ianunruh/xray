import { useEffect, useState } from "react";
import {
  AppShell,
  Group,
  Select,
  SimpleGrid,
  Stack,
  Title,
} from "@mantine/core";
import { PortfolioPanel } from "./components/PortfolioPanel";
import { MarketPanel } from "./components/MarketPanel";
import { orderBookClient, portfolioClient } from "./client";

function getParam(key: string): string {
  return new URLSearchParams(window.location.search).get(key) ?? "";
}

function setParam(key: string, value: string) {
  const params = new URLSearchParams(window.location.search);
  if (value) {
    params.set(key, value);
  } else {
    params.delete(key);
  }
  const qs = params.toString();
  history.replaceState(null, "", qs ? `?${qs}` : window.location.pathname);
}

export function App() {
  const [account, setAccount] = useState(getParam("account"));
  const [symbol, setSymbol] = useState(getParam("symbol"));
  const [accounts, setAccounts] = useState<string[]>([]);
  const [symbols, setSymbols] = useState<string[]>([]);

  useEffect(() => {
    portfolioClient.listPortfolios({}).then((r) => setAccounts(r.accountIds));
    orderBookClient.listSymbols({}).then((r) => setSymbols(r.symbols));
  }, []);

  return (
    <AppShell header={{ height: 50 }} padding="md">
      <AppShell.Header p="xs">
        <Group gap="md" h="100%">
          <Title order={4}>xray</Title>
          <Select
            size="xs"
            placeholder="Account"
            data={accounts}
            value={account || null}
            onChange={(v) => {
              const val = v ?? "";
              setAccount(val);
              setParam("account", val);
            }}
            searchable
            clearable
          />
          <Select
            size="xs"
            placeholder="Symbol"
            data={symbols}
            value={symbol || null}
            onChange={(v) => {
              const val = v ?? "";
              setSymbol(val);
              setParam("symbol", val);
            }}
            searchable
            clearable
          />
        </Group>
      </AppShell.Header>

      <AppShell.Main>
        {account && symbol ? (
          <SimpleGrid cols={{ base: 1, md: 2 }} spacing="md">
            <PortfolioPanel accountId={account} />
            <MarketPanel symbol={symbol} />
          </SimpleGrid>
        ) : (
          <Stack gap="md">
            {account && <PortfolioPanel accountId={account} />}
            {symbol && <MarketPanel symbol={symbol} />}
          </Stack>
        )}
      </AppShell.Main>
    </AppShell>
  );
}

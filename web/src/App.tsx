import { useState } from "react";
import {
  AppShell,
  Container,
  Group,
  Stack,
  TextInput,
  Title,
} from "@mantine/core";
import { PortfolioPanel } from "./components/PortfolioPanel";
import { MarketPanel } from "./components/MarketPanel";

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

  return (
    <AppShell header={{ height: 50 }} padding="md">
      <AppShell.Header p="xs">
        <Group gap="md" h="100%">
          <Title order={4}>xray</Title>
          <TextInput
            size="xs"
            placeholder="Account ID"
            value={account}
            onChange={(e) => {
              setAccount(e.currentTarget.value);
              setParam("account", e.currentTarget.value);
            }}
          />
          <TextInput
            size="xs"
            placeholder="Symbol"
            value={symbol}
            onChange={(e) => {
              const v = e.currentTarget.value.toUpperCase();
              setSymbol(v);
              setParam("symbol", v);
            }}
          />
        </Group>
      </AppShell.Header>

      <AppShell.Main>
        <Container size="xl">
          <Stack gap="md">
            {account && <PortfolioPanel accountId={account} />}
            {symbol && <MarketPanel symbol={symbol} />}
          </Stack>
        </Container>
      </AppShell.Main>
    </AppShell>
  );
}

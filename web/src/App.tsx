import { useEffect, useState } from "react";
import {
  AppShell,
  Button,
  Group,
  Modal,
  NumberInput,
  SegmentedControl,
  Select,
  SimpleGrid,
  Stack,
  TextInput,
  Title,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { PortfolioPanel } from "./components/PortfolioPanel";
import { BracketsPanel } from "./components/BracketsPanel";
import { OcosPanel } from "./components/OcosPanel";
import { MarketPanel } from "./components/MarketPanel";
import { OrderForm, type OrderPrefill } from "./components/OrderForm";
import { DiagnosticsPanel } from "./components/DiagnosticsPanel";
import { ChainPanel } from "./components/ChainPanel";
import { orderBookClient, portfolioClient } from "./client";
import { moneyToPrice } from "./format";

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

type View = "trading" | "diagnostics" | "chain";

function getViewParam(): View {
  const v = getParam("view");
  if (v === "diagnostics") return "diagnostics";
  if (v === "chain") return "chain";
  return "trading";
}

export function App() {
  const [view, setView] = useState<View>(getViewParam());
  const [account, setAccount] = useState(getParam("account"));
  const [symbol, setSymbol] = useState(getParam("symbol"));
  const [aggregate, setAggregate] = useState(getParam("aggregate"));
  const [correlation, setCorrelation] = useState(getParam("correlation"));
  const [accounts, setAccounts] = useState<string[]>([]);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [newAccOpened, newAccHandlers] = useDisclosure(false);
  const [newAccId, setNewAccId] = useState("");
  const [newAccDeposit, setNewAccDeposit] = useState<number | string>("");
  const [newAccLoading, setNewAccLoading] = useState(false);
  const [orderPrefill, setOrderPrefill] = useState<OrderPrefill | null>(null);

  // Receive a prefill from a portfolio table menu. Switches the
  // active symbol (so MarketPanel + OrderForm focus on it) and pushes
  // the prefill down to OrderForm via prop.
  function applyOrderPrefill(p: OrderPrefill) {
    setSymbol(p.symbol);
    setParam("symbol", p.symbol);
    setOrderPrefill(p);
  }

  function refreshAccounts() {
    portfolioClient.listPortfolios({}).then((r) => setAccounts(r.accountIds));
  }

  function jumpToAggregate(aggregateId: string) {
    setAggregate(aggregateId);
    setParam("aggregate", aggregateId);
    setView("diagnostics");
    setParam("view", "diagnostics");
  }

  useEffect(() => {
    refreshAccounts();
    orderBookClient.listSymbols({}).then((r) => setSymbols(r.symbols));
  }, []);

  async function handleNewAccount() {
    if (!newAccId.trim()) return;
    const deposit = Number(newAccDeposit);
    if (!deposit || deposit <= 0) return;
    setNewAccLoading(true);
    try {
      await portfolioClient.deposit({
        accountId: newAccId.trim(),
        amount: moneyToPrice(deposit),
      });
      notifications.show({
        title: "Portfolio created",
        message: `Created ${newAccId} with $${deposit} deposit`,
        color: "green",
      });
      const val = newAccId.trim();
      setAccount(val);
      setParam("account", val);
      refreshAccounts();
      setNewAccId("");
      setNewAccDeposit("");
      newAccHandlers.close();
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to create portfolio",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setNewAccLoading(false);
    }
  }

  return (
    <AppShell header={{ height: 50 }} padding="md">
      <AppShell.Header p="xs">
        <Group gap="md" h="100%">
          <Title order={4}>xray</Title>
          <SegmentedControl
            size="xs"
            value={view}
            onChange={(v) => {
              const next = v as View;
              setView(next);
              setParam("view", next === "trading" ? "" : next);
            }}
            data={[
              { label: "Trading", value: "trading" },
              { label: "Diagnostics", value: "diagnostics" },
              { label: "Chain", value: "chain" },
            ]}
          />
          {view === "trading" && (
            <>
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
              <Button size="xs" variant="subtle" onClick={newAccHandlers.open}>
                + New
              </Button>
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
            </>
          )}
        </Group>
      </AppShell.Header>

      <AppShell.Main>
        {view === "chain" ? (
          <ChainPanel
            initialCorrelationId={correlation}
            onCorrelationChange={(id) => {
              setCorrelation(id);
              setParam("correlation", id);
            }}
          />
        ) : view === "diagnostics" ? (
          <DiagnosticsPanel
            initialAggregateId={aggregate}
            onAggregateChange={(id) => {
              setAggregate(id);
              setParam("aggregate", id);
            }}
            onJumpToChain={(id) => {
              setCorrelation(id);
              setParam("correlation", id);
              setView("chain");
              setParam("view", "chain");
            }}
          />
        ) : account && symbol ? (
          <SimpleGrid cols={{ base: 1, md: 2 }} spacing="md">
            <Stack gap="md">
              <PortfolioPanel accountId={account} symbols={symbols} onJumpToAggregate={jumpToAggregate} onPrefillOrder={applyOrderPrefill} />
              <BracketsPanel accountId={account} onJumpToAggregate={jumpToAggregate} />
              <OcosPanel accountId={account} onJumpToAggregate={jumpToAggregate} />
              <OrderForm accountId={account} symbol={symbol} prefill={orderPrefill} />
            </Stack>
            <MarketPanel symbol={symbol} />
          </SimpleGrid>
        ) : (
          <Stack gap="md">
            {account && <PortfolioPanel accountId={account} symbols={symbols} onJumpToAggregate={jumpToAggregate} onPrefillOrder={applyOrderPrefill} />}
            {account && <BracketsPanel accountId={account} onJumpToAggregate={jumpToAggregate} />}
            {account && <OcosPanel accountId={account} onJumpToAggregate={jumpToAggregate} />}
            {symbol && <MarketPanel symbol={symbol} />}
          </Stack>
        )}
      </AppShell.Main>

      <Modal
        opened={newAccOpened}
        onClose={newAccHandlers.close}
        title="New Portfolio"
      >
        <Stack gap="sm">
          <TextInput
            label="Account ID"
            placeholder="my-account"
            value={newAccId}
            onChange={(e) => setNewAccId(e.currentTarget.value)}
            autoFocus
          />
          <NumberInput
            label="Initial Deposit"
            placeholder="0.00"
            min={0}
            decimalScale={4}
            value={newAccDeposit}
            onChange={setNewAccDeposit}
          />
          <Button onClick={handleNewAccount} loading={newAccLoading}>
            Create Portfolio
          </Button>
        </Stack>
      </Modal>
    </AppShell>
  );
}

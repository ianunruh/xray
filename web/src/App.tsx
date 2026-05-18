import { useEffect, useState } from "react";
import {
  AppShell,
  Button,
  Grid,
  Group,
  Modal,
  NumberInput,
  SegmentedControl,
  Select,
  Stack,
  Tabs,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import {
  PortfolioOrders,
  PortfolioPositions,
  PortfolioSummary,
} from "./components/PortfolioPanel";
import { BracketsPanel } from "./components/BracketsPanel";
import { OcosPanel } from "./components/OcosPanel";
import { TwapsPanel } from "./components/TwapsPanel";
import { MarketPanel } from "./components/MarketPanel";
import { OrderForm, type OrderPrefill } from "./components/OrderForm";
import { DiagnosticsPanel } from "./components/DiagnosticsPanel";
import { ChainPanel } from "./components/ChainPanel";
import { orderBookClient, portfolioClient } from "./client";
import { moneyToPrice } from "./format";
import { AccountDataProvider } from "./hooks/accountData";
import { MarketDepthProvider } from "./hooks/marketDepth";
import { useOrderStatusNotifications } from "./hooks/useOrderStatusNotifications";

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
type Tab = "trade" | "orders" | "positions";

// OrderStatusNotifier mounts the order-status notification hook for
// the active account. Rendered as a sibling rather than called inline
// so the subscription survives across view switches. Must be inside
// an AccountDataProvider — pulls portfolio data from context.
function OrderStatusNotifier() {
  useOrderStatusNotifications();
  return null;
}

function getViewParam(): View {
  const v = getParam("view");
  if (v === "diagnostics") return "diagnostics";
  if (v === "chain") return "chain";
  return "trading";
}

function getTabParam(): Tab {
  const t = getParam("tab");
  if (t === "orders") return "orders";
  if (t === "positions") return "positions";
  return "trade";
}

export function App() {
  const [view, setView] = useState<View>(getViewParam());
  const [tab, setTab] = useState<Tab>(getTabParam());
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
  // active symbol and tab so the OrderForm is in view, then pushes
  // the prefill down to OrderForm via prop.
  function applyOrderPrefill(p: OrderPrefill) {
    setSymbol(p.symbol);
    setParam("symbol", p.symbol);
    setTab("trade");
    setParam("tab", "");
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

  function renderTradingBody() {
    if (!account) {
      return (
        <Text c="dimmed">Select or create an account to start trading.</Text>
      );
    }
    return (
      <Stack gap="md">
        <PortfolioSummary symbols={symbols} />
        <Tabs
          value={tab}
          onChange={(v) => {
            const next = (v as Tab) ?? "trade";
            setTab(next);
            setParam("tab", next === "trade" ? "" : next);
          }}
          keepMounted={false}
        >
          <Tabs.List>
            <Tabs.Tab value="trade">Trade</Tabs.Tab>
            <Tabs.Tab value="orders">Orders</Tabs.Tab>
            <Tabs.Tab value="positions">Positions</Tabs.Tab>
          </Tabs.List>

          <Tabs.Panel value="trade" pt="md">
            {symbol ? (
              <MarketDepthProvider symbol={symbol}>
                <Grid gutter="md">
                  <Grid.Col span={{ base: 12, md: 8 }}>
                    <MarketPanel symbol={symbol} />
                  </Grid.Col>
                  <Grid.Col span={{ base: 12, md: 4 }}>
                    <OrderForm symbol={symbol} prefill={orderPrefill} />
                  </Grid.Col>
                </Grid>
              </MarketDepthProvider>
            ) : (
              <Text c="dimmed">Select a symbol to view the market.</Text>
            )}
          </Tabs.Panel>

          <Tabs.Panel value="orders" pt="md">
            <Stack gap="md">
              <PortfolioOrders onJumpToAggregate={jumpToAggregate} />
              <BracketsPanel
                accountId={account}
                onJumpToAggregate={jumpToAggregate}
              />
              <OcosPanel
                accountId={account}
                onJumpToAggregate={jumpToAggregate}
              />
              <TwapsPanel
                accountId={account}
                onJumpToAggregate={jumpToAggregate}
              />
            </Stack>
          </Tabs.Panel>

          <Tabs.Panel value="positions" pt="md">
            <PortfolioPositions
              onJumpToAggregate={jumpToAggregate}
              onPrefillOrder={applyOrderPrefill}
            />
          </Tabs.Panel>
        </Tabs>
      </Stack>
    );
  }

  const body =
    view === "chain" ? (
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
    ) : (
      renderTradingBody()
    );

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
                checkIconPosition="right"
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
                checkIconPosition="right"
              />
            </>
          )}
        </Group>
      </AppShell.Header>

      <AppShell.Main>
        {account ? (
          <AccountDataProvider accountId={account}>
            <OrderStatusNotifier />
            {body}
          </AccountDataProvider>
        ) : (
          body
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

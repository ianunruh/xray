import { useEffect, useState } from "react";
import {
  Button,
  Grid,
  Group,
  Modal,
  NumberInput,
  Select,
  Stack,
  Tabs,
  Text,
  TextInput,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import {
  useFetcher,
  useNavigate,
  useRevalidator,
  useSearchParams,
} from "react-router";
import type { Route } from "./+types/trading";
import {
  orderBookClient,
  portfolioClient,
} from "~/lib/client.server";
import { moneyToPrice } from "~/lib/format";
import { AccountDataProvider } from "~/hooks/accountData";
import { MarketDepthProvider } from "~/hooks/marketDepth";
import { useOrderStatusNotifications } from "~/hooks/useOrderStatusNotifications";
import {
  PortfolioOrders,
  PortfolioPositions,
  PortfolioSummary,
} from "~/components/PortfolioPanel";
import { BracketsPanel } from "~/components/BracketsPanel";
import { OcosPanel } from "~/components/OcosPanel";
import { TwapsPanel } from "~/components/TwapsPanel";
import { RecentPnLPanel } from "~/components/RecentPnLPanel";
import { MarketPanel } from "~/components/MarketPanel";
import { OrderForm, type OrderPrefill } from "~/components/OrderForm";

type Tab = "trade" | "orders" | "positions";

function parseTab(v: string | null): Tab {
  if (v === "orders") return "orders";
  if (v === "positions") return "positions";
  return "trade";
}

export async function loader() {
  const [{ accountIds }, { symbols }] = await Promise.all([
    portfolioClient.listPortfolios({}),
    orderBookClient.listSymbols({}),
  ]);
  return { accountIds, symbols };
}

type ActionResult =
  | { ok: true; intent: string; accountId?: string }
  | { ok: false; intent: string; error: string };

export async function action({
  request,
}: Route.ActionArgs): Promise<ActionResult> {
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");
  try {
    switch (intent) {
      case "create-account": {
        const accountId = String(form.get("accountId") ?? "").trim();
        const deposit = Number(form.get("deposit") ?? "0");
        if (!accountId) throw new Error("account id required");
        if (!deposit || deposit <= 0) throw new Error("deposit must be > 0");
        await portfolioClient.deposit({
          accountId,
          amount: moneyToPrice(deposit),
        });
        return { ok: true, intent, accountId };
      }
      default:
        return { ok: false, intent, error: `unknown intent: ${intent}` };
    }
  } catch (e: unknown) {
    return {
      ok: false,
      intent,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

// OrderStatusNotifier mounts the order-status notification hook for the
// active account. Rendered as a sibling so the subscription survives tab
// switches. Must be inside an AccountDataProvider — pulls portfolio state
// from context.
function OrderStatusNotifier() {
  useOrderStatusNotifications();
  return null;
}

export default function Trading({ loaderData }: Route.ComponentProps) {
  const { accountIds, symbols } = loaderData;
  const navigate = useNavigate();
  const revalidator = useRevalidator();
  const createFetcher = useFetcher<typeof action>();
  const [params, setParams] = useSearchParams();

  const account = params.get("account") ?? "";
  const symbol = params.get("symbol") ?? "";
  const tab = parseTab(params.get("tab"));

  const [newAccOpened, newAccHandlers] = useDisclosure(false);
  const [newAccId, setNewAccId] = useState("");
  const [newAccDeposit, setNewAccDeposit] = useState<number | string>("");
  const [orderPrefill, setOrderPrefill] = useState<OrderPrefill | null>(null);

  function updateParam(key: string, value: string) {
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value) next.set(key, value);
        else next.delete(key);
        return next;
      },
      { replace: true },
    );
  }

  function applyOrderPrefill(p: OrderPrefill) {
    updateParam("symbol", p.symbol);
    updateParam("tab", "");
    setOrderPrefill(p);
  }

  function jumpToAggregate(aggregateId: string) {
    navigate(`/events?aggregate=${encodeURIComponent(aggregateId)}`);
  }

  // React to create-account action completion.
  useEffect(() => {
    if (createFetcher.state !== "idle" || !createFetcher.data) return;
    const data = createFetcher.data;
    if (data.ok && data.accountId) {
      notifications.show({
        title: "Portfolio created",
        message: `Created ${data.accountId}`,
        color: "green",
      });
      newAccHandlers.close();
      setNewAccId("");
      setNewAccDeposit("");
      updateParam("account", data.accountId);
      revalidator.revalidate();
    } else if (!data.ok) {
      notifications.show({
        title: "Failed to create portfolio",
        message: data.error,
        color: "red",
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [createFetcher.state, createFetcher.data]);

  function submitNewAccount() {
    const id = newAccId.trim();
    if (!id) return;
    const dep = Number(newAccDeposit);
    if (!dep || dep <= 0) return;
    const fd = new FormData();
    fd.set("intent", "create-account");
    fd.set("accountId", id);
    fd.set("deposit", String(dep));
    createFetcher.submit(fd, { method: "post" });
  }

  const newAccLoading = createFetcher.state !== "idle";

  return (
    <Stack gap="md">
      <Group gap="md">
        <Select
          size="xs"
          placeholder="Account"
          data={accountIds}
          value={account || null}
          onChange={(v) => updateParam("account", v ?? "")}
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
          onChange={(v) => updateParam("symbol", v ?? "")}
          searchable
          clearable
          checkIconPosition="right"
        />
      </Group>

      {account ? (
        <AccountDataProvider accountId={account}>
          <OrderStatusNotifier />
          <TradingBody
            account={account}
            symbol={symbol}
            symbols={symbols}
            tab={tab}
            onTabChange={(t) => updateParam("tab", t === "trade" ? "" : t)}
            orderPrefill={orderPrefill}
            onJumpToAggregate={jumpToAggregate}
            onPrefillOrder={applyOrderPrefill}
          />
        </AccountDataProvider>
      ) : (
        <Text c="dimmed">Select or create an account to start trading.</Text>
      )}

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
          <Button onClick={submitNewAccount} loading={newAccLoading}>
            Create Portfolio
          </Button>
        </Stack>
      </Modal>
    </Stack>
  );
}

function TradingBody({
  account,
  symbol,
  symbols,
  tab,
  onTabChange,
  orderPrefill,
  onJumpToAggregate,
  onPrefillOrder,
}: {
  account: string;
  symbol: string;
  symbols: string[];
  tab: Tab;
  onTabChange: (t: Tab) => void;
  orderPrefill: OrderPrefill | null;
  onJumpToAggregate: (id: string) => void;
  onPrefillOrder: (p: OrderPrefill) => void;
}) {
  return (
    <Stack gap="md">
      <PortfolioSummary symbols={symbols} />
      <Tabs
        value={tab}
        onChange={(v) => onTabChange((v as Tab) ?? "trade")}
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
            <PortfolioOrders onJumpToAggregate={onJumpToAggregate} />
            <BracketsPanel
              accountId={account}
              onJumpToAggregate={onJumpToAggregate}
            />
            <OcosPanel
              accountId={account}
              onJumpToAggregate={onJumpToAggregate}
            />
            <TwapsPanel
              accountId={account}
              onJumpToAggregate={onJumpToAggregate}
            />
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="positions" pt="md">
          <Stack gap="md">
            <PortfolioPositions
              onJumpToAggregate={onJumpToAggregate}
              onPrefillOrder={onPrefillOrder}
            />
            <RecentPnLPanel accountId={account} />
          </Stack>
        </Tabs.Panel>
      </Tabs>
    </Stack>
  );
}

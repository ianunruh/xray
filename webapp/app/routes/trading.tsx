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
import { fromJsonString } from "@bufbuild/protobuf";
import {
  ConnectError,
  Code,
} from "@connectrpc/connect";
import {
  orderBookClient,
  portfolioClient,
  sagaClient,
} from "~/lib/client.server";
import { moneyToPrice } from "~/lib/format";
import { PlaceSagaRequestSchema } from "../../src/gen/saga/v1/saga_pb";
import {
  MarketPhase,
  type OrderType,
  type PositionSide,
  type Side,
} from "../../src/gen/orderbook/v1/events_pb";
import type { GetOfficialCloseResponse } from "../../src/gen/orderbook/v1/service_pb";
import type {
  FeeRecord,
  GetMarginSnapshotResponse,
  MarginCallRecord,
  PreviewOrderImpactResponse,
} from "../../src/gen/portfolio/v1/service_pb";
import type { ReplayBounds } from "~/lib/replay";
import { emptyLULDState, luldFromProto, type LULDState } from "~/lib/luld";
import { AccountDataProvider } from "~/hooks/accountData";
import { MarketDepthProvider } from "~/hooks/marketDepth";
import { useOrderStatusNotifications } from "~/hooks/useOrderStatusNotifications";
import {
  PortfolioOrders,
  PortfolioPositions,
  PortfolioSummary,
} from "~/components/PortfolioPanel";
import {
  BracketsPanel,
  type BracketRow,
} from "~/components/BracketsPanel";
import { OcosPanel, type OcoRow } from "~/components/OcosPanel";
import { TwapsPanel, type TwapRow } from "~/components/TwapsPanel";
import {
  RecentPnLPanel,
  type RealizedPnlRow,
} from "~/components/RecentPnLPanel";
import { MarketPanel } from "~/components/MarketPanel";
import { OrderForm, type OrderPrefill } from "~/components/OrderForm";
import { PortfolioFees } from "~/components/PortfolioFees";
import { SagaKind, SagaStatus } from "../../src/gen/saga/v1/saga_pb";

type Tab = "trade" | "orders" | "positions" | "fees";

const REVALIDATE_INTERVAL_MS = 2500;
const PNL_HISTORY_LIMIT = 25;
const FEE_HISTORY_LIMIT = 200;

function parseTab(v: string | null): Tab {
  if (v === "orders") return "orders";
  if (v === "positions") return "positions";
  if (v === "fees") return "fees";
  return "trade";
}

// loadSymbolData fans out the per-symbol reads (phase, official close,
// replay bounds) that the trading view's MarketPanel needs. Each call is
// wrapped in try/catch so a transient failure in one doesn't fail the
// whole loader. Returns sentinel "empty" values that map cleanly onto
// the panel's null-checks.
async function loadSymbolData(symbol: string) {
  const [phaseR, closeR, boundsR] = await Promise.allSettled([
    orderBookClient.getMarketStatus({ symbol }),
    orderBookClient.getOfficialClose({ symbol, sessionDate: "" }),
    orderBookClient.getReplayBounds({ symbol }),
  ]);

  const phase =
    phaseR.status === "fulfilled" &&
    phaseR.value.phase !== MarketPhase.UNSPECIFIED
      ? phaseR.value.phase
      : MarketPhase.CONTINUOUS;
  const sessionVolume =
    phaseR.status === "fulfilled" ? phaseR.value.sessionVolume : 0n;
  const luld =
    phaseR.status === "fulfilled"
      ? luldFromProto(phaseR.value)
      : emptyLULDState();

  let officialClose: GetOfficialCloseResponse | null = null;
  if (closeR.status === "fulfilled") {
    officialClose = closeR.value;
  } else if (
    closeR.reason instanceof ConnectError &&
    (closeR.reason.code === Code.NotFound ||
      closeR.reason.code === Code.Unimplemented)
  ) {
    officialClose = null;
  }

  let replayBounds: ReplayBounds | null = null;
  if (boundsR.status === "fulfilled") {
    const r = boundsR.value;
    if (r.firstTimestamp && r.lastTimestamp && r.lastVersion > 0) {
      replayBounds = {
        firstVersion: r.firstVersion,
        lastVersion: r.lastVersion,
        firstDate: new Date(
          Number(r.firstTimestamp.seconds) * 1000 +
            Math.floor(r.firstTimestamp.nanos / 1_000_000),
        ),
        lastDate: new Date(
          Number(r.lastTimestamp.seconds) * 1000 +
            Math.floor(r.lastTimestamp.nanos / 1_000_000),
        ),
        currentPhase:
          r.currentPhase === MarketPhase.UNSPECIFIED
            ? MarketPhase.CONTINUOUS
            : r.currentPhase,
      };
    }
  }

  return { phase, sessionVolume, officialClose, replayBounds, luld };
}

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const account = url.searchParams.get("account") ?? "";
  const symbol = url.searchParams.get("symbol") ?? "";

  const [{ accountIds }, { symbols }, symbolData] = await Promise.all([
    portfolioClient.listPortfolios({}),
    orderBookClient.listSymbols({}),
    symbol
      ? loadSymbolData(symbol)
      : Promise.resolve({
          phase: MarketPhase.CONTINUOUS,
          sessionVolume: 0n,
          officialClose: null as GetOfficialCloseResponse | null,
          replayBounds: null as ReplayBounds | null,
          luld: emptyLULDState(),
        }),
  ]);

  if (!account) {
    return {
      accountIds,
      symbols,
      brackets: [] as BracketRow[],
      ocos: [] as OcoRow[],
      twaps: [] as TwapRow[],
      closingPnl: [] as RealizedPnlRow[],
      marginSnapshot: null as GetMarginSnapshotResponse | null,
      marginCalls: [] as MarginCallRecord[],
      feeHistory: [] as FeeRecord[],
      ...symbolData,
    };
  }

  const [bracketsResp, ocosResp, twapsResp, pnlResp, marginResp, marginCallsResp, feesResp] =
    await Promise.all([
      sagaClient.list({
        accountId: account,
        kind: SagaKind.BRACKET,
        status: SagaStatus.ACTIVE,
      }),
      sagaClient.list({
        accountId: account,
        kind: SagaKind.OCO,
        status: SagaStatus.ACTIVE,
      }),
      sagaClient.list({
        accountId: account,
        kind: SagaKind.TWAP,
        status: SagaStatus.ACTIVE,
      }),
      portfolioClient.getPnL({ accountId: account }),
      portfolioClient.getMarginSnapshot({ accountId: account }),
      portfolioClient.listMarginCalls({ accountId: account, limit: 20 }),
      portfolioClient.listFeeHistory({ accountId: account, limit: FEE_HISTORY_LIMIT }),
    ]);

  const brackets: BracketRow[] = [];
  for (const s of bracketsResp.sagas) {
    if (s.details.case !== "bracket") continue;
    const d = s.details.value;
    brackets.push({
      sagaId: s.sagaId,
      symbol: s.symbol,
      entrySide: d.entrySide,
      entryPrice: d.entryPrice,
      entryQuantity: d.entryQuantity,
      takeProfitPrice: d.takeProfitPrice,
      stopLossPrice: d.stopLossPrice,
      phase: d.phase,
    });
  }

  const ocos: OcoRow[] = [];
  for (const s of ocosResp.sagas) {
    if (s.details.case !== "oco") continue;
    const d = s.details.value;
    ocos.push({
      sagaId: s.sagaId,
      symbol: s.symbol,
      exitSide: d.exitSide,
      quantity: d.quantity,
      takeProfitPrice: d.takeProfitPrice,
      stopLossPrice: d.stopLossPrice,
      settledQuantity: d.settledQuantity,
      phase: d.phase,
    });
  }

  const twaps: TwapRow[] = [];
  for (const s of twapsResp.sagas) {
    if (s.details.case !== "twap") continue;
    const d = s.details.value;
    twaps.push({
      sagaId: s.sagaId,
      symbol: s.symbol,
      side: d.side,
      totalQuantity: d.totalQuantity,
      limitPrice: d.limitPrice,
      totalFilledQuantity: d.totalFilledQuantity,
      totalCashSettled: d.totalCashSettled,
      sliceCount: d.sliceCount,
      slicesLaunched: d.slicesLaunched,
      completedSlices: d.slices.filter((s) => s.completed).length,
      sliceIntervalMs: d.sliceIntervalMs,
    });
  }

  // Pre-filter PnL history to closing fills (realized != 0), cap to the
  // last HISTORY_LIMIT entries, then reverse for newest-first display.
  // The server already orders by settled_at ascending.
  const closingPnl: RealizedPnlRow[] = pnlResp.history
    .filter((h) => h.realizedPnl !== 0n)
    .slice(-PNL_HISTORY_LIMIT)
    .reverse()
    .map((h) => ({
      symbol: h.symbol,
      side: h.side,
      positionSide: h.positionSide,
      quantity: h.quantity,
      price: h.price,
      realizedPnl: h.realizedPnl,
      settledAtMs: h.settledAt
        ? Number(h.settledAt.seconds) * 1000 +
          Math.floor(h.settledAt.nanos / 1_000_000)
        : 0,
    }));

  return {
    accountIds,
    symbols,
    brackets,
    ocos,
    twaps,
    closingPnl,
    // Proto messages flow through turbo-stream as structurally-identical
    // plain objects. Cast back to the proto types so panel consumers
    // (which import the proto types directly) don't have to worry about
    // RR's deeply-readonly loader transforms.
    marginSnapshot: marginResp as GetMarginSnapshotResponse,
    marginCalls: marginCallsResp.calls as MarginCallRecord[],
    feeHistory: feesResp.records as FeeRecord[],
    ...symbolData,
  };
}

// ActionResult mirrors the relevant inputs back to the client so
// notifications can render specifics (amount, symbol, etc.) without
// reaching into fetcher.formData — that field is typed away once the
// fetcher returns to idle.
type ActionResult =
  | {
      ok: true;
      intent: string;
      accountId?: string;
      symbol?: string;
      amount?: number;
      quantity?: number;
      preview?: PreviewOrderImpactResponse;
    }
  | { ok: false; intent: string; error: string };

export async function action({
  request,
}: Route.ActionArgs): Promise<ActionResult> {
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");
  try {
    switch (intent) {
      case "create-account": {
        // Same RPC as "deposit"; kept separate so the trading route can
        // auto-select the new account on success.
        const accountId = String(form.get("accountId") ?? "").trim();
        const amount = Number(form.get("amount") ?? "0");
        if (!accountId) throw new Error("account id required");
        if (!amount || amount <= 0) throw new Error("amount must be > 0");
        await portfolioClient.deposit({
          accountId,
          amount: moneyToPrice(amount),
        });
        return { ok: true, intent, accountId, amount };
      }
      case "deposit": {
        const accountId = String(form.get("accountId") ?? "").trim();
        const amount = Number(form.get("amount") ?? "0");
        if (!accountId) throw new Error("account id required");
        if (!amount || amount <= 0) throw new Error("amount must be > 0");
        await portfolioClient.deposit({
          accountId,
          amount: moneyToPrice(amount),
        });
        return { ok: true, intent, accountId, amount };
      }
      case "withdraw": {
        const accountId = String(form.get("accountId") ?? "").trim();
        const amount = Number(form.get("amount") ?? "0");
        if (!accountId) throw new Error("account id required");
        if (!amount || amount <= 0) throw new Error("amount must be > 0");
        await portfolioClient.withdraw({
          accountId,
          amount: moneyToPrice(amount),
        });
        return { ok: true, intent, accountId, amount };
      }
      case "credit-shares": {
        const accountId = String(form.get("accountId") ?? "").trim();
        const symbol = String(form.get("symbol") ?? "").trim();
        const quantity = Number(form.get("quantity") ?? "0");
        const costPerShare = Number(form.get("costPerShare") ?? "0");
        if (!accountId) throw new Error("account id required");
        if (!symbol) throw new Error("symbol required");
        if (!quantity || quantity <= 0) throw new Error("quantity must be > 0");
        if (!costPerShare || costPerShare <= 0)
          throw new Error("cost per share must be > 0");
        await portfolioClient.creditShares({
          accountId,
          symbol,
          quantity: BigInt(quantity),
          costPerShare: moneyToPrice(costPerShare),
        });
        return { ok: true, intent, accountId, symbol, quantity };
      }
      case "place-saga": {
        // Round-trip the full PlaceSagaRequest through protojson — handles
        // the plan oneOf (single/bracket/oco/twap) and bigint fields
        // without bespoke serialization.
        const requestJson = String(form.get("request") ?? "");
        if (!requestJson) throw new Error("missing request");
        const req = fromJsonString(PlaceSagaRequestSchema, requestJson);
        await sagaClient.place(req);
        return { ok: true, intent };
      }
      case "cancel-saga": {
        const sagaId = String(form.get("sagaId") ?? "");
        if (!sagaId) throw new Error("missing sagaId");
        await sagaClient.cancel({ sagaId });
        return { ok: true, intent };
      }
      case "preview-impact": {
        // Server-side preview of buying-power / margin impact before the
        // user commits an order. Fired by the OrderForm on every
        // (debounced) input change via useFetcher.
        const preview = await portfolioClient.previewOrderImpact({
          accountId: String(form.get("accountId") ?? ""),
          symbol: String(form.get("symbol") ?? ""),
          side: Number(form.get("side") ?? "0") as Side,
          positionSide: Number(
            form.get("positionSide") ?? "0",
          ) as PositionSide,
          orderType: Number(form.get("orderType") ?? "0") as OrderType,
          price: BigInt(String(form.get("price") ?? "0")),
          quantity: BigInt(String(form.get("quantity") ?? "0")),
        });
        return { ok: true, intent, preview };
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
  const {
    accountIds,
    symbols,
    brackets,
    ocos,
    twaps,
    closingPnl,
    phase,
    sessionVolume,
    replayBounds,
    luld,
  } = loaderData;
  // RR's loader serialization deeply-readonly-flattens proto message
  // types in a way that doesn't structurally match the original
  // Message<...>-branded shapes, even though the runtime data is
  // identical. Cast through here so consumer panels keep their natural
  // proto types instead of every prop needing a re-derived shape.
  const marginSnapshot =
    loaderData.marginSnapshot as GetMarginSnapshotResponse | null;
  const marginCalls = loaderData.marginCalls as MarginCallRecord[];
  const feeHistory = loaderData.feeHistory as FeeRecord[];
  const officialClose =
    loaderData.officialClose as GetOfficialCloseResponse | null;
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

  // Periodic refresh — surfaces fills, saga phase changes, P&L updates
  // without a manual click. Pauses while a revalidation is in flight to
  // avoid pile-up on slow networks.
  useEffect(() => {
    if (!account) return;
    const id = window.setInterval(() => {
      if (revalidator.state === "idle") revalidator.revalidate();
    }, REVALIDATE_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, [account, revalidator]);

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
    fd.set("amount", String(dep));
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
        <AccountDataProvider
          accountId={account}
          margin={marginSnapshot}
          marginCalls={marginCalls}
        >
          <OrderStatusNotifier />
          <TradingBody
            symbol={symbol}
            symbols={symbols}
            tab={tab}
            brackets={brackets}
            ocos={ocos}
            twaps={twaps}
            closingPnl={closingPnl}
            feeHistory={feeHistory}
            phase={phase}
            sessionVolume={sessionVolume}
            officialClose={officialClose}
            replayBounds={replayBounds}
            luld={luld}
            onRefreshReplay={() => revalidator.revalidate()}
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
  symbol,
  symbols,
  tab,
  brackets,
  ocos,
  twaps,
  closingPnl,
  feeHistory,
  phase,
  sessionVolume,
  officialClose,
  replayBounds,
  luld,
  onRefreshReplay,
  onTabChange,
  orderPrefill,
  onJumpToAggregate,
  onPrefillOrder,
}: {
  symbol: string;
  symbols: string[];
  tab: Tab;
  brackets: BracketRow[];
  ocos: OcoRow[];
  twaps: TwapRow[];
  closingPnl: RealizedPnlRow[];
  feeHistory: FeeRecord[];
  phase: MarketPhase;
  sessionVolume: bigint;
  officialClose: GetOfficialCloseResponse | null;
  replayBounds: ReplayBounds | null;
  luld: LULDState;
  onRefreshReplay: () => void;
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
          <Tabs.Tab value="fees">Fees</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="trade" pt="md">
          {symbol ? (
            <MarketDepthProvider symbol={symbol}>
              <Grid gutter="md">
                <Grid.Col span={{ base: 12, md: 8 }}>
                  <MarketPanel
                    symbol={symbol}
                    phase={phase}
                    sessionVolume={sessionVolume}
                    officialClose={officialClose}
                    replayBounds={replayBounds}
                    luld={luld}
                    onRefreshReplay={onRefreshReplay}
                  />
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
            <PortfolioOrders
              onJumpToAggregate={onJumpToAggregate}
              onPrefillOrder={onPrefillOrder}
            />
            <BracketsPanel
              rows={brackets}
              onJumpToAggregate={onJumpToAggregate}
            />
            <OcosPanel rows={ocos} onJumpToAggregate={onJumpToAggregate} />
            <TwapsPanel rows={twaps} onJumpToAggregate={onJumpToAggregate} />
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="positions" pt="md">
          <Stack gap="md">
            <PortfolioPositions
              onJumpToAggregate={onJumpToAggregate}
              onPrefillOrder={onPrefillOrder}
            />
            <RecentPnLPanel rows={closingPnl} />
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="fees" pt="md">
          <PortfolioFees rows={feeHistory} />
        </Tabs.Panel>
      </Tabs>
    </Stack>
  );
}


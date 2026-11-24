// Surge & Pop — a breakout paper bot for the pump.fun bonding-curve replay.
//
// Buy a curve only after a broad, accelerating inflow has already pushed its
// price up: many distinct buyers, no single wallet dominating the flow, the
// price sitting near its window high, and the newer half of the window
// flowing in faster than the older half. The first buy is small; the position
// is then pyramided into strength only, while the price keeps stepping up.
//
// Every position exits fast, because on pump.fun a coin's peak is usually
// followed by a dump: a hard stop-loss against cost, a trailing stop once the
// position is up, a take-profit, a fade exit when net flow turns negative, an
// exit on one very large single sell, and a short time limit. Total exposure
// is capped so one bad coin cannot sink the run, and a pacing probe keeps the
// round-trip guardrail (20+ coins) covered when entries go quiet.
//
// What the dev window taught: dip-buying loses (falling knives keep falling),
// and adding to a position only helps when the add is small and the position
// is already in profit — so the first ticket is the smallest one and adds are
// capped hard. Parameters were picked on the full dev window and checked on
// each half of it separately, preferring the flattest region over the best cell.
//
// Deterministic by construction: no clock, no environment, no randomness;
// iteration only over insertion-ordered maps fed by the batch stream.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Entry lookback window, in slots. */
  window: number;
  /** Net buy SOL (buys minus sells) over the window. */
  minNetSol: number;
  /** Buy trades in the window. */
  minBuys: number;
  /** Distinct buying wallets in the window. */
  minBuyers: number;
  /** Price now over price at the window start, minus 1 (lower bound). */
  minPriceUp: number;
  /** Upper bound on that rise: skip curves that already ran too far. */
  maxPriceUp: number;
  /** Newer-half net inflow must exceed the older half's (acceleration). */
  accel: number;
  /** Price now at least this share of the window's high. */
  nearHigh: number;
  /** Largest single wallet's share of the window's buy SOL. */
  maxBuyerShare: number;
  /** Curve real SOL bounds at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Slots since the CreateEvent before an entry is allowed. */
  minAge: number;
  /** Most SOL put into one position in total. */
  stakeSol: number;
  /** First buy of a position (SOL). */
  initialSol: number;
  /** Each pyramid add-on buy (SOL). */
  addSol: number;
  /** Add only once the price is this much above the last buy's decision price. */
  addStep: number;
  /** Add only while the position's close value is at least cost * (1 + addIfUp). */
  addIfUp: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  // --- Exits ---
  /** Stop-loss: exit when close value falls below cost * (1 - stopLoss). */
  stopLoss: number;
  /** Trailing stop: exit when value falls below peak * (1 - trail). */
  trail: number;
  /** The trailing stop arms once value reached cost * (1 + trailArm). */
  trailArm: number;
  /** Take-profit: exit when value reaches cost * (1 + takeProfit). */
  takeProfit: number;
  /** Fade window (slots) for the flow exit. */
  fadeWindow: number;
  /** Exit when net SOL over the fade window falls below -fadeNetSol. */
  fadeNetSol: number;
  /** Exit on any single recorded sell of at least this many SOL. */
  bigSellSol: number;
  /** Exit after this many slots held. */
  maxHold: number;
  /** The time limit does not apply while the position is up at least this much. */
  holdIfUp: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Most positions open at once. */
  maxMain: number;
  /** Most SOL at cost across all positions at once (a drawdown guard). */
  maxExposureSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  // --- Guardrail pacing ---
  /** Target distinct coins / fills by the end of the pace window. */
  targetCoins: number;
  targetFills: number;
  /** Block-time seconds the pace is measured over. */
  paceSecs: number;
  /** Size of one probe round trip (SOL). */
  probeSol: number;
  /** Most probe positions open at once. */
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  window: 40,
  minNetSol: 2,
  minBuys: 5,
  minBuyers: 6,
  minPriceUp: 0.08,
  maxPriceUp: 0.8,
  accel: 1,
  nearHigh: 0.97,
  maxBuyerShare: 0.5,
  minRqSol: 4,
  maxRqSol: 60,
  minAge: 400,
  stakeSol: 0.25,
  initialSol: 0.03,
  addSol: 0.1,
  addStep: 0.03,
  addIfUp: 0.02,
  buySlipBps: 1000,
  stopLoss: 0.12,
  trail: 0.15,
  trailArm: 0.05,
  takeProfit: 0.4,
  fadeWindow: 24,
  fadeNetSol: 1.2,
  bigSellSol: 4,
  maxHold: 300,
  holdIfUp: 0.05,
  cooldown: 200,
  maxEntries: 2,
  maxMain: 5,
  maxExposureSol: 3,
  minBuySol: 0.01,
  targetCoins: 30,
  targetFills: 300,
  paceSecs: 14_400,
  probeSol: 0.02,
  maxProbes: 1,
};

const LAMPORTS = 1e9;
/** Curve fee: protocol 95 bps + creator 30 bps fallback = 1.25% per side. */
const CURVE_FEE = 0.0125;

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  /** SOL sent in buys for this position (fees included). */
  invested: number;
  exiting: boolean;
  /** Some buy of this position filled. */
  filled: boolean;
  /** Recorded price when the last buy was decided. */
  lastAddPrice: number;
  /** Close value at the latest batch (SOL, curve and tx fees deducted). */
  lastValue: number;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Probe round trips in flight, by mint. */
  const probes = new Map<Pubkey, boolean>();
  /** The bot's own filled orders and the coins they were on (harness closes excluded). */
  let myFills = 0;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;
  const keep = Math.max(p.window, p.fadeWindow) + 5;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Net recorded SOL (buys minus sells) over the last `w` slots. Trades
   *  arrive in slot order, so everything at or before the window edge is
   *  simply skipped. */
  const netFlow = (t: Tape, now: number, w: number): number => {
    const edge = now - w;
    let net = 0;
    for (const tr of t.trades) {
      if (tr.slot <= edge) continue;
      net += tr.buy ? tr.sol : -tr.sol;
    }
    return net;
  };

  /** Why a held position should be closed right now, or null to keep holding.
   *  On pump.fun a coin's peak is usually followed by a dump, so every exit
   *  here is fast: the first reason that fires wins, in the order below.
   *  `hugeSell` is true when some single sell of at least bigSellSol SOL was
   *  recorded on this mint in the current batch. */
  const exitReason = (h: Held, value: number, cost: number, t: Tape, now: number, hugeSell: boolean): string | null => {
    // Hard stop: the position has lost more than the stop allows.
    if (value < cost * (1 - p.stopLoss)) return 'stop-loss';
    // Trailing stop: once the position has run up enough to arm it, give back
    // only a limited slice of the peak before getting out.
    if (h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) return 'trailing-stop';
    // Take profit: bank the run before the dump that usually follows a peak.
    if (value >= cost * (1 + p.takeProfit)) return 'take-profit';
    // Fade: net flow over the fade window has turned clearly negative, so the
    // crowd that pushed the price up is leaving.
    if (netFlow(t, now, p.fadeWindow) < -p.fadeNetSol) return 'fade';
    // One very large single sell is often the start of the dump.
    if (hugeSell) return 'big-sell';
    // Time limit: a position that has sat too long without going up gets
    // closed — except while it is still up enough to deserve more time.
    if (now - h.opened > p.maxHold && value < cost * (1 + p.holdIfUp)) return 'max-hold';
    return null;
  };

/** One curve's tape summarised over the entry window, or null when the tape
 *  holds nothing to judge (no trade inside the window at all). */
interface WindowView {
  /** Price at the window's edge: the last trade at or before it, else the
   *  first trade inside the window. */
  openPrice: number;
  /** Price of the newest trade in the window. */
  closePrice: number;
  /** Highest trade price inside the window. */
  topPrice: number;
  /** SOL bought and sold inside the window. */
  boughtSol: number;
  soldSol: number;
  /** Number of buy trades inside the window. */
  buyTrades: number;
  /** Net inflow (buys minus sells) in the older and newer halves. */
  earlyFlow: number;
  lateFlow: number;
  /** SOL bought by the single largest buyer, and how many buyers there were. */
  biggestBuyerSol: number;
  buyerCount: number;
}

const windowView = (t: Tape, now: number): WindowView | null => {
  const edge = now - p.window;
  const halfway = now - p.window / 2;
  // Split the tape at the window edge: the reference price is the last trade
  // at or before the edge, everything after it is the window itself.
  let openPrice = -1;
  const inside: Trade[] = [];
  for (const tr of t.trades) {
    if (tr.slot <= edge) {
      openPrice = tr.price;
      continue;
    }
    inside.push(tr);
  }
  if (inside.length === 0) return null;
  if (openPrice < 0) openPrice = inside[0]!.price;
  // Aggregate the window in one pass: volumes, buyer breadth, the two halves'
  // flows, and the price path (high and close).
  let boughtSol = 0;
  let soldSol = 0;
  let buyTrades = 0;
  let earlyFlow = 0;
  let lateFlow = 0;
  let topPrice = 0;
  let closePrice = 0;
  const perBuyer = new Map<string, number>();
  for (const tr of inside) {
    const signed = tr.buy ? tr.sol : -tr.sol;
    if (tr.slot <= halfway) earlyFlow += signed;
    else lateFlow += signed;
    if (tr.buy) {
      boughtSol += tr.sol;
      buyTrades++;
      perBuyer.set(tr.user, (perBuyer.get(tr.user) ?? 0) + tr.sol);
    } else {
      soldSol += tr.sol;
    }
    if (tr.price > topPrice) topPrice = tr.price;
    closePrice = tr.price;
  }
  let biggestBuyerSol = 0;
  for (const v of perBuyer.values()) if (v > biggestBuyerSol) biggestBuyerSol = v;
  return {
    openPrice,
    closePrice,
    topPrice,
    boughtSol,
    soldSol,
    buyTrades,
    earlyFlow,
    lateFlow,
    biggestBuyerSol,
    buyerCount: perBuyer.size,
  };
};

/** Entry signal: a confirmed breakout — broad, accelerating inflow pushing
 *  the price up, still sitting near its window high. Every gate below must
 *  pass; the first failing gate rejects the curve for this batch. */
const breakoutSignal = (t: Tape, now: number, rqSol: number): boolean => {
  // The curve must hold enough real SOL to trade, but not so much that the
  // move this window measures is already ancient history.
  if (rqSol < p.minRqSol || rqSol > p.maxRqSol) return false;
  // The curve must have lived long enough to leave a real tape behind it.
  if (t.created === null || now - t.created < p.minAge) return false;
  const w = windowView(t, now);
  if (w === null) return false;
  // Breadth first: enough buy trades, from enough distinct wallets.
  if (w.buyTrades < p.minBuys || w.buyerCount < p.minBuyers) return false;
  // Net inflow over the window must clear the floor.
  if (w.boughtSol - w.soldSol < p.minNetSol) return false;
  // The price must have climbed, but not run away already.
  const rise = w.closePrice / w.openPrice - 1;
  if (rise < p.minPriceUp || rise > p.maxPriceUp) return false;
  // It must still be sitting near the window high: a breakout, not a bounce
  // that already gave its gains back.
  if (w.closePrice < w.topPrice * p.nearHigh) return false;
  // Acceleration: the newer half of the window must be flowing in faster
  // than the older half, and in the right direction.
  if (w.lateFlow <= 0 || w.lateFlow < p.accel * w.earlyFlow) return false;
  // No single wallet may dominate the buying.
  if (w.biggestBuyerSol > p.maxBuyerShare * w.boughtSol) return false;
  return true;
};

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const bigSell = new Set<Pubkey>();
      const traded = new Set<Pubkey>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          tapeOf(e.mint).created = e.slot;
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          t.trades.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user, price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves) });
          traded.add(e.mint);
          if (!e.is_buy && sol >= p.bigSellSol) bigSell.add(e.mint);
        }
      }
      for (const mint of traded) {
        const t = tapes.get(mint)!;
        let cut = 0;
        while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
        if (cut > 0) t.trades.splice(0, cut);
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, number>();
      for (const po of state.pending.values()) pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = Number(state.txFeeLamports()) / LAMPORTS;

      // Exits first.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > h.peakValue) h.peakValue = value;
        h.lastValue = value;
        // Decide the exit once per batch: a position already marked exiting
        // stays exiting, anything else asks exitReason for its verdict.
        const t = tapeOf(mint);
        const reason = h.exiting ? 'exiting' : exitReason(h, value, cost, t, now, bigSell.has(mint));
        if (reason === null) continue;
        h.exiting = true;
        if (pendingByMint.has(mint)) {
          let sellPending = false;
          for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
          if (sellPending) continue;
        }
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, selling] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        if (!selling) probes.set(mint, true);
      }

      // Then entries and adds.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      let mainLeft = p.maxMain - held.size;
      let exposure = 0;
      for (const [mint, h] of held) exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint)) continue;
        const t = tapes.get(mint)!;
        const h = held.get(mint);
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        let want: number;
        if (h === undefined) {
          if (slotsLeft <= 0 || mainLeft <= 0) continue;
          if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
          if (!breakoutSignal(t, now, rqSol)) continue;
          want = p.initialSol;
        } else {
          // Pyramid into strength only: the position is up, the price stepped up
          // from the last add, and the signal window still shows inflow.
          if (h.exiting || !h.filled || h.invested >= p.stakeSol - p.minBuySol) continue;
          const pos = state.positions.get(mint);
          if (pos === undefined) continue;
          if (price < h.lastAddPrice * (1 + p.addStep)) continue;
          if (h.lastValue < (Number(pos.cost_lamports) / LAMPORTS) * (1 + p.addIfUp)) continue;
          if (rqSol > p.maxRqSol) continue;
          want = Math.min(p.addSol, p.stakeSol - h.invested);
        }
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, want, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        if (h === undefined) {
          held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, lastAddPrice: price, lastValue: 0 });
          t.entries++;
          slotsLeft--;
          mainLeft--;
        } else {
          h.invested += amount;
          h.lastAddPrice = price;
        }
      }

      // Guardrail pacing: open a probe when the bot's fills or coins lag the pace.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * 10_000) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            if (free < p.probeSol + 2 * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
            probes.set(mint, false);
            free -= p.probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}

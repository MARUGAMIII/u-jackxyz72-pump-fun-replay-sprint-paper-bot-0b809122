// Surge & Pop — a breakout paper bot for the pump.fun bonding-curve replay
// (agent-fun/replay/v2). The grader runs `node /bot/main.ts`; the strategy
// lives in strategy.ts and the SDK shim is the kit's sdk/ts/makefun-bot.ts,
// vendored byte for byte.
//
// It buys only confirmed breakouts — broad, accelerating buy inflow pushing
// the price up near its window high, many distinct buyers, no single wallet
// dominating — starts small, pyramids into strength only, and exits fast
// (stop-loss, trailing stop, take-profit, fading flow, one very large single
// sell, short time limit), because a pump.fun coin's peak is usually followed
// by a dump. See strategy.ts for the full rules and the dev-window evidence.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));

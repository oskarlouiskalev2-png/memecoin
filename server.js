/**
 * MEMECOIN QUANT RELAY
 * --------------------
 * A long-running Node service that:
 *   1. Subscribes to Helius Yellowstone gRPC (LaserStream) for launchpad programs
 *      (pump.fun fully decoded via anchor event CPI; LaunchLab / Meteora DBC /
 *      Moonshot via a balance-delta heuristic).
 *   2. Subscribes to Birdeye's SUBSCRIBE_MEME WebSocket for token metadata,
 *      progress %, sniper/bundler/insider counts, etc.
 *   3. Re-broadcasts everything as normalized JSON over a plain WebSocket that
 *      the browser dashboard connects to.
 *   4. Proxies a few Birdeye / pump.fun REST calls (creator history, wallet PnL,
 *      first-funded) so your API keys never touch the browser.
 *
 * DEPLOY: Railway / Fly.io / Render / any VPS / locally with `node server.js`.
 * NOT Vercel serverless — this process must stay alive and accept inbound WS.
 *
 * ENV:
 *   HELIUS_GRPC_ENDPOINT  e.g. https://laserstream-mainnet-fra.helius-rpc.com
 *   HELIUS_API_KEY        your Helius key (used as the gRPC x-token)
 *   BIRDEYE_API_KEY       your Birdeye key
 *   PORT                  default 8787
 */

import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import Client, { CommitmentLevel } from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PORT = Number(process.env.PORT || 8787);
const HELIUS_GRPC_ENDPOINT = process.env.HELIUS_GRPC_ENDPOINT || "";
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || "";
const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY || "";

const PROGRAMS = {
  PUMP_FUN: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
  RAYDIUM_LAUNCHLAB: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
  METEORA_DBC: "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
  MOONSHOT: "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",
};
const PROGRAM_TO_PLATFORM = {
  [PROGRAMS.PUMP_FUN]: "pump_dot_fun",
  [PROGRAMS.RAYDIUM_LAUNCHLAB]: "raydium_launchlab",
  [PROGRAMS.METEORA_DBC]: "meteora_dynamic_bonding_curve",
  [PROGRAMS.MOONSHOT]: "moonshot",
};

// pump.fun anchor event discriminators (first 8 bytes of "Program data:" payload)
const DISC_TRADE = Buffer.from([189, 219, 127, 211, 78, 230, 97, 238]);
const DISC_CREATE = Buffer.from([27, 114, 169, 77, 222, 235, 99, 118]);

// pump.fun bonding-curve constant: initial real token reserves
const PUMP_INITIAL_REAL_TOKEN_RESERVES = 793_100_000_000_000n;

// Birdeye REST endpoints proxied to the browser. If Birdeye renames a path,
// fix it here — everything else stays the same.
const BIRDEYE_BASE = "https://public-api.birdeye.so";
const BIRDEYE_WS = `wss://public-api.birdeye.so/socket/solana?x-api-key=${BIRDEYE_API_KEY}`;
const BE = {
  walletPnl: (w) => `${BIRDEYE_BASE}/wallet/v2/pnl?wallet=${w}`,
  firstBuyers: (mint, limit = 20) =>
    `${BIRDEYE_BASE}/token/v1/first-buyers?address=${mint}&limit=${limit}`,
  creationInfo: (mint) =>
    `${BIRDEYE_BASE}/defi/token_creation_info?address=${mint}`,
  firstFunded: `${BIRDEYE_BASE}/wallet/v2/tx/first-funded`, // POST { wallets: [] }
};
// pump.fun's public frontend API — used only for "creator's previous launches"
const PUMP_CREATED_COINS = (wallet) =>
  `https://frontend-api.pump.fun/coins/user-created-coins/${wallet}?offset=0&limit=50`;

// ---------------------------------------------------------------------------
// Fan-out: browser clients
// ---------------------------------------------------------------------------
const server = http.createServer(handleHttp);
const wss = new WebSocketServer({ server, path: "/stream" });
const clients = new Set();

wss.on("connection", (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: "hello", ts: Date.now(), programs: PROGRAMS }));
  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// ---------------------------------------------------------------------------
// Helius Yellowstone gRPC
// ---------------------------------------------------------------------------
async function startGrpc() {
  if (!HELIUS_GRPC_ENDPOINT || !HELIUS_API_KEY) {
    console.warn("[grpc] HELIUS_GRPC_ENDPOINT / HELIUS_API_KEY not set — skipping chain feed");
    return;
  }
  const client = new Client(HELIUS_GRPC_ENDPOINT, HELIUS_API_KEY, {
    "grpc.max_receive_message_length": 64 * 1024 * 1024,
  });

  const connect = async () => {
    try {
      const stream = await client.subscribe();
      stream.on("data", onGrpcData);
      stream.on("error", (e) => {
        console.error("[grpc] stream error:", e.message);
        setTimeout(connect, 2000);
      });
      stream.on("end", () => setTimeout(connect, 2000));

      stream.write({
        transactions: {
          launchpads: {
            accountInclude: Object.values(PROGRAMS),
            accountExclude: [],
            accountRequired: [],
            vote: false,
            failed: false,
          },
        },
        accounts: {},
        slots: {},
        blocks: {},
        blocksMeta: {},
        entry: {},
        accountsDataSlice: [],
        commitment: CommitmentLevel.PROCESSED,
        ping: undefined,
      });
      console.log("[grpc] subscribed to launchpad programs");
    } catch (e) {
      console.error("[grpc] connect failed:", e.message);
      setTimeout(connect, 3000);
    }
  };
  connect();
}

function onGrpcData(data) {
  const txu = data?.transaction;
  if (!txu?.transaction) return;
  const tx = txu.transaction;
  const meta = tx.meta;
  if (!meta || meta.err) return;

  const slot = Number(txu.slot ?? 0);
  const sig = tx.signature ? bs58.encode(Buffer.from(tx.signature)) : "";
  const accountKeys = collectAccountKeys(tx, meta);
  const invoked = new Set(accountKeys);
  const now = Date.now();

  // --- pump.fun: decode anchor event CPIs from logs (exact) -----------------
  if (invoked.has(PROGRAMS.PUMP_FUN)) {
    const logs = meta.logMessages || [];
    for (const line of logs) {
      if (!line.startsWith("Program data: ")) continue;
      let buf;
      try {
        buf = Buffer.from(line.slice("Program data: ".length), "base64");
      } catch {
        continue;
      }
      if (buf.length < 8) continue;
      const disc = buf.subarray(0, 8);

      if (disc.equals(DISC_TRADE) && buf.length >= 8 + 32 + 8 + 8 + 1 + 32 + 8 + 32) {
        const t = decodeTradeEvent(buf);
        if (t) {
          broadcast({
            type: "trade",
            platform: "pump_dot_fun",
            decoded: true,
            sig, slot, ts: now,
            mint: t.mint,
            buyer: t.user,
            isBuy: t.isBuy,
            solLamports: t.solAmount.toString(),
            tokenAmount: t.tokenAmount.toString(),
            virtualSolReserves: t.virtualSolReserves.toString(),
            virtualTokenReserves: t.virtualTokenReserves.toString(),
            realSolReserves: t.realSolReserves.toString(),
            realTokenReserves: t.realTokenReserves.toString(),
            priceSol: priceFromVirtual(t.virtualSolReserves, t.virtualTokenReserves),
            progressPct: pumpProgress(t.realTokenReserves),
          });
        }
      } else if (disc.equals(DISC_CREATE)) {
        const c = decodeCreateEvent(buf);
        if (c) {
          broadcast({
            type: "create",
            platform: "pump_dot_fun",
            sig, slot, ts: now,
            mint: c.mint,
            name: c.name,
            symbol: c.symbol,
            uri: c.uri,
            creator: c.user,
            bondingCurve: c.bondingCurve,
          });
        }
      }
    }
    return; // pump handled exactly; don't double-count via heuristic
  }

  // --- other launchpads: balance-delta heuristic (approximate) --------------
  const program = Object.values(PROGRAMS).find(
    (p) => p !== PROGRAMS.PUMP_FUN && invoked.has(p)
  );
  if (!program) return;

  const h = heuristicTrade(tx, meta, accountKeys);
  if (h) {
    broadcast({
      type: "trade",
      platform: PROGRAM_TO_PLATFORM[program],
      decoded: false,
      sig, slot, ts: now,
      mint: h.mint,
      buyer: h.feePayer,
      isBuy: h.isBuy,
      solLamports: h.solLamports.toString(),
      tokenAmount: h.tokenAmount,
      priceSol: h.priceSol,
      progressPct: null, // filled in client-side from Birdeye MEME_DATA
    });
  }
}

function collectAccountKeys(tx, meta) {
  const keys = [];
  const msg = tx.transaction?.message;
  const push = (arr) => {
    for (const k of arr || []) keys.push(bs58.encode(Buffer.from(k)));
  };
  push(msg?.accountKeys);
  push(meta?.loadedWritableAddresses);
  push(meta?.loadedReadonlyAddresses);
  return keys;
}

function decodeTradeEvent(buf) {
  try {
    let o = 8;
    const mint = bs58.encode(buf.subarray(o, o + 32)); o += 32;
    const solAmount = buf.readBigUInt64LE(o); o += 8;
    const tokenAmount = buf.readBigUInt64LE(o); o += 8;
    const isBuy = buf.readUInt8(o) === 1; o += 1;
    const user = bs58.encode(buf.subarray(o, o + 32)); o += 32;
    o += 8; // timestamp i64 (we use wall-clock arrival instead)
    const virtualSolReserves = buf.readBigUInt64LE(o); o += 8;
    const virtualTokenReserves = buf.readBigUInt64LE(o); o += 8;
    const realSolReserves = buf.readBigUInt64LE(o); o += 8;
    const realTokenReserves = buf.readBigUInt64LE(o); o += 8;
    return { mint, solAmount, tokenAmount, isBuy, user,
      virtualSolReserves, virtualTokenReserves, realSolReserves, realTokenReserves };
  } catch {
    return null;
  }
}

function decodeCreateEvent(buf) {
  try {
    let o = 8;
    const readStr = () => {
      const len = buf.readUInt32LE(o); o += 4;
      const s = buf.subarray(o, o + len).toString("utf8"); o += len;
      return s;
    };
    const name = readStr();
    const symbol = readStr();
    const uri = readStr();
    const mint = bs58.encode(buf.subarray(o, o + 32)); o += 32;
    const bondingCurve = bs58.encode(buf.subarray(o, o + 32)); o += 32;
    const user = bs58.encode(buf.subarray(o, o + 32)); o += 32;
    return { name, symbol, uri, mint, bondingCurve, user };
  } catch {
    return null;
  }
}

function priceFromVirtual(vSol, vTok) {
  if (!vTok || vTok === 0n) return null;
  // price in SOL per whole token (token = 6 decimals, SOL = 9)
  return (Number(vSol) / 1e9) / (Number(vTok) / 1e6);
}

function pumpProgress(realTokenReserves) {
  const sold = PUMP_INITIAL_REAL_TOKEN_RESERVES - realTokenReserves;
  const pct = Number((sold * 10000n) / PUMP_INITIAL_REAL_TOKEN_RESERVES) / 100;
  return Math.max(0, Math.min(100, pct));
}

/** Approximate buy/sell for non-pump launchpads from fee-payer balance deltas. */
function heuristicTrade(tx, meta, accountKeys) {
  try {
    const feePayer = accountKeys[0];
    const pre = Number(meta.preBalances?.[0] ?? 0);
    const post = Number(meta.postBalances?.[0] ?? 0);
    const fee = Number(meta.fee ?? 0);
    const solDelta = post - pre + fee; // negative → spent SOL → buy
    if (Math.abs(solDelta) < 1000) return null; // dust / non-swap

    // fee payer's token delta for a non-SOL mint
    let mint = null, tokenAmount = 0;
    const preTB = meta.preTokenBalances || [];
    const postTB = meta.postTokenBalances || [];
    for (const p of postTB) {
      if (accountKeys[p.accountIndex] === undefined) continue;
      if (p.owner !== feePayer) continue;
      if (p.mint === "So11111111111111111111111111111111111111112") continue;
      const before = preTB.find((q) => q.accountIndex === p.accountIndex);
      const d =
        Number(p.uiTokenAmount?.amount ?? 0) -
        Number(before?.uiTokenAmount?.amount ?? 0);
      if (d !== 0) { mint = p.mint; tokenAmount = Math.abs(d); break; }
    }
    if (!mint) return null;

    const isBuy = solDelta < 0;
    const solLamports = BigInt(Math.abs(solDelta));
    const priceSol = tokenAmount > 0
      ? (Number(solLamports) / 1e9) / (tokenAmount / 1e6)
      : null;
    return { feePayer, mint, isBuy, solLamports, tokenAmount, priceSol };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Birdeye SUBSCRIBE_MEME WebSocket
// ---------------------------------------------------------------------------
function startBirdeyeWs() {
  if (!BIRDEYE_API_KEY) {
    console.warn("[birdeye] BIRDEYE_API_KEY not set — skipping meme metadata feed");
    return;
  }
  const connect = () => {
    const ws = new WebSocket(BIRDEYE_WS, "echo-protocol", {
      headers: { Origin: "ws://public-api.birdeye.so" },
    });
    ws.on("open", () => {
      console.log("[birdeye] connected");
      ws.send(JSON.stringify({
        type: "SUBSCRIBE_MEME",
        data: {
          graduated: false,
          progress_percent: { min: 0, max: 100 },
          intervals: ["1m", "5m"],
        },
      }));
    });
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.type === "MEME_DATA") broadcast({ type: "meme", ts: Date.now(), data: msg.data });
      } catch { /* ignore */ }
    });
    ws.on("close", () => setTimeout(connect, 3000));
    ws.on("error", (e) => console.error("[birdeye] ws error:", e.message));
  };
  connect();
}

// ---------------------------------------------------------------------------
// REST proxies (keys stay server-side)
// ---------------------------------------------------------------------------
const beHeaders = {
  "X-API-KEY": BIRDEYE_API_KEY,
  "x-chain": "solana",
  accept: "application/json",
};

async function handleHttp(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.end();
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const send = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };

  try {
    if (url.pathname === "/health") return send(200, { ok: true, clients: clients.size });

    // Creator's previous launches (pump.fun public API) + summary stats
    let m;
    if ((m = url.pathname.match(/^\/api\/creator-history\/([1-9A-HJ-NP-Za-km-z]{32,44})$/))) {
      const r = await fetch(PUMP_CREATED_COINS(m[1]));
      const coins = r.ok ? await r.json() : [];
      const list = Array.isArray(coins) ? coins : [];
      const launches = list.length;
      const graduated = list.filter((c) => c.complete || c.raydium_pool).length;
      const bestMcap = list.reduce((a, c) => Math.max(a, Number(c.usd_market_cap || 0)), 0);
      return send(200, { launches, graduated, gradRate: launches ? graduated / launches : null, bestMcap, raw: list.slice(0, 10) });
    }

    if ((m = url.pathname.match(/^\/api\/wallet-pnl\/([1-9A-HJ-NP-Za-km-z]{32,44})$/))) {
      const r = await fetch(BE.walletPnl(m[1]), { headers: beHeaders });
      return send(r.status, await r.json());
    }

    if ((m = url.pathname.match(/^\/api\/first-buyers\/([1-9A-HJ-NP-Za-km-z]{32,44})$/))) {
      const r = await fetch(BE.firstBuyers(m[1]), { headers: beHeaders });
      return send(r.status, await r.json());
    }

    if ((m = url.pathname.match(/^\/api\/creation-info\/([1-9A-HJ-NP-Za-km-z]{32,44})$/))) {
      const r = await fetch(BE.creationInfo(m[1]), { headers: beHeaders });
      return send(r.status, await r.json());
    }

    if (url.pathname === "/api/first-funded" && req.method === "POST") {
      const body = await readBody(req);
      const r = await fetch(BE.firstFunded, {
        method: "POST",
        headers: { ...beHeaders, "Content-Type": "application/json" },
        body,
      });
      return send(r.status, await r.json());
    }

    send(404, { error: "not found" });
  } catch (e) {
    send(500, { error: e.message });
  }
}

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function readBody(req) {
  return new Promise((resolve) => {
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => resolve(b));
  });
}

// ---------------------------------------------------------------------------
// Helius standard WebSocket fallback (FREE TIER)
// Uses logsSubscribe on the pump.fun program. Decodes the same anchor events
// as the gRPC path, so trades/creates are identical. Limitations vs gRPC:
//   - pump.fun only (logs don't carry the balance metas the other-launchpad
//     heuristic needs)
//   - higher latency, may drop under heavy load — fine for validation
// ---------------------------------------------------------------------------
function startHeliusWsFallback() {
  const url = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  const connect = () => {
    const ws = new WebSocket(url);
    let pingTimer;
    ws.on("open", () => {
      console.log("[helius-ws] connected (free-tier logsSubscribe mode, pump.fun only)");
      ws.send(JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "logsSubscribe",
        params: [{ mentions: [PROGRAMS.PUMP_FUN] }, { commitment: "processed" }],
      }));
      pingTimer = setInterval(() => { try { ws.ping(); } catch {} }, 15000);
    });
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        const v = msg?.params?.result?.value;
        if (!v || v.err) return;
        const now = Date.now();
        for (const line of v.logs || []) {
          if (!line.startsWith("Program data: ")) continue;
          let buf;
          try { buf = Buffer.from(line.slice("Program data: ".length), "base64"); } catch { continue; }
          if (buf.length < 8) continue;
          const disc = buf.subarray(0, 8);
          if (disc.equals(DISC_TRADE)) {
            const t = decodeTradeEvent(buf);
            if (t) broadcast({
              type: "trade", platform: "pump_dot_fun", decoded: true,
              sig: v.signature, slot: 0, ts: now,
              mint: t.mint, buyer: t.user, isBuy: t.isBuy,
              solLamports: t.solAmount.toString(),
              tokenAmount: t.tokenAmount.toString(),
              priceSol: priceFromVirtual(t.virtualSolReserves, t.virtualTokenReserves),
              progressPct: pumpProgress(t.realTokenReserves),
            });
          } else if (disc.equals(DISC_CREATE)) {
            const c = decodeCreateEvent(buf);
            if (c) broadcast({
              type: "create", platform: "pump_dot_fun",
              sig: v.signature, slot: 0, ts: now,
              mint: c.mint, name: c.name, symbol: c.symbol, uri: c.uri,
              creator: c.user, bondingCurve: c.bondingCurve,
            });
          }
        }
      } catch { /* ignore */ }
    });
    ws.on("close", () => { clearInterval(pingTimer); setTimeout(connect, 2000); });
    ws.on("error", (e) => console.error("[helius-ws] error:", e.message));
  };
  connect();
}

// ---------------------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`relay listening on :${PORT}  (ws: /stream)`);
  if (HELIUS_GRPC_ENDPOINT && HELIUS_API_KEY) {
    console.log("[mode] gRPC / LaserStream (all launchpads)");
    startGrpc();
  } else if (HELIUS_API_KEY) {
    console.log("[mode] free-tier logsSubscribe fallback (pump.fun only)");
    startHeliusWsFallback();
  } else {
    console.warn("[mode] no HELIUS_API_KEY — no chain feed");
  }
  startBirdeyeWs();
});

export const DAY_MS = 86_400_000;
export const WEEK_MS = 7 * DAY_MS;
export const GAP_MS = 1_800_000; // 30 minutes
export const LATE_MS = DAY_MS; // 24 hours

export interface PipelineInput {
  manifest: string;
  productsCsv: string;
  campaignsCsv: string;
  eventsCsvs: { name: string; content: string }[];
  clicksCsvs: { name: string; content: string }[];
  linksCsvs: { name: string; content: string }[];
}

export interface UserDailyRow {
  user_id: string;
  day: number;
  day_iso: string;
  sessions: number;
  events: number;
  revenue_cents: number;
  top_category: string | null;
}

export interface CampaignDailyRow {
  campaign_id: string;
  channel: string;
  day: number;
  day_iso: string;
  sessions: number;
  purchasing_sessions: number;
  revenue_cents: number;
  assisted_sessions: number;
}

export interface LinkGroup {
  canonical_user_id: string;
  aliases: string[];
}

export interface PipelineStats {
  window_start_ms: number;
  window_end_ms: number;
  window_start_iso: string;
  window_end_iso: string;
  ndays: number;
  products_count: number;
  campaigns_count: number;
  raw_events_rows: number;
  dropped_invalid_or_late_events: number;
  deduped_duplicate_events: number;
  valid_events_processed: number;
  raw_clicks_rows: number;
  valid_clicks_processed: number;
  link_edges: number;
  resolved_link_groups: number;
  total_sessions: number;
  total_purchasing_sessions: number;
  total_revenue_cents: number;
  execution_ms: number;
}

export interface PipelineResult {
  user_daily: UserDailyRow[];
  campaign_daily: CampaignDailyRow[];
  link_groups: LinkGroup[];
  stats: PipelineStats;
}

function cmpAscii(a: string, b: string): number {
  if (a === b) return 0;
  const minLen = Math.min(a.length, b.length);
  for (let i = 0; i < minLen; i++) {
    const ca = a.charCodeAt(i);
    const cb = b.charCodeAt(i);
    if (ca !== cb) return ca - cb;
  }
  return a.length - b.length;
}

function floorDiv(a: number, b: number): number {
  return Math.floor(a / b);
}

export function dayToIso(day: number): string {
  return new Date(day * DAY_MS).toISOString().slice(0, 10);
}

function unq(f: string): string | null {
  const n = f.length;
  if (n >= 2 && f[0] === '"' && f[n - 1] === '"') {
    const inner = f.slice(1, n - 1);
    return inner.includes('"') ? null : inner;
  }
  if (f.includes('"')) return null;
  return f;
}

function splitExact(line: string, expectedCols: number): string[] | null {
  const parts = line.split(',');
  if (parts.length !== expectedCols) return null;
  return parts;
}

function forLines(csv: string, skipHeader: boolean, cb: (line: string) => void): void {
  const lines = csv.split('\n');
  const start = skipHeader ? 1 : 0;
  for (let i = start; i < lines.length; i++) {
    let line = lines[i];
    if (line.endsWith('\r')) line = line.slice(0, -1);
    if (line.length === 0) continue;
    cb(line);
  }
}

function daysFromCivil(y: number, m: number, d: number): number {
  const yy = m <= 2 ? y - 1 : y;
  const era = Math.trunc((yy >= 0 ? yy : yy - 399) / 400);
  const yoe = yy - era * 400;
  const doy = Math.trunc((153 * (m > 2 ? m - 3 : m + 9) + 2) / 5) + d - 1;
  const doe = yoe * 365 + Math.trunc(yoe / 4) - Math.trunc(yoe / 100) + doy;
  return era * 146097 + doe - 719468;
}

function d2(s: string, i: number): number | null {
  const a = s.charCodeAt(i);
  const b = s.charCodeAt(i + 1);
  if (a >= 48 && a <= 57 && b >= 48 && b <= 57) {
    return (a - 48) * 10 + (b - 48);
  }
  return null;
}

function parseIso(f: string): number | null {
  const n = f.length;
  if (n !== 20 && n !== 25) return null;
  if (f[4] !== '-' || f[7] !== '-' || f[10] !== 'T' || f[13] !== ':' || f[16] !== ':') {
    return null;
  }
  const y1 = d2(f, 0);
  const y2 = d2(f, 2);
  const mo = d2(f, 5);
  const d = d2(f, 8);
  const h = d2(f, 11);
  const mi = d2(f, 14);
  const s = d2(f, 17);
  if (y1 === null || y2 === null || mo === null || d === null || h === null || mi === null || s === null) {
    return null;
  }
  const y = y1 * 100 + y2;
  if (mo < 1 || mo > 12 || h > 23 || mi > 59 || s > 59 || d < 1) return null;
  const leap = y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0);
  const ml =
    mo === 1 || mo === 3 || mo === 5 || mo === 7 || mo === 8 || mo === 10 || mo === 12
      ? 31
      : mo === 4 || mo === 6 || mo === 9 || mo === 11
      ? 30
      : leap
      ? 29
      : 28;
  if (d > ml) return null;

  let off = 0;
  if (n === 20) {
    if (f[19] !== 'Z') return null;
  } else {
    const signChar = f[19];
    const sign = signChar === '+' ? 1 : signChar === '-' ? -1 : 0;
    if (sign === 0 || f[22] !== ':') return null;
    const oh = d2(f, 20);
    const om = d2(f, 23);
    if (oh === null || om === null || oh > 23 || om > 59) return null;
    off = sign * (oh * 60 + om) * 60_000;
  }
  const days = daysFromCivil(y, mo, d);
  return (days * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off;
}

export function parseTs(f: string): number | null {
  if (f.length === 0) return null;
  let allDigits = true;
  for (let i = 0; i < f.length; i++) {
    const c = f.charCodeAt(i);
    if (c < 48 || c > 57) {
      allDigits = false;
      break;
    }
  }
  if (allDigits) {
    let i = 0;
    while (i < f.length && f[i] === '0') i++;
    const g = f.slice(i);
    if (g.length > 18) return null;
    if (g.length === 0) return 0;
    const v = Number(g);
    return Number.isFinite(v) ? v : null;
  }
  return parseIso(f);
}

function parsePrice(f: string): number | null {
  if (f.length === 0) return null;
  let v = 0;
  for (let i = 0; i < f.length; i++) {
    const c = f.charCodeAt(i);
    if (c < 48 || c > 57) return null;
    v = v * 10 + (c - 48);
    if (v > 10_000_000) return null;
  }
  return v === 0 ? null : v;
}

interface RawEvent {
  eid: string;
  ingest: number;
  valid: boolean;
  user: string;
  ts: number;
  purchase: boolean;
  price: number;
  catRank: number;
}

interface CleanEvent {
  ts: number;
  purchase: boolean;
  price: number;
  catRank: number;
}

interface CleanClick {
  ts: number;
  cid: string;
  campIdx: number;
}

function buildLinks(linksCsvs: { name: string; content: string }[]): {
  resolve: (user: string) => string;
  groups: LinkGroup[];
  edgeCount: number;
} {
  const idOf = new Map<string, number>();
  const names: string[] = [];
  const parent: number[] = [];
  const edges: [number, number][] = [];

  const getId = (u: string): number => {
    const existing = idOf.get(u);
    if (existing !== undefined) return existing;
    const id = names.length;
    idOf.set(u, id);
    names.push(u);
    parent.push(id);
    return id;
  };

  for (const file of linksCsvs) {
    forLines(file.content, true, (line) => {
      const f = splitExact(line, 2);
      if (!f) return;
      const a = unq(f[0]);
      const b = unq(f[1]);
      if (a && b && a.length > 0 && b.length > 0) {
        edges.push([getId(a), getId(b)]);
      }
    });
  }

  const find = (x: number): number => {
    let cur = x;
    while (parent[cur] !== cur) {
      parent[cur] = parent[parent[cur]];
      cur = parent[cur];
    }
    return cur;
  };

  for (const [a, b] of edges) {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) {
      if (ra < rb) parent[rb] = ra;
      else parent[ra] = rb;
    }
  }

  const bestForRoot = new Map<number, string>();
  const membersForRoot = new Map<number, string[]>();
  for (let i = 0; i < names.length; i++) {
    const r = find(i);
    const curBest = bestForRoot.get(r);
    if (curBest === undefined || cmpAscii(names[i], curBest) < 0) {
      bestForRoot.set(r, names[i]);
    }
    let arr = membersForRoot.get(r);
    if (!arr) {
      arr = [];
      membersForRoot.set(r, arr);
    }
    arr.push(names[i]);
  }

  const aliasToCanonical = new Map<string, string>();
  const groups: LinkGroup[] = [];
  for (const [r, members] of membersForRoot.entries()) {
    const canonical = bestForRoot.get(r)!;
    members.sort(cmpAscii);
    for (const m of members) {
      aliasToCanonical.set(m, canonical);
    }
    groups.push({ canonical_user_id: canonical, aliases: members });
  }
  groups.sort((a, b) => cmpAscii(a.canonical_user_id, b.canonical_user_id));

  return {
    resolve: (u: string) => aliasToCanonical.get(u) ?? u,
    groups,
    edgeCount: edges.length,
  };
}

function lowerBound<T>(arr: T[], pred: (item: T) => boolean): number {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (pred(arr[mid])) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

export function runPipeline(input: PipelineInput): PipelineResult {
  const t0 = performance.now();

  const manifestObj = JSON.parse(input.manifest) as { window_start?: string | number; window_end?: string | number };
  const wsStr = String(manifestObj.window_start ?? '');
  const weStr = String(manifestObj.window_end ?? '');
  const ws = parseTs(wsStr);
  const we = parseTs(weStr);
  if (ws === null || we === null || we <= ws) {
    throw new Error('Invalid manifest.json: window_start and window_end must be valid timestamps with window_end > window_start.');
  }

  const clickLo = ws - WEEK_MS;
  const day0 = floorDiv(ws, DAY_MS);
  const ndays = Math.max(1, floorDiv(we - 1, DAY_MS) - day0 + 1);

  // Parse products.csv
  const prodRows: [string, string][] = [];
  forLines(input.productsCsv, true, (line) => {
    const f = splitExact(line, 2);
    if (!f) return;
    const pid = unq(f[0]);
    const cat = unq(f[1]);
    if (pid !== null && cat !== null) {
      prodRows.push([pid, cat]);
    }
  });

  const catSet = new Set<string>(prodRows.map((r) => r[1]));
  catSet.add('unknown');
  const catNames = Array.from(catSet).sort(cmpAscii);
  const catRankMap = new Map<string, number>();
  catNames.forEach((c, idx) => catRankMap.set(c, idx));
  const unknownRank = catRankMap.get('unknown')!;
  const prodMap = new Map<string, number>();
  for (const [pid, cat] of prodRows) {
    prodMap.set(pid, catRankMap.get(cat) ?? unknownRank);
  }

  // Parse campaigns.csv
  const campMap = new Map<string, number>();
  const campIds: string[] = [];
  const campCh: string[] = [];
  forLines(input.campaignsCsv, true, (line) => {
    const f = splitExact(line, 2);
    if (!f) return;
    const cid = unq(f[0]);
    const ch = unq(f[1]);
    if (cid === null || ch === null) return;
    if (campMap.has(cid)) return;
    campMap.set(cid, campIds.length);
    campIds.push(cid);
    campCh.push(ch);
  });

  // Phase 1: Links
  const links = buildLinks(input.linksCsvs);

  // Phase 1 & 2: Events + Deduplication
  let rawEventsRows = 0;
  let dupEvents = 0;
  const dedupMap = new Map<string, RawEvent>();

  for (const file of input.eventsCsvs) {
    forLines(file.content, true, (line) => {
      rawEventsRows++;
      const f = splitExact(line, 7);
      if (!f) return;
      const eid = unq(f[0]);
      const user = unq(f[1]);
      const tsStr = unq(f[2]);
      const ingStr = unq(f[3]);
      const ty = unq(f[4]);
      const prod = unq(f[5]);
      const priceStr = unq(f[6]);
      if (eid === null || user === null || tsStr === null || ingStr === null || ty === null || prod === null || priceStr === null) {
        return;
      }
      if (eid.length === 0) return;

      const ingest = parseTs(ingStr) ?? Number.POSITIVE_INFINITY;
      let valid = false;
      let ts = 0;
      let purchase = false;
      let price = 0;
      let catRank = 0;

      if (user.length > 0 && prod.length > 0 && Number.isFinite(ingest)) {
        if (ty === 'view' || ty === 'cart' || ty === 'purchase') {
          const parsedTs = parseTs(tsStr);
          if (parsedTs !== null && parsedTs >= ws && parsedTs < we && ingest - parsedTs <= LATE_MS) {
            if (ty === 'purchase') {
              const p = parsePrice(priceStr);
              if (p !== null) {
                valid = true;
                ts = parsedTs;
                purchase = true;
                price = p;
                catRank = prodMap.get(prod) ?? unknownRank;
              }
            } else {
              valid = true;
              ts = parsedTs;
            }
          }
        }
      }

      const existing = dedupMap.get(eid);
      if (!existing) {
        dedupMap.set(eid, { eid, ingest, valid, user, ts, purchase, price, catRank });
      } else {
        dupEvents++;
        if (ingest < existing.ingest) {
          dedupMap.set(eid, { eid, ingest, valid, user, ts, purchase, price, catRank });
        }
      }
    });
  }

  let droppedEvents = 0;
  let validEvents = 0;
  const personEvents = new Map<string, CleanEvent[]>();
  for (const ev of dedupMap.values()) {
    if (!ev.valid) {
      droppedEvents++;
      continue;
    }
    validEvents++;
    const key = links.resolve(ev.user);
    let arr = personEvents.get(key);
    if (!arr) {
      arr = [];
      personEvents.set(key, arr);
    }
    arr.push({ ts: ev.ts, purchase: ev.purchase, price: ev.price, catRank: ev.catRank });
  }

  // Phase 2: Clicks
  let rawClicksRows = 0;
  let validClicks = 0;
  const personClicks = new Map<string, CleanClick[]>();
  for (const file of input.clicksCsvs) {
    forLines(file.content, true, (line) => {
      rawClicksRows++;
      const f = splitExact(line, 4);
      if (!f) return;
      const cid = unq(f[0]);
      const user = unq(f[1]);
      const camp = unq(f[2]);
      const tsStr = unq(f[3]);
      if (cid === null || user === null || camp === null || tsStr === null) return;
      if (cid.length === 0 || user.length === 0) return;
      const t = parseTs(tsStr);
      if (t === null || t < clickLo || t >= we) return;
      const ci = campMap.get(camp);
      if (ci === undefined) return;

      validClicks++;
      const key = links.resolve(user);
      let arr = personClicks.get(key);
      if (!arr) {
        arr = [];
        personClicks.set(key, arr);
      }
      arr.push({ ts: t, cid, campIdx: ci });
    });
  }

  // Phase 3: Sessionization, Attribution, Assists
  const ncamp = campIds.length;
  const nc1 = ncamp + 1;
  // [sessions, purchasing_sessions, revenue_cents, assisted_sessions]
  const campAcc: [number, number, number, number][] = Array.from({ length: nc1 * ndays }, () => [0, 0, 0, 0]);

  const userDailyRows: UserDailyRow[] = [];
  let totalSessions = 0;
  let totalPurchasingSessions = 0;
  let totalRevenueCents = 0;

  const sortedPersons = Array.from(personEvents.keys()).sort(cmpAscii);

  for (const personKey of sortedPersons) {
    const evs = personEvents.get(personKey)!;
    evs.sort((a, b) => a.ts - b.ts);

    const clicks = personClicks.get(personKey) ?? [];
    if (clicks.length > 1) {
      clicks.sort((a, b) => (a.ts !== b.ts ? a.ts - b.ts : cmpAscii(a.cid, b.cid)));
    }

    const sess: { start: number; rev: number; purch: boolean; runIdx: number }[] = [];
    const runs: { day: number; first: number; end: number; events: number; rev: number; topRank: number | null }[] = [];
    let dayCats = new Map<number, number>();
    let prevTs = Number.NEGATIVE_INFINITY;

    const topCatRank = (cats: Map<number, number>): number | null => {
      let best: number | null = null;
      let bestRev = Number.NEGATIVE_INFINITY;
      for (const [rank, rev] of cats.entries()) {
        if (rev > bestRev || (rev === bestRev && (best === null || rank < best))) {
          bestRev = rev;
          best = rank;
        }
      }
      return best;
    };

    for (const e of evs) {
      if (sess.length === 0 || e.ts - prevTs > GAP_MS) {
        const day = floorDiv(e.ts, DAY_MS);
        const lastRun = runs[runs.length - 1];
        if (!lastRun || lastRun.day !== day) {
          if (lastRun) {
            lastRun.topRank = topCatRank(dayCats);
          }
          dayCats = new Map<number, number>();
          runs.push({ day, first: sess.length, end: sess.length, events: 0, rev: 0, topRank: null });
        }
        const runIdx = runs.length - 1;
        sess.push({ start: e.ts, rev: 0, purch: false, runIdx });
      }
      prevTs = e.ts;
      const curRun = runs[runs.length - 1];
      curRun.events++;
      curRun.end = sess.length;
      if (e.purchase) {
        curRun.rev += e.price;
        const curSess = sess[sess.length - 1];
        curSess.rev += e.price;
        curSess.purch = true;
        dayCats.set(e.catRank, (dayCats.get(e.catRank) ?? 0) + e.price);
      }
    }
    if (runs.length > 0) {
      runs[runs.length - 1].topRank = topCatRank(dayCats);
    }

    totalSessions += sess.length;

    // Last-touch credit
    let j = 0;
    for (let i = 0; i < sess.length; i++) {
      const s = sess[i];
      let campIdx = ncamp; // organic
      if (clicks.length > 0) {
        while (j < clicks.length && clicks[j].ts <= s.start) {
          j++;
        }
        if (j > 0) {
          const last = clicks[j - 1];
          if (s.start - last.ts <= WEEK_MS) {
            const t = last.ts;
            let k = j - 1;
            if (k > 0 && clicks[k - 1].ts === t) {
              k = lowerBound(clicks.slice(0, k), (c) => c.ts < t);
            }
            campIdx = clicks[k].campIdx;
          }
        }
      }
      const dayIdx = runs[s.runIdx].day - day0;
      const cell = campAcc[campIdx * ndays + dayIdx];
      cell[0] += 1;
      if (s.purch) {
        cell[1] += 1;
        totalPurchasingSessions += 1;
      }
      cell[2] += s.rev;
      totalRevenueCents += s.rev;
    }

    // Assisted sessions
    if (clicks.length > 0) {
      const byCamp = clicks.map((c) => ({ campIdx: c.campIdx, ts: c.ts }));
      byCamp.sort((a, b) => (a.campIdx !== b.campIdx ? a.campIdx - b.campIdx : a.ts - b.ts));

      const addAssist = (campIdx: number, loIdx: number, hiIdx: number) => {
        for (const r of runs) {
          const lo = Math.max(loIdx, r.first);
          const hi = Math.min(hiIdx, r.end);
          if (lo < hi) {
            campAcc[campIdx * ndays + (r.day - day0)][3] += hi - lo;
          }
        }
      };

      let idx = 0;
      while (idx < byCamp.length) {
        const campIdx = byCamp[idx].campIdx;
        let cur: [number, number] | null = null;
        while (idx < byCamp.length && byCamp[idx].campIdx === campIdx) {
          const t = byCamp[idx].ts;
          idx++;
          const lo = lowerBound(sess, (s) => s.start < t);
          const hi = lowerBound(sess, (s) => s.start <= t + WEEK_MS);
          if (lo >= hi) continue;
          if (cur && lo <= cur[1]) {
            cur[1] = Math.max(cur[1], hi);
          } else {
            if (cur) addAssist(campIdx, cur[0], cur[1]);
            cur = [lo, hi];
          }
        }
        if (cur) addAssist(campIdx, cur[0], cur[1]);
      }
    }

    // Emit user_daily rows
    for (const r of runs) {
      userDailyRows.push({
        user_id: personKey,
        day: r.day,
        day_iso: dayToIso(r.day),
        sessions: r.end - r.first,
        events: r.events,
        revenue_cents: r.rev,
        top_category: r.topRank !== null ? catNames[r.topRank] : null,
      });
    }
  }

  userDailyRows.sort((a, b) => {
    const c = cmpAscii(a.user_id, b.user_id);
    return c !== 0 ? c : a.day - b.day;
  });

  const campaignDailyRows: CampaignDailyRow[] = [];
  for (let c = 0; c < nc1; c++) {
    for (let d = 0; d < ndays; d++) {
      const a = campAcc[c * ndays + d];
      if (a[0] === 0 && a[3] === 0) continue;
      const day = day0 + d;
      campaignDailyRows.push({
        campaign_id: c < ncamp ? campIds[c] : 'organic',
        channel: c < ncamp ? campCh[c] : 'organic',
        day,
        day_iso: dayToIso(day),
        sessions: a[0],
        purchasing_sessions: a[1],
        revenue_cents: a[2],
        assisted_sessions: a[3],
      });
    }
  }

  const executionMs = Math.round((performance.now() - t0) * 100) / 100;

  return {
    user_daily: userDailyRows,
    campaign_daily: campaignDailyRows,
    link_groups: links.groups,
    stats: {
      window_start_ms: ws,
      window_end_ms: we,
      window_start_iso: new Date(ws).toISOString(),
      window_end_iso: new Date(we).toISOString(),
      ndays,
      products_count: prodMap.size,
      campaigns_count: campIds.length,
      raw_events_rows: rawEventsRows,
      dropped_invalid_or_late_events: droppedEvents,
      deduped_duplicate_events: dupEvents,
      valid_events_processed: validEvents,
      raw_clicks_rows: rawClicksRows,
      valid_clicks_processed: validClicks,
      link_edges: links.edgeCount,
      resolved_link_groups: links.groups.length,
      total_sessions: totalSessions,
      total_purchasing_sessions: totalPurchasingSessions,
      total_revenue_cents: totalRevenueCents,
      execution_ms: executionMs,
    },
  };
}

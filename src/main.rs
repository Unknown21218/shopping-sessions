// Shopping Sessions pipeline: single-binary, multi-threaded, exact to rules.md.
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const LATE_MS: i64 = DAY_MS;
const B1_SHIFT: u32 = 58; // 64 buckets by event-id hash
const B1: usize = 1 << (64 - B1_SHIFT);
const B2_SHIFT: u32 = 58; // 64 buckets by person-key hash
const B2: usize = 1 << (64 - B2_SHIFT);
const CHUNK: usize = 512 << 10; // 512 KB chunks for balanced multi-core scheduling
const NO_CAT: u16 = 0xFFFF;

// ---------------------------------------------------------------- utilities

#[inline(always)]
fn hash_bytes(b: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h: u64 = (b.len() as u64).wrapping_mul(0xff51_afd7_ed55_8ccd) ^ 0x2545_F491_4F6C_DD1D;
    let mut p = b;
    while p.len() >= 8 {
        let w = u64::from_le_bytes(p[..8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(K);
        h ^= h >> 32;
        p = &p[8..];
    }
    if !p.is_empty() {
        let mut buf = [0u8; 8];
        buf[..p.len()].copy_from_slice(p);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(K);
        h ^= h >> 32;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    if h == 0 {
        1
    } else {
        h
    }
}

#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}

#[inline(always)]
fn unpack(p: u64) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts((p & 0xFFFF_FFFF_FFFF) as *const u8, (p >> 48) as usize) }
}

#[inline(always)]
fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

#[inline(always)]
fn has_quote(f: &[u8]) -> bool {
    if f.len() <= 24 {
        f.iter().any(|&c| c == b'"')
    } else {
        memchr(b'"', f).is_some()
    }
}

#[inline(always)]
fn unq(f: &[u8]) -> Option<&[u8]> {
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' {
        let inner = &f[1..n - 1];
        if has_quote(inner) {
            None
        } else {
            Some(inner)
        }
    } else if has_quote(f) {
        None
    } else {
        Some(f)
    }
}

#[inline(always)]
fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut start = 0;
    let mut k = 0;
    for pos in memchr_iter(b',', line) {
        if k >= N - 1 {
            return None;
        }
        out[k] = &line[start..pos];
        k += 1;
        start = pos + 1;
    }
    if k != N - 1 {
        return None;
    }
    out[k] = &line[start..];
    Some(out)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn d2(f: &[u8], i: usize) -> Option<i64> {
    let (a, b) = (f[i], f[i + 1]);
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') as i64) * 10 + (b - b'0') as i64)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
struct IsoDateCache {
    date_bytes: [u8; 10],
    day_ms: i64,
    valid: bool,
}

impl Default for IsoDateCache {
    fn default() -> Self {
        Self { date_bytes: [0; 10], day_ms: 0, valid: false }
    }
}

#[inline(always)]
fn parse_iso_cached(f: &[u8], cache: &mut IsoDateCache) -> Option<i64> {
    let n = f.len();
    if n != 20 && n != 25 {
        return None;
    }
    if f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let day_ms = if cache.valid && f[..10] == cache.date_bytes {
        cache.day_ms
    } else {
        if f[4] != b'-' || f[7] != b'-' {
            return None;
        }
        let y = d2(f, 0)? * 100 + d2(f, 2)?;
        let mo = d2(f, 5)?;
        let d = d2(f, 8)?;
        if !(1..=12).contains(&mo) || d < 1 {
            return None;
        }
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let ml = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if leap {
                    29
                } else {
                    28
                }
            }
        };
        if d > ml {
            return None;
        }
        let dm = days_from_civil(y, mo, d) * 86_400_000;
        cache.date_bytes.copy_from_slice(&f[..10]);
        cache.day_ms = dm;
        cache.valid = true;
        dm
    };

    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let off: i64 = if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match f[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if f[22] != b':' {
            return None;
        }
        let oh = d2(f, 20)?;
        let om = d2(f, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om) * 60_000
    };
    Some(day_ms + (h * 3600 + mi * 60 + s) * 1000 - off)
}

#[inline(always)]
fn parse_ts(f: &[u8], cache: &mut IsoDateCache) -> Option<i64> {
    let n = f.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if f[10] == b'T' {
            return parse_iso_cached(f, cache);
        }
    }
    if f.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < n && f[i] == b'0' {
            i += 1;
        }
        let g = &f[i..];
        if g.len() > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in g {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    parse_iso_cached(f, cache)
}

#[inline(always)]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in f {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

fn json_str(text: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let rest = &rest[a..];
    let b = rest.find('"').unwrap();
    rest[..b].to_string()
}

// ---------------------------------------------------------------- threads

fn par_run<S: Send, M, F>(nthreads: usize, ntasks: usize, order: Option<&[usize]>, mk: M, f: F) -> Vec<S>
where
    M: Fn(usize) -> S + Sync,
    F: Fn(&mut S, usize) + Sync,
{
    par_run_fin(nthreads, ntasks, order, mk, f, |_| {})
}

fn par_run_fin<S: Send, M, F, E>(
    nthreads: usize,
    ntasks: usize,
    order: Option<&[usize]>,
    mk: M,
    f: F,
    fin: E,
) -> Vec<S>
where
    M: Fn(usize) -> S + Sync,
    F: Fn(&mut S, usize) + Sync,
    E: Fn(&mut S) + Sync,
{
    let next = AtomicUsize::new(0);
    thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let next = &next;
                let f = &f;
                let mk = &mk;
                let fin = &fin;
                sc.spawn(move || {
                    let mut s = mk(t);
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        let task = match order {
                            Some(o) => o[i],
                            None => i,
                        };
                        f(&mut s, task);
                    }
                    fin(&mut s);
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn par_drop<T: Send>(mut items: Vec<T>) {
    if items.is_empty() {
        return;
    }
    thread::scope(|sc| {
        for item in items.drain(..) {
            sc.spawn(move || drop(item));
        }
    });
}

// ---------------------------------------------------------------- files

fn load_dir(dir: &str) -> Vec<&'static [u8]> {
    let mut names: Vec<String> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.ends_with(".csv") && e.path().is_file()
            })
            .map(|e| e.path().to_string_lossy().to_string())
            .collect(),
        Err(_) => vec![],
    };
    names.sort();
    names.iter().map(|p| load_file(p)).collect()
}

fn load_file(p: &str) -> &'static [u8] {
    let f = File::open(p).expect("open");
    let len = f.metadata().unwrap().len();
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// (file index, start, end) chunks aligned to line starts; chunk with start==0 holds the header.
fn make_chunks(files: &[&'static [u8]]) -> Vec<(usize, usize, usize)> {
    let mut out = vec![];
    for (fi, d) in files.iter().enumerate() {
        let len = d.len();
        if len == 0 {
            continue;
        }
        let mut bounds = vec![0usize];
        let mut k = 1;
        while k * CHUNK < len {
            let s = k * CHUNK;
            let a = if d[s - 1] == b'\n' {
                s
            } else {
                memchr(b'\n', &d[s..]).map_or(len, |i| s + i + 1)
            };
            if a > *bounds.last().unwrap() && a < len {
                bounds.push(a);
            }
            k += 1;
        }
        bounds.push(len);
        for w in bounds.windows(2) {
            out.push((fi, w[0], w[1]));
        }
    }
    out
}

#[inline(always)]
fn for_lines(data: &[u8], first: bool, mut f: impl FnMut(&[u8])) {
    let mut pos = 0;
    if first {
        pos = memchr(b'\n', data).map_or(data.len(), |i| i + 1);
    }
    while pos < data.len() {
        let end = memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
        f(&data[pos..end]);
        pos = end + 1;
    }
}

// ---------------------------------------------------------------- context tables

struct SmallMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}
impl SmallMap {
    fn new(n: usize) -> Self {
        let cap = (n * 2 + 2).next_power_of_two().max(16);
        SmallMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    fn insert(&mut self, h: u64, v: u32) {
        let mut i = (h as usize) & self.mask;
        loop {
            if self.keys[i] == 0 || self.keys[i] == h {
                self.keys[i] = h;
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, h: u64) -> Option<u32> {
        let mut i = (h as usize) & self.mask;
        loop {
            let k = unsafe { *self.keys.get_unchecked(i) };
            if k == h {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    click_lo: i64,
    day0: i64,
    ndays: usize,
    prod: SmallMap, // product hash -> category rank
    unknown_rank: u16,
    camp: SmallMap, // campaign hash -> idx
    camp_ids: Vec<Vec<u8>>,
    camp_ch: Vec<Vec<u8>>,
    cat_ba: Vec<ByteArray>, // pre-built ByteArrays by rank (zero-alloc clone)
}

fn read_table(path: &str) -> Vec<Vec<Vec<u8>>> {
    let d = load_file(path);
    let mut rows = vec![];
    for_lines(d, true, |l| {
        if let Some(f) = split::<2>(l) {
            if let (Some(a), Some(b)) = (unq(f[0]), unq(f[1])) {
                rows.push(vec![a.to_vec(), b.to_vec()]);
            }
        }
    });
    rows
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C)]
struct Raw {
    eid: u64,
    eptr: u64,
    ingest: i64,
    uh: u64,
    uptr: u64,
    ts: i64,
    price: u32,
    cat: u16,
    flags: u8,
    _p: u8,
}
const F_VALID: u8 = 1;
const F_PURCH: u8 = 2;

#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    ts: i64,
    ptr: u64,
    pc: u64, // price | cat<<32 | purchase<<62
}
#[derive(Clone, Copy)]
struct Ck {
    key: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}

#[inline(always)]
fn parse_event(line: &[u8], cx: &Ctx, cache: &mut IsoDateCache) -> Option<Raw> {
    let f = split::<7>(line)?;
    let eid = unq(f[0])?;
    let user = unq(f[1])?;
    let ts = unq(f[2])?;
    let ing = unq(f[3])?;
    let ty = unq(f[4])?;
    let prod = unq(f[5])?;
    let price = unq(f[6])?;
    if eid.is_empty() {
        return None;
    }
    let ingest = match parse_ts(ing, cache) {
        Some(v) => v,
        None => i64::MAX,
    };
    let mut r = Raw {
        eid: hash_bytes(eid),
        eptr: pack(eid),
        ingest,
        uh: 0,
        uptr: 0,
        ts: 0,
        price: 0,
        cat: 0,
        flags: 0,
        _p: 0,
    };
    if user.is_empty() || prod.is_empty() || ingest == i64::MAX {
        return Some(r);
    }
    let kind = match ty {
        b"view" | b"cart" => 1,
        b"purchase" => 3,
        _ => return Some(r),
    };
    let t = match parse_ts(ts, cache) {
        Some(t) if t >= cx.ws && t < cx.we => t,
        _ => return Some(r),
    };
    if ingest - t > LATE_MS {
        return Some(r);
    }
    let mut flags = F_VALID;
    if kind == 3 {
        match parse_price(price) {
            Some(p) => {
                r.price = p;
                flags |= F_PURCH;
                r.cat = cx.prod.get(hash_bytes(prod)).map_or(cx.unknown_rank as u32, |v| v) as u16;
            }
            None => return Some(r),
        }
    }
    r.flags = flags;
    r.ts = t;
    r.uh = hash_bytes(user);
    r.uptr = pack(user);
    Some(r)
}

// ---------------------------------------------------------------- links

#[derive(Clone, Copy, Default)]
struct Slot {
    key: u64,
    kptr: u64,
    pk: u64,
    pptr: u64,
}
struct LinkTable {
    slots: Vec<Slot>,
    mask: usize,
}
impl LinkTable {
    #[inline(always)]
    fn get(&self, h: u64, raw_key: &[u8]) -> Option<(u64, u64)> {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.key == h && unpack(s.kptr) == raw_key {
                return Some((s.pk, s.pptr));
            }
            if s.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn build_links(files: &[&'static [u8]], nthreads: usize) -> LinkTable {
    let chunks = make_chunks(files);
    if chunks.is_empty() {
        return LinkTable { slots: vec![Slot::default()], mask: 0 };
    }
    let per_thread_pairs: Vec<Vec<(u64, u64, u64, u64)>> = par_run(
        nthreads.min(chunks.len()).max(1),
        chunks.len(),
        None,
        |_| Vec::new(),
        |pairs, ti| {
            let (fi, a, b) = chunks[ti];
            for_lines(&files[fi][a..b], a == 0, |l| {
                if let Some(f) = split::<2>(l) {
                    if let (Some(a), Some(b)) = (unq(f[0]), unq(f[1])) {
                        if !a.is_empty() && !b.is_empty() {
                            pairs.push((hash_bytes(a), pack(a), hash_bytes(b), pack(b)));
                        }
                    }
                }
            });
        },
    );

    let total_pairs: usize = per_thread_pairs.iter().map(|v| v.len()).sum();
    if total_pairs == 0 {
        return LinkTable { slots: vec![Slot::default()], mask: 0 };
    }

    let cap = (total_pairs * 4 + 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut slots = vec![Slot::default(); cap];
    let mut node_of_slot = vec![0u32; cap];
    let mut parent: Vec<u32> = Vec::with_capacity(total_pairs * 2);
    let mut nptr: Vec<u64> = Vec::with_capacity(total_pairs * 2);
    let mut nhash: Vec<u64> = Vec::with_capacity(total_pairs * 2);
    let mut edges: Vec<(u32, u32)> = Vec::with_capacity(total_pairs);

    for pairs in &per_thread_pairs {
        for &(ha, pa, hb, pb) in pairs {
            let mut ids = [0u32; 2];
            for (k, (h, p)) in [(ha, pa), (hb, pb)].into_iter().enumerate() {
                let raw_bytes = unpack(p);
                let mut i = (h as usize) & mask;
                loop {
                    if slots[i].key == h && unpack(slots[i].kptr) == raw_bytes {
                        ids[k] = node_of_slot[i];
                        break;
                    }
                    if slots[i].key == 0 {
                        slots[i].key = h;
                        slots[i].kptr = p;
                        let n = parent.len() as u32;
                        node_of_slot[i] = n;
                        parent.push(n);
                        nptr.push(p);
                        nhash.push(h);
                        ids[k] = n;
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
            edges.push((ids[0], ids[1]));
        }
    }
    drop(per_thread_pairs);

    for &(a, b) in &edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    let n = parent.len();
    let mut best = vec![u32::MAX; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let b = best[r];
        if b == u32::MAX || unpack(nptr[i]) < unpack(nptr[b as usize]) {
            best[r] = i as u32;
        }
    }
    for i in 0..cap {
        if slots[i].key != 0 {
            let node = node_of_slot[i];
            let r = find(&mut parent, node) as usize;
            let b = best[r] as usize;
            slots[i].pk = nhash[b];
            slots[i].pptr = nptr[b];
        }
    }
    LinkTable { slots, mask }
}

// ---------------------------------------------------------------- phase states

struct P1 {
    buckets: Vec<Vec<Raw>>,
    cache: IsoDateCache,
}
struct P2 {
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
    recs_scratch: Vec<Raw>,
    tab_scratch: Vec<u32>,
    cache: IsoDateCache,
}

#[inline(always)]
fn parse_click(line: &[u8], cx: &Ctx, links: &LinkTable, cache: &mut IsoDateCache) -> Option<Ck> {
    let f = split::<4>(line)?;
    let cid = unq(f[0])?;
    let user = unq(f[1])?;
    let camp = unq(f[2])?;
    let ts = unq(f[3])?;
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = match parse_ts(ts, cache) {
        Some(t) if t >= cx.click_lo && t < cx.we => t,
        _ => return None,
    };
    let ci = cx.camp.get(hash_bytes(camp))?;
    let uh = hash_bytes(user);
    let key = match links.get(uh, user) {
        Some((pk, _)) => pk,
        None => uh,
    };
    Some(Ck { key, ts: t, cid: pack(cid), camp: ci })
}

fn dedup_bucket(b: usize, p1: &[P1], links: &LinkTable, st: &mut P2) {
    let n: usize = p1.iter().map(|w| w.buckets[b].len()).sum();
    if n == 0 {
        return;
    }
    st.recs_scratch.clear();
    st.recs_scratch.reserve(n);
    for w in p1 {
        st.recs_scratch.extend_from_slice(&w.buckets[b]);
    }
    let cap = (2 * n).next_power_of_two().max(16);
    let mask = cap - 1;
    st.tab_scratch.clear();
    st.tab_scratch.resize(cap, u32::MAX);

    let recs = &mut st.recs_scratch;
    let tab = &mut st.tab_scratch;

    for i in 0..n {
        let e = recs[i].eid;
        let eb = unpack(recs[i].eptr);
        let mut s = (e as usize) & mask;
        loop {
            let j = unsafe { *tab.get_unchecked(s) };
            if j == u32::MAX {
                unsafe { *tab.get_unchecked_mut(s) = i as u32 };
                break;
            }
            let ju = j as usize;
            if recs[ju].eid == e && unpack(recs[ju].eptr) == eb {
                if recs[i].ingest < recs[ju].ingest {
                    recs[ju].flags = 0;
                    unsafe { *tab.get_unchecked_mut(s) = i as u32 };
                } else {
                    recs[i].flags = 0;
                }
                break;
            }
            s = (s + 1) & mask;
        }
    }
    for r in recs.iter() {
        if r.flags & F_VALID == 0 {
            continue;
        }
        let (key, ptr) = match links.get(r.uh, unpack(r.uptr)) {
            Some(v) => v,
            None => (r.uh, r.uptr),
        };
        let pc = (r.price as u64) | ((r.cat as u64) << 32) | (if r.flags & F_PURCH != 0 { 1u64 << 62 } else { 0 });
        st.ev[(key >> B2_SHIFT) as usize].push(Ev { key, ts: r.ts, ptr, pc });
    }
}

// ---------------------------------------------------------------- phase 3: sessions

struct Sess {
    start: i64,
    rev: i64,
    purch: bool,
    run: u32,
}
struct Run {
    day: i64,
    first: u32,
    end: u32,
    events: i64,
    rev: i64,
    top: u16,
}
struct UserRow {
    ptr: u64,
    day: i32,
    sessions: i64,
    events: i64,
    rev: i64,
    top: u16,
}

type PqWriter = SerializedFileWriter<BufWriter<File>>;

struct W3 {
    id: usize,
    rows: Vec<UserRow>,
    acc: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
    writer: Option<PqWriter>,
    nfiles: usize,
    evbuf: Vec<Ev>,
    ckbuf: Vec<Ck>,
    sess: Vec<Sess>,
    runs: Vec<Run>,
    daycats: Vec<(u16, i64)>,
    tmp: Vec<(u32, i64)>,
}

const USER_SCHEMA: &str = "message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (STRING); }";
const CAMP_SCHEMA: &str = "message campaign_daily { REQUIRED BINARY campaign_id (STRING); REQUIRED BINARY channel (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }";

fn new_writer(path: &str, schema: &str) -> PqWriter {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(1 << 20)
            .build(),
    );
    let f = File::create(path).expect("create output");
    SerializedFileWriter::new(BufWriter::with_capacity(1 << 20, f), schema, props).unwrap()
}

fn flush_user_rows(st: &mut W3, cx: &Ctx, out_dir: &str) {
    if st.rows.is_empty() {
        return;
    }
    if st.writer.is_none() {
        st.writer = Some(new_writer(&format!("{}/user_daily/part-{}.parquet", out_dir, st.id), USER_SCHEMA));
        st.nfiles += 1;
    }
    let w = st.writer.as_mut().unwrap();
    let mut rg = w.next_row_group().unwrap();
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<ByteArray> = st.rows.iter().map(|r| ByteArray::from(unpack(r.ptr).to_vec())).collect();
        c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<i32> = st.rows.iter().map(|r| r.day).collect();
        c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    for sel in 0..3 {
        let mut c = rg.next_column().unwrap().unwrap();
        let v: Vec<i64> = st
            .rows
            .iter()
            .map(|r| match sel {
                0 => r.sessions,
                1 => r.events,
                _ => r.rev,
            })
            .collect();
        c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
    }
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let mut vals: Vec<ByteArray> = Vec::with_capacity(st.rows.len());
        let mut defs: Vec<i16> = Vec::with_capacity(st.rows.len());
        for r in &st.rows {
            if r.top == NO_CAT {
                defs.push(0);
            } else {
                defs.push(1);
                vals.push(cx.cat_ba[r.top as usize].clone());
            }
        }
        c.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).unwrap();
        c.close().unwrap();
    }
    rg.close().unwrap();
    st.rows.clear();
}

fn process_person(evs: &[Ev], clicks: &[Ck], st: &mut W3, cx: &Ctx) {
    let ncamp = cx.camp_ids.len();
    let nd = cx.ndays;
    st.sess.clear();
    st.runs.clear();
    st.daycats.clear();
    let mut prev = i64::MIN;
    for e in evs {
        if st.sess.is_empty() || e.ts - prev > GAP_MS {
            let day = floor_div(e.ts, DAY_MS);
            let new_day = match st.runs.last() {
                Some(r) => r.day != day,
                None => true,
            };
            if new_day {
                if let Some(r) = st.runs.last_mut() {
                    r.top = top_cat(&st.daycats);
                }
                st.daycats.clear();
                st.runs.push(Run { day, first: st.sess.len() as u32, end: 0, events: 0, rev: 0, top: NO_CAT });
            }
            let run = (st.runs.len() - 1) as u32;
            st.sess.push(Sess { start: e.ts, rev: 0, purch: false, run });
        }
        prev = e.ts;
        let run = st.runs.last_mut().unwrap();
        run.events += 1;
        run.end = st.sess.len() as u32;
        if (e.pc >> 62) & 1 == 1 {
            let price = (e.pc & 0xFFFF_FFFF) as i64;
            let cat = ((e.pc >> 32) & 0xFFFF) as u16;
            run.rev += price;
            let s = st.sess.last_mut().unwrap();
            s.rev += price;
            s.purch = true;
            match st.daycats.iter_mut().find(|x| x.0 == cat) {
                Some(x) => x.1 += price,
                None => st.daycats.push((cat, price)),
            }
        }
    }
    if let Some(r) = st.runs.last_mut() {
        r.top = top_cat(&st.daycats);
    }
    // credit
    let nsess = st.sess.len();
    let mut j = 0usize;
    for i in 0..nsess {
        let s = &st.sess[i];
        let mut camp = ncamp;
        if !clicks.is_empty() {
            while j < clicks.len() && clicks[j].ts <= s.start {
                j += 1;
            }
            if j > 0 {
                let last = &clicks[j - 1];
                if s.start - last.ts <= WEEK_MS {
                    let t = last.ts;
                    let mut k = j - 1;
                    if k > 0 && clicks[k - 1].ts == t {
                        k = clicks[..k].partition_point(|c| c.ts < t);
                    }
                    camp = clicks[k].camp as usize;
                }
            }
        }
        let day = (st.runs[s.run as usize].day - cx.day0) as usize;
        let a = &mut st.acc[camp * nd + day];
        a[0] += 1;
        if s.purch {
            a[1] += 1;
        }
        a[2] += s.rev;
    }
    // assists
    if !clicks.is_empty() {
        st.tmp.clear();
        st.tmp.extend(clicks.iter().map(|c| (c.camp, c.ts)));
        st.tmp.sort_unstable();
        let mut i = 0;
        while i < st.tmp.len() {
            let camp = st.tmp[i].0;
            let mut cur: Option<(usize, usize)> = None;
            while i < st.tmp.len() && st.tmp[i].0 == camp {
                let t = st.tmp[i].1;
                i += 1;
                let lo = st.sess.partition_point(|s| s.start < t);
                let hi = st.sess.partition_point(|s| s.start <= t + WEEK_MS);
                if lo >= hi {
                    continue;
                }
                match cur {
                    Some((a, b)) if lo <= b => cur = Some((a, b.max(hi))),
                    Some((a, b)) => {
                        add_assist(st, cx, camp as usize, a, b);
                        cur = Some((lo, hi));
                    }
                    None => cur = Some((lo, hi)),
                }
            }
            if let Some((a, b)) = cur {
                add_assist(st, cx, camp as usize, a, b);
            }
        }
    }
    // user rows
    let ptr = evs[0].ptr;
    for r in &st.runs {
        st.rows.push(UserRow {
            ptr,
            day: r.day as i32,
            sessions: (r.end - r.first) as i64,
            events: r.events,
            rev: r.rev,
            top: r.top,
        });
    }
}

#[inline(always)]
fn add_assist(st: &mut W3, cx: &Ctx, camp: usize, a: usize, b: usize) {
    let nd = cx.ndays;
    for r in &st.runs {
        let lo = a.max(r.first as usize);
        let hi = b.min(r.end as usize);
        if lo < hi {
            st.acc[camp * nd + (r.day - cx.day0) as usize][3] += (hi - lo) as i64;
        }
    }
}

#[inline(always)]
fn top_cat(c: &[(u16, i64)]) -> u16 {
    let mut best = NO_CAT;
    let mut bs = i64::MIN;
    for &(cat, s) in c {
        if s > bs || (s == bs && cat < best) {
            bs = s;
            best = cat;
        }
    }
    best
}

fn phase3_bucket(b: usize, p2: &[P2], st: &mut W3, cx: &Ctx, out_dir: &str) {
    st.evbuf.clear();
    st.ckbuf.clear();
    for w in p2 {
        st.evbuf.extend_from_slice(&w.ev[b]);
        st.ckbuf.extend_from_slice(&w.ck[b]);
    }
    if st.evbuf.is_empty() {
        return;
    }
    st.evbuf.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(a.ts.cmp(&b.ts)));
    st.ckbuf.sort_unstable_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| unpack(a.cid).cmp(unpack(b.cid)))
    });
    let evbuf = std::mem::take(&mut st.evbuf);
    let ckbuf = std::mem::take(&mut st.ckbuf);
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < evbuf.len() {
        let key = evbuf[i].key;
        let mut j = i + 1;
        while j < evbuf.len() && evbuf[j].key == key {
            j += 1;
        }
        while ci < ckbuf.len() && ckbuf[ci].key < key {
            ci += 1;
        }
        let mut ce = ci;
        while ce < ckbuf.len() && ckbuf[ce].key == key {
            ce += 1;
        }
        process_person(&evbuf[i..j], &ckbuf[ci..ce], st, cx);
        ci = ce;
        i = j;
    }
    st.evbuf = evbuf;
    st.ckbuf = ckbuf;
    if st.rows.len() >= 100_000 {
        flush_user_rows(st, cx, out_dir);
    }
}

// ---------------------------------------------------------------- main

fn main() {
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let nthreads = nthreads.max(1);

    let manifest = std::fs::read_to_string(format!("{}/manifest.json", data)).expect("manifest");
    let mut ts_cache = IsoDateCache::default();
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes(), &mut ts_cache).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes(), &mut ts_cache).expect("window_end");

    // products, campaigns
    let prows = read_table(&format!("{}/products.csv", data));
    let mut cats: Vec<Vec<u8>> = prows.iter().map(|r| r[1].clone()).collect();
    cats.sort();
    cats.dedup();
    if !cats.iter().any(|c| c == b"unknown") {
        cats.push(b"unknown".to_vec());
        cats.sort();
    }
    let rank_of = |c: &[u8]| cats.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u32;
    let mut prod = SmallMap::new(prows.len());
    for r in &prows {
        prod.insert(hash_bytes(&r[0]), rank_of(&r[1]));
    }
    let unknown_rank = rank_of(b"unknown") as u16;
    let cat_ba: Vec<ByteArray> = cats.iter().map(|c| ByteArray::from(c.clone())).collect();

    let crows = read_table(&format!("{}/campaigns.csv", data));
    let mut camp = SmallMap::new(crows.len());
    let mut camp_ids = vec![];
    let mut camp_ch = vec![];
    for r in &crows {
        let h = hash_bytes(&r[0]);
        if camp.get(h).is_some() {
            continue;
        }
        camp.insert(h, camp_ids.len() as u32);
        camp_ids.push(r[0].clone());
        camp_ch.push(r[1].clone());
    }
    let day0 = floor_div(ws, DAY_MS);
    let ndays = (floor_div(we - 1, DAY_MS) - day0 + 1).max(1) as usize;
    let cx = Ctx {
        ws,
        we,
        click_lo: ws - WEEK_MS,
        day0,
        ndays,
        prod,
        unknown_rank,
        camp,
        camp_ids,
        camp_ch,
        cat_ba,
    };

    let ev_files = load_dir(&format!("{}/events", data));
    let ck_files = load_dir(&format!("{}/clicks", data));
    let ln_files = load_dir(&format!("{}/links", data));
    let ev_chunks = make_chunks(&ev_files);
    let ck_chunks = make_chunks(&ck_files);

    std::fs::create_dir_all(format!("{}/user_daily", out)).unwrap();
    std::fs::create_dir_all(format!("{}/campaign_daily", out)).unwrap();

    // phase 1: parse events + parallel union-find over links
    let links = build_links(&ln_files, nthreads);
    let p1: Vec<P1> = par_run(
        nthreads,
        ev_chunks.len(),
        None,
        |_| P1 {
            buckets: (0..B1).map(|_| Vec::new()).collect(),
            cache: IsoDateCache::default(),
        },
        |st, ti| {
            let (fi, a, b) = ev_chunks[ti];
            for_lines(&ev_files[fi][a..b], a == 0, |l| {
                if let Some(r) = parse_event(l, &cx, &mut st.cache) {
                    st.buckets[(r.eid >> B1_SHIFT) as usize].push(r);
                }
            });
        },
    );

    // phase 2: clicks + dedup buckets
    let nck = ck_chunks.len();
    let p2: Vec<P2> = par_run(
        nthreads,
        nck + B1,
        None,
        |_| P2 {
            ev: (0..B2).map(|_| Vec::new()).collect(),
            ck: (0..B2).map(|_| Vec::new()).collect(),
            recs_scratch: Vec::new(),
            tab_scratch: Vec::new(),
            cache: IsoDateCache::default(),
        },
        |st, ti| {
            if ti < nck {
                let (fi, a, b) = ck_chunks[ti];
                for_lines(&ck_files[fi][a..b], a == 0, |l| {
                    if let Some(c) = parse_click(l, &cx, &links, &mut st.cache) {
                        st.ck[(c.key >> B2_SHIFT) as usize].push(c);
                    }
                });
            } else {
                dedup_bucket(ti - nck, &p1, &links, st);
            }
        },
    );
    par_drop(p1);
    drop(links);

    // phase 3: sessions, attribution, parallel Parquet output
    let mut sizes: Vec<(usize, usize)> = (0..B2).map(|b| (p2.iter().map(|w| w.ev[b].len()).sum::<usize>(), b)).collect();
    sizes.sort_by(|a, b| b.0.cmp(&a.0));
    let order: Vec<usize> = sizes.iter().map(|x| x.1).collect();
    let nc1 = cx.camp_ids.len() + 1;
    let w3s: Vec<W3> = par_run_fin(
        nthreads,
        B2,
        Some(&order),
        |id| W3 {
            id,
            rows: Vec::new(),
            acc: vec![[0; 4]; nc1 * ndays],
            writer: None,
            nfiles: 0,
            evbuf: Vec::new(),
            ckbuf: Vec::new(),
            sess: Vec::new(),
            runs: Vec::new(),
            daycats: Vec::new(),
            tmp: Vec::new(),
        },
        |st, b| phase3_bucket(b, &p2, st, &cx, &out),
        |st| {
            flush_user_rows(st, &cx, &out);
            if let Some(wr) = st.writer.take() {
                wr.close().unwrap();
            }
        },
    );
    par_drop(p2);

    let mut total = vec![[0i64; 4]; nc1 * ndays];
    let mut nfiles = 0;
    for w in w3s {
        nfiles += w.nfiles;
        for (t, a) in total.iter_mut().zip(w.acc.iter()) {
            for k in 0..4 {
                t[k] += a[k];
            }
        }
    }
    if nfiles == 0 {
        let wr = new_writer(&format!("{}/user_daily/part-0.parquet", out), USER_SCHEMA);
        wr.close().unwrap();
    }

    // campaign_daily
    let mut wr = new_writer(&format!("{}/campaign_daily/part-0.parquet", out), CAMP_SCHEMA);
    let mut cid: Vec<ByteArray> = vec![];
    let mut chn: Vec<ByteArray> = vec![];
    let mut day: Vec<i32> = vec![];
    let mut cols: [Vec<i64>; 4] = [vec![], vec![], vec![], vec![]];
    for c in 0..nc1 {
        for d in 0..ndays {
            let a = total[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c < cx.camp_ids.len() {
                cid.push(ByteArray::from(cx.camp_ids[c].clone()));
                chn.push(ByteArray::from(cx.camp_ch[c].clone()));
            } else {
                cid.push(ByteArray::from(b"organic".to_vec()));
                chn.push(ByteArray::from(b"organic".to_vec()));
            }
            day.push((day0 + d as i64) as i32);
            for k in 0..4 {
                cols[k].push(a[k]);
            }
        }
    }
    if !day.is_empty() {
        let mut rg = wr.next_row_group().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&cid, None, None).unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&chn, None, None).unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int32Type>().write_batch(&day, None, None).unwrap();
        c.close().unwrap();
        for k in 0..4 {
            let mut c = rg.next_column().unwrap().unwrap();
            c.typed::<Int64Type>().write_batch(&cols[k], None, None).unwrap();
            c.close().unwrap();
        }
        rg.close().unwrap();
    }
    wr.close().unwrap();
}

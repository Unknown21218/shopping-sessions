// Shopping Sessions pipeline: single-binary, multi-threaded, exact to rules.md.
use memchr::memchr;
use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
static COLLECT_NS: AtomicU64 = AtomicU64::new(0);
static INDEX_NS: AtomicU64 = AtomicU64::new(0);
static RESOLVE_NS: AtomicU64 = AtomicU64::new(0);
use std::sync::Arc;
use std::thread;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const LATE_MS: i64 = DAY_MS;
const B1_SHIFT: u32 = 55; // 512 buckets by event-id hash
const B1: usize = 1 << (64 - B1_SHIFT);
const B2_SHIFT: u32 = 55; // 512 buckets by person-key hash
const B2: usize = 1 << (64 - B2_SHIFT);
const CHUNK: usize = 8 << 20;
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
        let tail = if b.len() >= 8 {
            // The last word contains the tail in its high bytes.
            let word = u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap());
            word >> ((8 - p.len()) * 8)
        } else {
            let mut tail = 0u64;
            for (i, &byte) in p.iter().enumerate() {
                tail |= u64::from(byte) << (i * 8);
            }
            tail
        };
        h = (h ^ tail).wrapping_mul(K);
        h ^= h >> 32;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    if h == 0 {
        1
    } else {
        h
    }
}

#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    // Reserve the largest length tag for a descriptor for longer strings.
    // The slices all point into the mmaps retained for the life of the process.
    if s.len() >= u16::MAX as usize {
        let descriptor = Box::into_raw(Box::new((s.as_ptr(), s.len())));
        (descriptor as u64) | ((u16::MAX as u64) << 48)
    } else {
        (s.as_ptr() as u64) | ((s.len() as u64) << 48)
    }
}
#[inline(always)]
fn unpack(p: u64) -> &'static [u8] {
    let ptr = p & 0xFFFF_FFFF_FFFF;
    let len = (p >> 48) as usize;
    unsafe {
        if len == u16::MAX as usize {
            let &(ptr, len) = &*(ptr as *const (*const u8, usize));
            std::slice::from_raw_parts(ptr, len)
        } else {
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }
}

#[inline(always)]
fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

#[inline(always)]
fn split_scalar<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut pos = 0;
    for (i, field) in out.iter_mut().enumerate() {
        let quoted = line.get(pos) == Some(&b'"');
        pos += quoted as usize;
        let start = pos;
        while pos < line.len() && line[pos] != b',' && line[pos] != b'"' {
            pos += 1;
        }
        *field = &line[start..pos];
        if quoted {
            if line.get(pos) != Some(&b'"') {
                return None;
            }
            pos += 1;
        }
        if i + 1 < N {
            if line.get(pos) != Some(&b',') {
                return None;
            }
            pos += 1;
        } else if pos != line.len() {
            return None;
        }
    }
    Some(out)
}

#[inline(always)]
fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if (32..=128).contains(&line.len()) {
        // Both vector parsers load only bytes within the slice.
        if std::arch::is_x86_feature_detected!("avx512bw") {
            return unsafe { split_avx512(line) };
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe { split_avx2(line) };
        }
    }
    split_scalar(line)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn split_avx2<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;

    let comma = _mm256_set1_epi8(b',' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let mut commas = 0u128;
    let mut quotes = 0u128;
    // The final vector overlaps the preceding block for a partial tail.
    // OR-ing the masks counts each byte only once.
    let mut offset = 0;
    loop {
        let v = _mm256_loadu_si256(line.as_ptr().add(offset).cast());
        commas |= (u128::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, comma)) as u32)) << offset;
        quotes |= (u128::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, quote)) as u32)) << offset;
        if offset + 32 == line.len() {
            break;
        }
        offset = (offset + 32).min(line.len() - 32);
    }
    split_masks(line, commas, quotes)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
unsafe fn split_avx512<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;

    let comma = _mm512_set1_epi8(b',' as i8);
    let quote = _mm512_set1_epi8(b'"' as i8);
    let ptr = line.as_ptr();
    let low_mask = u64::MAX >> 64usize.saturating_sub(line.len());
    let lo = _mm512_maskz_loadu_epi8(low_mask, ptr.cast());
    let mut commas = u128::from(_mm512_cmpeq_epi8_mask(lo, comma));
    let mut quotes = u128::from(_mm512_cmpeq_epi8_mask(lo, quote));
    if line.len() > 64 {
        let high_mask = u64::MAX >> (128 - line.len());
        let hi = _mm512_maskz_loadu_epi8(high_mask, ptr.add(64).cast());
        commas |= u128::from(_mm512_cmpeq_epi8_mask(hi, comma)) << 64;
        quotes |= u128::from(_mm512_cmpeq_epi8_mask(hi, quote)) << 64;
    }
    split_masks(line, commas, quotes)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn split_masks<const N: usize>(line: &[u8], mut commas: u128, quotes: u128) -> Option<[&[u8]; N]> {
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0;
    let mut expected_quotes = 0u128;
    for (i, field) in out.iter_mut().enumerate() {
        let end = if i + 1 == N { line.len() } else { commas.trailing_zeros() as usize };
        commas &= commas.wrapping_sub(1);
        if end >= start + 2 && line[start] == b'"' {
            expected_quotes |= (1u128 << start) | (1u128 << (end - 1));
            *field = &line[start + 1..end - 1];
        } else {
            *field = &line[start..end];
        }
        start = end + 1;
    }
    (quotes == expected_quotes).then_some(out)
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

// A small worker-local cache avoids repeating calendar arithmetic. The
// full date is compared on a hit, so arbitrary dates can share a slot.
struct DateCache([([u8; 10], i64); 32]);

impl DateCache {
    fn new() -> Self {
        Self([(*b"1970-01-01", 0); 32])
    }
}

fn parse_iso(f: &[u8], cache: &mut DateCache) -> Option<i64> {
    let n = f.len();
    if n != 20 && n != 25 {
        return None;
    }
    if f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let slot = ((f[8] as usize) * 10 + f[9] as usize) & 31;
    let entry = &mut cache.0[slot];
    let day_ms = if f[..10] == entry.0 {
        entry.1
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
            _ => 28 + i64::from(leap),
        };
        if d > ml {
            return None;
        }
        let value = days_from_civil(y, mo, d) * DAY_MS;
        entry.0.copy_from_slice(&f[..10]);
        entry.1 = value;
        value
    };
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
fn parse_ts(mut f: &[u8], cache: &mut DateCache) -> Option<i64> {
    if f.is_empty() {
        return None;
    }
    if matches!(f.len(), 20 | 25) && f[10] == b'T' {
        return parse_iso(f, cache);
    }
    while f.len() > 18 && f.first() == Some(&b'0') {
        f = &f[1..];
    }
    if f.len() > 18 {
        return None;
    }
    let mut v = 0i64;
    for &c in f {
        let digit = c.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        v = v * 10 + i64::from(digit);
    }
    Some(v)
}

#[inline(always)]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() || !f.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut v: u64 = 0;
    for &c in f {
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

fn par_run<S: Send, M, F, D>(nthreads: usize, ntasks: usize, order: Option<&[usize]>, mk: M, f: F, done: D) -> Vec<S>
where
    M: Fn(usize) -> S + Sync,
    F: Fn(&mut S, usize) + Sync,
    D: Fn(&mut S) + Sync,
{
    let next = AtomicUsize::new(0);
    thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let next = &next;
                let f = &f;
                let mk = &mk;
                let done = &done;
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
                    done(&mut s);
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
unsafe fn parse_events_fast(data: &[u8], first: bool, cx: &Ctx, st: &mut P1) {
    use std::arch::x86_64::*;
    let mut pos = if first { memchr(b'\n', data).map_or(data.len(), |i| i + 1) } else { 0 };
    let comma = _mm512_set1_epi8(b',' as i8);
    let quote = _mm512_set1_epi8(b'"' as i8);
    let newline = _mm512_set1_epi8(b'\n' as i8);
    while pos < data.len() {
        let available = data.len() - pos;
        let m = u64::MAX >> (64usize.saturating_sub(available));
        let v = _mm512_maskz_loadu_epi8(m, data.as_ptr().add(pos).cast());
        let newlines = _mm512_cmpeq_epi8_mask(v, newline);
        let mut commas = u128::from(_mm512_cmpeq_epi8_mask(v, comma));
        let mut quotes = u128::from(_mm512_cmpeq_epi8_mask(v, quote));
        let mut len = if newlines != 0 { newlines.trailing_zeros() as usize } else { available.min(64) };
        if len == 64 && available > 64 {
            let m = u64::MAX >> (128usize.saturating_sub(available));
            let v = _mm512_maskz_loadu_epi8(m, data.as_ptr().add(pos + 64).cast());
            let newlines = _mm512_cmpeq_epi8_mask(v, newline);
            commas |= u128::from(_mm512_cmpeq_epi8_mask(v, comma)) << 64;
            quotes |= u128::from(_mm512_cmpeq_epi8_mask(v, quote)) << 64;
            len = if newlines != 0 { 64 + newlines.trailing_zeros() as usize } else { available.min(128) };
        }
        if len == 128 && available > 128 {
            let end = memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
            if let Some(r) = parse_event(&data[pos..end], cx, &mut st.dates) { st.buckets[(r.eid >> B1_SHIFT) as usize].push(r); }
            pos = end + 1;
        } else {
            let mask = if len == 128 { u128::MAX } else { (1u128 << len) - 1 };
            if let Some(f) = split_masks::<7>(&data[pos..pos + len], commas & mask, quotes & mask) {
                if let Some(r) = parse_event_fields(f, cx, &mut st.dates) { st.buckets[(r.eid >> B1_SHIFT) as usize].push(r); }
            }
            pos += len + 1;
        }
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
    cat_names: Vec<Vec<u8>>, // by rank
}

fn read_table(path: &str) -> Vec<Vec<Vec<u8>>> {
    let d = load_file(path);
    let mut rows = vec![];
    for_lines(d, true, |l| {
        if let Some(f) = split::<2>(l) {
            rows.push(vec![f[0].to_vec(), f[1].to_vec()]);
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
fn parse_event(line: &[u8], cx: &Ctx, dates: &mut DateCache) -> Option<Raw> {
    parse_event_fields(split::<7>(line)?, cx, dates)
}

#[inline(always)]
fn parse_event_fields(f: [&[u8]; 7], cx: &Ctx, dates: &mut DateCache) -> Option<Raw> {
    let [eid, user, ts, ing, ty, prod, price] = f;
    if eid.is_empty() {
        return None;
    }
    let ingest = match parse_ts(ing, dates) {
        Some(v) => v,
        None => i64::MAX,
    };
    let mut r = Raw { eid: hash_bytes(eid), eptr: pack(eid), ingest, uh: 0, uptr: 0, ts: 0, price: 0, cat: 0, flags: 0, _p: 0 };
    if user.is_empty() || prod.is_empty() || ingest == i64::MAX {
        return Some(r);
    }
    let kind = match ty {
        b"view" | b"cart" => 1,
        b"purchase" => 3,
        _ => return Some(r),
    };
    let t = match parse_ts(ts, dates) {
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

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn build_links(files: &[&'static [u8]]) -> LinkTable {
    let mut pairs: Vec<(u64, u64, u64, u64)> = vec![];
    for d in files {
        for_lines(d, true, |l| {
            if let Some(f) = split::<2>(l) {
                let [a, b] = f;
                if !a.is_empty() && !b.is_empty() {
                    pairs.push((hash_bytes(a), pack(a), hash_bytes(b), pack(b)));
                }
            }
        });
    }
    if pairs.is_empty() {
        return LinkTable { slots: vec![Slot::default()], mask: 0 };
    }
    let cap = (pairs.len() * 4 + 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut slots = vec![Slot::default(); cap];
    let mut node_of_slot = vec![0u32; cap];
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    let mut nptr: Vec<u64> = Vec::with_capacity(pairs.len() * 2);
    let mut nhash: Vec<u64> = Vec::with_capacity(pairs.len() * 2);
    let mut edges: Vec<(u32, u32)> = Vec::with_capacity(pairs.len());
    for &(ha, pa, hb, pb) in &pairs {
        let mut ids = [0u32; 2];
        for (k, (h, p)) in [(ha, pa), (hb, pb)].into_iter().enumerate() {
            let mut i = (h as usize) & mask;
            loop {
                if slots[i].key == h && unpack(slots[i].kptr) == unpack(p) {
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
    drop(pairs);
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
    dates: DateCache,
    buckets: Vec<Vec<Raw>>,
}
struct P2 {
    dates: DateCache,
    recs: Vec<Raw>,
    slots: Vec<u32>,
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
}

fn parse_click(line: &[u8], cx: &Ctx, links: &LinkTable, dates: &mut DateCache) -> Option<Ck> {
    let f = split::<4>(line)?;
    let [cid, user, camp, ts] = f;
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = match parse_ts(ts, dates) {
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
    let bucket_time = std::time::Instant::now();
    let recs = &mut st.recs;
    recs.clear();
    recs.reserve(n);
    for w in p1 {
        recs.extend_from_slice(&w.buckets[b]);
    }
    let cap = (2 * n).next_power_of_two().max(16);
    let mask = cap - 1;
    let tab = &mut st.slots;
    tab.clear();
    tab.resize(cap, 0);

    COLLECT_NS.fetch_add(bucket_time.elapsed().as_nanos() as u64, Ordering::Relaxed);
    let bucket_time = std::time::Instant::now();
    // Keep a short hash tag beside each record index. Most occupied
    // slots have a different tag, so probing does not touch the record.
    let index_bits = u32::try_from(n).expect("dedup bucket too large");
    let index_mask = u32::MAX >> index_bits.leading_zeros();
    for i in 0..n {
        #[cfg(target_arch = "x86_64")]
        if i + 16 < n {
            let next = (recs[i + 16].eid as usize) & mask;
            unsafe { std::arch::x86_64::_mm_prefetch(tab.as_ptr().add(next).cast(), std::arch::x86_64::_MM_HINT_T0); }
        }
        let e = recs[i].eid;
        let tag = ((e >> 32) as u32) & !index_mask;
        let slot_value = tag | (i as u32 + 1);
        let mut s = (e as usize) & mask;
        loop {
            let j = tab[s];
            if j == 0 {
                tab[s] = slot_value;
                break;
            }
            if j & !index_mask == tag {
                let ju = ((j & index_mask) - 1) as usize;
                if recs[ju].eid == e && unpack(recs[ju].eptr) == unpack(recs[i].eptr) {
                    if recs[i].ingest < recs[ju].ingest {
                        recs[ju].flags = 0;
                        tab[s] = slot_value;
                    } else {
                        recs[i].flags = 0;
                    }
                    break;
                }
            }
            s = (s + 1) & mask;
        }
    }
    INDEX_NS.fetch_add(bucket_time.elapsed().as_nanos() as u64, Ordering::Relaxed);
    let bucket_time = std::time::Instant::now();
    for i in 0..recs.len() {
        #[cfg(target_arch = "x86_64")]
        if i + 16 < recs.len() {
            let slot = (recs[i + 16].uh as usize) & links.mask;
            unsafe { std::arch::x86_64::_mm_prefetch(links.slots.as_ptr().add(slot).cast(), std::arch::x86_64::_MM_HINT_T0); }
        }
        let r = &recs[i];
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
    RESOLVE_NS.fetch_add(bucket_time.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
    groups: Vec<Group>,
    index: Vec<u32>,
    ev_ids: Vec<u32>,
    ck_ids: Vec<u32>,
    ordered_ev: Vec<Ev>,
    ordered_ck: Vec<Ck>,
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
        let mut vals: Vec<ByteArray> = Vec::new();
        let mut defs: Vec<i16> = Vec::with_capacity(st.rows.len());
        for r in &st.rows {
            if r.top == NO_CAT {
                defs.push(0);
            } else {
                defs.push(1);
                vals.push(ByteArray::from(cx.cat_names[r.top as usize].clone()));
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

#[derive(Clone, Copy)]
struct Group {
    key: u64,
    ec: usize,
    cc: usize,
    es: usize,
    cs: usize,
    ei: usize,
    ci: usize,
}

fn phase3_bucket(b: usize, p2: &[P2], st: &mut W3, cx: &Ctx, out_dir: &str) {
    st.evbuf.clear();
    st.ckbuf.clear();
    for w in p2 {
        st.evbuf.extend_from_slice(&w.ev[b]);
        st.ckbuf.extend_from_slice(&w.ck[b]);
    }
    let n = st.evbuf.len();
    if n == 0 { return; }
    let cap = (2*n).next_power_of_two().max(16);
    let mask = cap - 1;
    st.index.clear(); st.index.resize(cap, u32::MAX);
    st.groups.clear(); st.ev_ids.clear(); st.ck_ids.clear();
    for e in &st.evbuf {
        let mut j = (e.key as usize) & mask;
        loop {
            let gid = st.index[j];
            if gid == u32::MAX {
                let id = st.groups.len() as u32;
                st.groups.push(Group {key:e.key, ec:1, cc:0, es:0, cs:0, ei:0, ci:0});
                st.index[j] = id;
                st.ev_ids.push(id);
                break;
            }
            if st.groups[gid as usize].key == e.key {
                st.groups[gid as usize].ec += 1;
                st.ev_ids.push(gid);
                break;
            }
            j = (j + 1) & mask;
        }
    }
    for ck in &st.ckbuf {
        let mut j = (ck.key as usize) & mask;
        loop {
            let gid = st.index[j];
            if gid == u32::MAX { st.ck_ids.push(gid); break; }
            if st.groups[gid as usize].key == ck.key {
                st.groups[gid as usize].cc += 1;
                st.ck_ids.push(gid);
                break;
            }
            j = (j+1)&mask;
        }
    }
    let mut es = 0; let mut cs = 0;
    for g in &mut st.groups {
        g.es = es; g.ei = es; es += g.ec;
        g.cs = cs; g.ci = cs; cs += g.cc;
    }
    st.ordered_ev.clear(); st.ordered_ev.extend_from_slice(&st.evbuf);
    st.ordered_ck.clear(); st.ordered_ck.extend_from_slice(&st.ckbuf);
    for (e, &gid) in st.evbuf.iter().zip(&st.ev_ids) {
        let g = &mut st.groups[gid as usize];
        st.ordered_ev[g.ei] = *e; g.ei += 1;
    }
    for (ck, &gid) in st.ckbuf.iter().zip(&st.ck_ids) {
        if gid != u32::MAX {
            let g = &mut st.groups[gid as usize];
            st.ordered_ck[g.ci] = *ck; g.ci += 1;
        }
    }
    let mut ev = std::mem::take(&mut st.ordered_ev);
    let mut ck = std::mem::take(&mut st.ordered_ck);
    let groups = std::mem::take(&mut st.groups);
    for g in &groups {
        let events = &mut ev[g.es..g.es+g.ec];
        let clicks = &mut ck[g.cs..g.cs+g.cc];
        events.sort_unstable_by_key(|e| e.ts);
        clicks.sort_unstable_by(|a,b| a.ts.cmp(&b.ts).then_with(|| unpack(a.cid).cmp(unpack(b.cid))));
        process_person(events, clicks, st, cx);
    }
    st.ordered_ev = ev; st.ordered_ck = ck; st.groups = groups;
    if st.rows.len() >= 200_000 { flush_user_rows(st,cx,out_dir); }
}

// ---------------------------------------------------------------- main

fn main() {
    let overall=std::time::Instant::now();
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let nthreads = nthreads.max(1);

    let manifest = std::fs::read_to_string(format!("{}/manifest.json", data)).expect("manifest");
    let mut dates = DateCache::new();
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes(), &mut dates).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes(), &mut dates).expect("window_end");

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
        cat_names: cats.clone(),
    };

    let ev_files = load_dir(&format!("{}/events", data));
    let ck_files = load_dir(&format!("{}/clicks", data));
    let ln_files = load_dir(&format!("{}/links", data));
    let ev_chunks = make_chunks(&ev_files);
    let ck_chunks = make_chunks(&ck_files);

    std::fs::create_dir_all(format!("{}/user_daily", out)).unwrap();
    std::fs::create_dir_all(format!("{}/campaign_daily", out)).unwrap();

    eprintln!("setup {:?}", overall.elapsed());
    let stage = std::time::Instant::now();
    // phase 1: parse events (+ links in a side thread)
    let (links, p1): (LinkTable, Vec<P1>) = thread::scope(|sc| {
        let lh = sc.spawn(|| build_links(&ln_files));
        let p1 = par_run(
            nthreads,
            ev_chunks.len(),
            None,
            |_| P1 { dates: DateCache::new(), buckets: (0..B1).map(|_| Vec::new()).collect() },
            |st, ti| {
                let (fi, a, b) = ev_chunks[ti];
                let data = &ev_files[fi][a..b];
                #[cfg(target_arch = "x86_64")]
                if std::arch::is_x86_feature_detected!("avx512bw") {
                    unsafe { parse_events_fast(data, a == 0, &cx, st); }
                } else {
                    for_lines(data, a == 0, |l| {
                        if let Some(r) = parse_event(l, &cx, &mut st.dates) {
                            st.buckets[(r.eid >> B1_SHIFT) as usize].push(r);
                        }
                    });
                }
                #[cfg(not(target_arch = "x86_64"))]
                for_lines(data, a == 0, |l| {
                    if let Some(r) = parse_event(l, &cx, &mut st.dates) {
                        st.buckets[(r.eid >> B1_SHIFT) as usize].push(r);
                    }
                });
            },
            |_| {},
        );
        (lh.join().unwrap(), p1)
    });

    eprintln!("parse {:?}", stage.elapsed());
    let stage = std::time::Instant::now();
    // phase 2: clicks + dedup buckets
    let nck = ck_chunks.len();
    let p2: Vec<P2> = par_run(
        nthreads,
        nck + B1,
        None,
        |_| P2 { dates: DateCache::new(), recs: Vec::new(), slots: Vec::new(), ev: (0..B2).map(|_| Vec::new()).collect(), ck: (0..B2).map(|_| Vec::new()).collect() },
        |st, ti| {
            if ti < nck {
                let (fi, a, b) = ck_chunks[ti];
                for_lines(&ck_files[fi][a..b], a == 0, |l| {
                    if let Some(c) = parse_click(l, &cx, &links, &mut st.dates) {
                        st.ck[(c.key >> B2_SHIFT) as usize].push(c);
                    }
                });
            } else {
                dedup_bucket(ti - nck, &p1, &links, st);
            }
        },
        |_| {},
    );
    drop(p1);
    drop(links);

    eprintln!("dedup {:?}, sum of worker times: collect {}ms index {}ms resolve {}ms", stage.elapsed(), COLLECT_NS.load(Ordering::Relaxed)/1000000, INDEX_NS.load(Ordering::Relaxed)/1000000, RESOLVE_NS.load(Ordering::Relaxed)/1000000);
    let stage = std::time::Instant::now();
    // phase 3: sessions, attribution, output
    let mut sizes: Vec<(usize, usize)> = (0..B2).map(|b| (p2.iter().map(|w| w.ev[b].len()).sum::<usize>(), b)).collect();
    sizes.sort_by(|a, b| b.0.cmp(&a.0));
    let order: Vec<usize> = sizes.iter().map(|x| x.1).collect();
    let nc1 = cx.camp_ids.len() + 1;
    let w3s: Vec<W3> = par_run(
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
            groups: Vec::new(),
            index: Vec::new(),
            ev_ids: Vec::new(),
            ck_ids: Vec::new(),
            ordered_ev: Vec::new(),
            ordered_ck: Vec::new(),
        },
        |st, b| phase3_bucket(b, &p2, st, &cx, &out),
        |st| {
            flush_user_rows(st, &cx, &out);
            if let Some(wr) = st.writer.take() {
                wr.close().unwrap();
            }
        },
    );
    drop(p2);

    eprintln!("sessions {:?}", stage.elapsed());
    let stage = std::time::Instant::now();
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

    eprintln!("reduce {:?}", stage.elapsed());
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
    eprintln!("total {:?}", overall.elapsed());
}

#[cfg(test)]
mod tests {
    use super::{pack, split, split_scalar, unpack};

    #[test]
    fn vector_csv_matches_scalar_across_block_boundaries() {
        for start in 0..132 {
            for len in [0, 1, 7, 25, 31, 32, 33] {
                let line = format!("{} ,\"{}\",,tail", "a".repeat(start), "b".repeat(len));
                assert_eq!(split::<4>(line.as_bytes()), split_scalar::<4>(line.as_bytes()));
                for pos in [0, start + 1, start + 2 + len, line.len() - 1] {
                    let mut malformed = line.as_bytes().to_vec();
                    malformed[pos] = b'"';
                    assert_eq!(split::<4>(&malformed), split_scalar::<4>(&malformed));
                }
            }
        }
    }

    #[test]
    fn packed_strings_preserve_long_lengths() {
        for len in [0, 1, 65_534, 65_535, 65_536] {
            let value = vec![b'a'; len];
            assert_eq!(unpack(pack(&value)), value);
        }
    }

    // A deliberately simple reference for the CSV dialect in rules.md.
    fn reference(line: &[u8], count: usize) -> Option<Vec<&[u8]>> {
        let fields: Vec<_> = line.split(|&b| b == b',').collect();
        if fields.len() != count {
            return None;
        }
        fields.into_iter().map(|field| {
            if field.first() == Some(&b'"') && field.last() == Some(&b'"') && field.len() >= 2 {
                let inner = &field[1..field.len() - 1];
                (!inner.contains(&b'"')).then_some(inner)
            } else {
                (!field.contains(&b'"')).then_some(field)
            }
        }).collect()
    }

    #[test]
    fn split_matches_csv_dialect_on_short_lines() {
        const ALPHABET: &[u8] = b"a,\"\r";
        for len in 0..=7 {
            for mut code in 0..ALPHABET.len().pow(len) {
                let mut line = vec![0; len as usize];
                for byte in &mut line {
                    *byte = ALPHABET[code % ALPHABET.len()];
                    code /= ALPHABET.len();
                }
                assert_eq!(split::<2>(&line).map(|v| v.to_vec()), reference(&line, 2), "{line:?}");
                assert_eq!(split::<4>(&line).map(|v| v.to_vec()), reference(&line, 4), "{line:?}");
                assert_eq!(split::<7>(&line).map(|v| v.to_vec()), reference(&line, 7), "{line:?}");
            }
        }
    }
}

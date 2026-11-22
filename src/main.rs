// Shopping Sessions pipeline: single-binary, multi-threaded, exact to rules.md.
use bytes::Bytes;
use memchr::memchr;
use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const LATE_MS: i64 = DAY_MS;
const B1: usize = 256;
const B2: usize = 256;
const CHUNK: usize = 8 << 20;
const NO_CAT: u32 = u32::MAX;
const INVALID_EVENT: u64 = u64::MAX;
const WIDE_EVENT: u64 = 1 << 63;
const TS_MASK: u64 = (1 << 31) - 1;
const PRICE_MASK: u64 = (1 << 24) - 1;
const PTR_MASK: u64 = (1 << 48) - 1;
const PACKED_LONG: usize = (1 << 15) - 1;

// ---------------------------------------------------------------- utilities

#[inline(always)]
fn hash_bytes(b: &[u8]) -> u64 {
    const K0: u64 = 0xa076_1d64_78bd_642f;
    const K1: u64 = 0xe703_7ed1_a0b4_28db;
    #[inline(always)]
    fn mix(a: u64, b: u64) -> u64 {
        let p = (a as u128) * (b as u128);
        p as u64 ^ (p >> 64) as u64
    }
    let n = b.len();
    let (a, z) = if n >= 8 {
        let mut prefix = K0;
        if n > 16 {
            for bytes in b.chunks_exact(8).skip(1) {
                let word = u64::from_le_bytes(bytes.try_into().unwrap());
                prefix = mix(prefix ^ word, K1);
            }
        }
        (
            prefix ^ u64::from_le_bytes(b[..8].try_into().unwrap()),
            u64::from_le_bytes(b[n - 8..].try_into().unwrap()),
        )
    } else if n >= 4 {
        (
            K0 ^ u32::from_le_bytes(b[..4].try_into().unwrap()) as u64,
            u32::from_le_bytes(b[n - 4..].try_into().unwrap()) as u64,
        )
    } else if n != 0 {
        (K0 ^ b[0] as u64, ((b[n / 2] as u64) << 8) | b[n - 1] as u64)
    } else {
        (K0, 0)
    };
    let h = mix(a ^ (n as u64), z ^ K1);
    if h == 0 { 1 } else { h }
}

const EXACT_USER: u64 = 1 << 63;

// Eight or nine nonzero ASCII bytes fit exactly into one word at seven
// bits per byte. Keep the source pointer as well for lexical comparison
// and output; only the uncommon wider keys need it during hash probes.
#[inline(always)]
fn user_code(user: &[u8]) -> u64 {
    user_code_impl::<false>(user)
}

#[inline(always)]
fn user_code_impl<const BMI2: bool>(user: &[u8]) -> u64 {
    if matches!(user.len(), 8 | 9) {
        let w = u64::from_le_bytes(user[..8].try_into().unwrap());
        let last = if user.len() == 9 { user[8] } else { 0 };
        const HIGH: u64 = 0x8080_8080_8080_8080;
        if w & HIGH == 0
            && w.wrapping_sub(0x0101_0101_0101_0101) & !w & HIGH == 0
            && (user.len() == 8 || (1..128).contains(&last))
        {
            #[cfg(target_arch = "x86_64")]
            if BMI2 {
                // The only true specialization is inside the BMI2-dispatched scanner.
                let compact = unsafe { std::arch::x86_64::_pext_u64(w, 0x7f7f_7f7f_7f7f_7f7f) };
                return EXACT_USER | compact | ((last as u64) << 56);
            }
            let w = (w & 0x007f_007f_007f_007f) | ((w >> 1) & 0x3f80_3f80_3f80_3f80);
            let w = (w & 0x0000_3fff_0000_3fff) | ((w >> 2) & 0x0fff_c000_0fff_c000);
            let w = (w & 0x0000_0000_0fff_ffff) | ((w >> 4) & 0x00ff_ffff_f000_0000);
            return EXACT_USER | w | ((last as u64) << 56);
        }
    }
    let code = hash_bytes(user) & !EXACT_USER;
    if code == 0 { 1 } else { code }
}

#[inline(always)]
fn user_hash(code: u64) -> u64 {
    let h = (code ^ (code >> 29)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    h ^ (h >> 32)
}

#[inline(always)]
fn same_user(code: u64, a: u64, b: u64) -> bool {
    code & EXACT_USER != 0 || a == b || unpack(a) == unpack(b)
}

#[inline]
fn short_name(code: u64) -> ([u8; 8], u8) {
    let w = (code & 0x0000_0000_0fff_ffff) | ((code & 0x00ff_ffff_f000_0000) << 4);
    let w = (w & 0x0000_3fff_0000_3fff) | ((w & 0x0fff_c000_0fff_c000) << 2);
    let w = (w & 0x007f_007f_007f_007f) | ((w & 0x3f80_3f80_3f80_3f80) << 1);
    (w.to_le_bytes(), ((code >> 56) & 127) as u8)
}

#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    // Reserve the upper bit for compact IDs, and the largest remaining
    // length tag for a descriptor for longer strings.
    // The slices all point into the mmaps retained for the life of the process.
    if s.len() >= PACKED_LONG {
        let descriptor = Box::into_raw(Box::new((s.as_ptr(), s.len())));
        (descriptor as u64) | ((PACKED_LONG as u64) << 48)
    } else {
        (s.as_ptr() as u64) | ((s.len() as u64) << 48)
    }
}
#[inline(always)]
fn unpack(p: u64) -> &'static [u8] {
    let ptr = p & PTR_MASK;
    let len = (p >> 48) as usize;
    unsafe {
        if len == PACKED_LONG {
            let &(ptr, len) = &*(ptr as *const (*const u8, usize));
            std::slice::from_raw_parts(ptr, len)
        } else {
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }
}

// The source stores ingestion times in fixed-width, ordered UTC form.
// Only exact duplicates need to compare these bytes after parsing.
#[inline(always)]
fn event_ref(id: &[u8], ingest: Option<&[u8]>) -> u64 {
    let Some(ingest) = ingest else {
        return long_event_ref(id, b"9999-99-99T99:99:99Z");
    };
    let offset = (ingest.as_ptr() as usize).wrapping_sub(id.as_ptr() as usize);
    if id.len() < 255 && offset < 256 {
        id.as_ptr() as u64 | ((id.len() as u64) << 48) | ((offset as u64) << 56)
    } else {
        long_event_ref(id, ingest)
    }
}

#[cold]
fn long_event_ref(id: &[u8], ingest: &[u8]) -> u64 {
    let desc = Box::into_raw(Box::new((pack(id), pack(ingest))));
    desc as u64 | (255u64 << 48)
}

#[inline(always)]
fn event_fields(value: u64) -> (&'static [u8], &'static [u8]) {
    let len = (value >> 48) as u8;
    let ptr = (value & PTR_MASK) as *const u8;
    unsafe {
        if len == 255 {
            let &(id, ingest) = &*ptr.cast::<(u64, u64)>();
            (unpack(id), unpack(ingest))
        } else {
            (
                std::slice::from_raw_parts(ptr, len as usize),
                std::slice::from_raw_parts(ptr.add((value >> 56) as usize), 20),
            )
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
    // Each opening quote borrows from the next comma (or the implicit
    // delimiter beyond the row). This turns on the bits through the last
    // byte of that field, which must be its closing quote.
    let opens = quotes & ((commas << 1) | 1);
    let ends = (commas >> 1) | (1u128 << (line.len() - 1));
    let closes = commas.wrapping_sub(opens) & !commas & ends;
    if quotes != (opens | closes) || opens & closes != 0 {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0;
    for (i, field) in out.iter_mut().enumerate() {
        let end = if i + 1 == N { line.len() } else { commas.trailing_zeros() as usize };
        commas &= commas.wrapping_sub(1);
        let q = ((opens >> (start & 127)) & 1) as usize & usize::from(start != end);
        // Comma positions are sorted, and quote validation guarantees
        // that a quoted field has at least two bytes.
        *field = unsafe { line.get_unchecked(start + q..end - q) };
        start = end + 1;
    }
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

// A small worker-local cache avoids repeating calendar arithmetic. The
// full date is compared on a hit, so arbitrary dates can share a slot.
struct DateCache([([u8; 10], i64); 32]);

impl DateCache {
    fn new() -> Self {
        Self([(*b"1970-01-01", 0); 32])
    }
}

#[inline(always)]
fn ascii_digits8(v: u64) -> bool {
    const HI: u64 = 0xf0f0_f0f0_f0f0_f0f0;
    const ZERO: u64 = 0x3030_3030_3030_3030;
    (v & HI) == ZERO && (v.wrapping_add(0x0606_0606_0606_0606) & HI) == ZERO
}

#[inline(always)]
fn eight_digits(v: u64) -> u64 {
    let pairs = (v & 0x000f_000f_000f_000f) * 10 + ((v >> 8) & 0x000f_000f_000f_000f);
    let fours = (pairs & 0x0000_00ff_0000_00ff) * 100
        + ((pairs >> 16) & 0x0000_00ff_0000_00ff);
    (fours & 0xffff) * 10_000 + (fours >> 32)
}

#[inline(always)]
fn date_prefix(f: &[u8], cache: &mut DateCache) -> Option<i64> {
    let slot = ((f[8] as usize) * 10 + f[9] as usize) & 31;
    let entry = &mut cache.0[slot];
    if f[..10] == entry.0 {
        Some(entry.1)
    } else {
        cache_date(&f[..10], entry)
    }
}

#[cold]
fn cache_date(f: &[u8], entry: &mut ([u8; 10], i64)) -> Option<i64> {
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
    entry.0.copy_from_slice(f);
    entry.1 = value;
    Some(value)
}

#[inline(always)]
fn parse_iso<const UTC_ONLY: bool>(f: &[u8], cache: &mut DateCache) -> Option<i64> {
    let n = f.len();
    if n != 20 && (UTC_ONLY || n != 25) {
        return None;
    }
    let clock = u64::from_le_bytes(f[11..19].try_into().unwrap());
    const SEPARATORS: u64 = 0x0000_ff00_00ff_0000;
    if f[10] != b'T' || clock & SEPARATORS != 0x0000_3a00_003a_0000
        || !ascii_digits8((clock & !SEPARATORS) | 0x0000_3000_0030_0000)
    {
        return None;
    }
    const PAIRS: u64 = u64::from_le_bytes([15, 0, 0, 15, 0, 0, 15, 0]);
    const LIMITS: u64 = u64::from_le_bytes([128 - 24, 0, 0, 128 - 60, 0, 0, 128 - 60, 0]);
    const TOP: u64 = u64::from_le_bytes([128, 0, 0, 128, 0, 0, 128, 0]);
    let pairs = (clock & PAIRS) * 10 + ((clock >> 8) & PAIRS);
    if (pairs + LIMITS) & TOP != 0 {
        return None;
    }
    let h = pairs as u8 as i64;
    let mi = (pairs >> 24) as u8 as i64;
    let s = (pairs >> 48) as u8 as i64;
    let day_ms = date_prefix(f, cache)?;
    let off: i64 = if UTC_ONLY || n == 20 {
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
fn timely(ing: &[u8], event: i64, cache: &mut DateCache) -> bool {
    // The input contract makes ingest_ts valid F1. An ingest UTC date
    // no later than the event's date cannot exceed the 24-hour cutoff.
    if ing.len() != 20 || ing[19] != b'Z' {
        return false;
    }
    match date_prefix(ing, cache) {
        Some(day) if day <= event => true,
        Some(day) if day - event <= LATE_MS => {
            parse_iso::<true>(ing, cache).is_some_and(|i| i - event <= LATE_MS)
        }
        _ => false,
    }
}

#[inline(always)]
fn parse_ts(f: &[u8], cache: &mut DateCache) -> Option<i64> {
    if f.len() == 13 {
        let first = u64::from_le_bytes(f[..8].try_into().unwrap());
        let last = u64::from_le_bytes(f[5..].try_into().unwrap());
        return (ascii_digits8(first) && ascii_digits8(last))
            .then(|| ((eight_digits(first) / 1000) * 100_000_000 + eight_digits(last)) as i64);
    }
    if matches!(f.len(), 20 | 25) && f[10] == b'T' {
        return parse_iso::<false>(f, cache);
    }
    parse_epoch(f)
}

#[inline(always)]
fn parse_row_timestamp<const VECTOR: bool>(f: &[u8], cache: &mut DateCache) -> Option<i64> {
    #[cfg(target_arch = "x86_64")]
    if VECTOR && f.len() == 13 {
        use std::arch::x86_64::*;
        // Overlapping reads cover every byte without crossing the field end.
        let first = u64::from_le_bytes(f[..8].try_into().unwrap());
        let last = u64::from_le_bytes(f[5..].try_into().unwrap());
        return unsafe {
            let text = _mm_set_epi64x(last as i64, first as i64);
            let digits = _mm_sub_epi8(text, _mm_set1_epi8(b'0' as i8));
            let excess = _mm_subs_epu8(digits, _mm_set1_epi8(9));
            if _mm_testz_si128(excess, excess) == 0 {
                None
            } else {
                let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
                let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0064));
                let high = i64::from(_mm_cvtsi128_si32(quads)) * 10 + i64::from(f[4] - b'0');
                let low = _mm_cvtsi128_si64(_mm_srli_si128::<8>(quads)) as u64;
                Some(high * 100_000_000 + i64::from(low as u32) * 10_000 + (low >> 32) as i64)
            }
        };
    }
    parse_ts(f, cache)
}

#[cold]
fn parse_epoch(mut f: &[u8]) -> Option<i64> {
    if f.is_empty() {
        return None;
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
    let mut v: u32 = 0;
    for &c in f {
        let digit = c.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        v = v * 10 + digit as u32;
        if v > 10_000_000 { return None; }
    }
    if v == 0 {
        None
    } else {
        Some(v)
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
fn for_lines<'a>(data: &'a [u8], first: bool, mut f: impl FnMut(&'a [u8])) {
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
#[target_feature(enable = "avx512bw,bmi2,popcnt")]
unsafe fn parse_events_fast(data: &[u8], first: bool, cx: &Ctx, st: &mut P1, links: Option<&LinkTable>) {
    scan_rows_avx512::<7>(data, first, |fields| {
        if let Some(row) = parse_event_fields::<true>(fields, cx, &mut st.dates) {
            st.route_event(row, links);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,bmi2,popcnt")]
unsafe fn parse_clicks_fast(data: &[u8], first: bool, cx: &Ctx, st: &mut P1, links: Option<&LinkTable>) {
    scan_rows_avx512::<4>(data, first, |fields| {
        if let Some(row) = parse_click_fields::<true>(fields, cx, &mut st.dates) {
            st.route_click(row, links);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn scan_rows_avx512<const N: usize>(data: &[u8], first: bool, mut visit: impl FnMut([&[u8]; N])) {
    use std::arch::x86_64::*;
    let mut pos = if first { memchr(b'\n', data).map_or(data.len(), |i| i + 1) } else { 0 };
    let comma = _mm512_set1_epi8(b',' as i8);
    let quote = _mm512_set1_epi8(b'"' as i8);
    let newline = _mm512_set1_epi8(b'\n' as i8);
    while pos + 128 <= data.len() {
        let p = data.as_ptr().add(pos);
        let a = _mm512_loadu_si512(p.cast());
        let nl = _mm512_cmpeq_epi8_mask(a, newline);
        let c = _mm512_cmpeq_epi8_mask(a, comma);
        let q = _mm512_cmpeq_epi8_mask(a, quote);
        let (len, commas, quotes) = if nl != 0 {
            let cut = nl.trailing_zeros();
            (cut as usize, u128::from(_bzhi_u64(c, cut)), u128::from(_bzhi_u64(q, cut)))
        } else {
            let b = _mm512_loadu_si512(p.add(64).cast());
            let nl = _mm512_cmpeq_epi8_mask(b, newline);
            if nl == 0 {
                let end = memchr(b'\n', &data[pos + 128..]).map_or(data.len(), |i| pos + 128 + i);
                if let Some(fields) = split_scalar::<N>(&data[pos..end]) {
                    visit(fields);
                }
                pos = (end + 1).min(data.len());
                continue;
            }
            let cut = nl.trailing_zeros();
            let c1 = _bzhi_u64(_mm512_cmpeq_epi8_mask(b, comma), cut);
            let q1 = _bzhi_u64(_mm512_cmpeq_epi8_mask(b, quote), cut);
            (64 + cut as usize, u128::from(c) | (u128::from(c1) << 64), u128::from(q) | (u128::from(q1) << 64))
        };
        if let Some(fields) = split_masks_bmi2::<N>(&data[pos..pos + len], commas, quotes) {
            visit(fields);
        }
        pos += len + 1;
    }
    for_lines(&data[pos..], false, |line| {
        if let Some(fields) = split_scalar::<N>(line) { visit(fields); }
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi2,popcnt")]
unsafe fn parse_events_avx2(data: &[u8], first: bool, cx: &Ctx, st: &mut P1, links: Option<&LinkTable>) {
    scan_rows_avx2::<7>(data, first, |fields| {
        if let Some(row) = parse_event_fields::<true>(fields, cx, &mut st.dates) {
            st.route_event(row, links);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi2,popcnt")]
unsafe fn parse_clicks_avx2(data: &[u8], first: bool, cx: &Ctx, st: &mut P1, links: Option<&LinkTable>) {
    scan_rows_avx2::<4>(data, first, |fields| {
        if let Some(row) = parse_click_fields::<true>(fields, cx, &mut st.dates) {
            st.route_click(row, links);
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn scan_rows_avx2<const N: usize>(data: &[u8], first: bool, mut visit: impl FnMut([&[u8]; N])) {
    use std::arch::x86_64::*;
    let mut pos = if first { memchr(b'\n', data).map_or(data.len(), |i| i + 1) } else { 0 };
    let comma = _mm256_set1_epi8(b',' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let newline = _mm256_set1_epi8(b'\n' as i8);
    // Pair two YMM masks into one word. Most rows need either one or
    // two words; only the tail and very long rows use the scalar splitter.
    macro_rules! mask64 {
        ($a:expr, $b:expr, $ch:expr) => {
            u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8($a, $ch)) as u32)
                | (u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8($b, $ch)) as u32) << 32)
        };
    }
    while pos + 128 <= data.len() {
        let p = data.as_ptr().add(pos);
        let a = _mm256_loadu_si256(p.cast());
        let b = _mm256_loadu_si256(p.add(32).cast());
        let nl = mask64!(a, b, newline);
        let c = mask64!(a, b, comma);
        let q = mask64!(a, b, quote);
        let (len, commas, quotes) = if nl != 0 {
            let cut = nl.trailing_zeros();
            (cut as usize, u128::from(_bzhi_u64(c, cut)), u128::from(_bzhi_u64(q, cut)))
        } else {
            let a = _mm256_loadu_si256(p.add(64).cast());
            let b = _mm256_loadu_si256(p.add(96).cast());
            let nl = mask64!(a, b, newline);
            if nl == 0 {
                let end = memchr(b'\n', &data[pos + 128..]).map_or(data.len(), |i| pos + 128 + i);
                if let Some(fields) = split_scalar::<N>(&data[pos..end]) {
                    visit(fields);
                }
                pos = (end + 1).min(data.len());
                continue;
            }
            let cut = nl.trailing_zeros();
            let c1 = _bzhi_u64(mask64!(a, b, comma), cut);
            let q1 = _bzhi_u64(mask64!(a, b, quote), cut);
            (64 + cut as usize, u128::from(c) | (u128::from(c1) << 64), u128::from(q) | (u128::from(q1) << 64))
        };
        if let Some(fields) = split_masks_bmi2::<N>(&data[pos..pos + len], commas, quotes) {
            visit(fields);
        }
        pos += len + 1;
    }
    for_lines(&data[pos..], false, |line| {
        if let Some(fields) = split_scalar::<N>(line) { visit(fields); }
    });
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn split_masks_bmi2<const N: usize>(line: &[u8], mut commas: u128, quotes: u128) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::_pext_u64;
    if commas.count_ones() as usize != N - 1 { return None; }
    // The scanners remove all bits past the line ending. These callers
    // have at least two fields, so the comma count also proves len > 0.
    let starts = (commas << 1) | 1;
    let opens = quotes & starts;
    let ends = (commas >> 1) | (1u128 << (line.len() - 1));
    let closes = commas.wrapping_sub(opens) & !commas & ends;
    if quotes != (opens | closes) || opens & closes != 0 { return None; }
    let lo = starts as u64;
    let mut quoted = _pext_u64(quotes as u64, lo)
        | (_pext_u64((quotes >> 64) as u64, (starts >> 64) as u64) << lo.count_ones());
    let mut out = [&[][..]; N];
    let mut start = 0;
    for (i, field) in out.iter_mut().enumerate() {
        let end = if i + 1 == N { line.len() } else { commas.trailing_zeros() as usize };
        commas &= commas.wrapping_sub(1);
        let q = (quoted & 1) as usize;
        quoted >>= 1;
        // Distinct opening and closing quotes guarantee room to trim.
        *field = line.get_unchecked(start + q..end - q);
        start = end + 1;
    }
    Some(out)
}

// ---------------------------------------------------------------- context tables

struct SmallMap {
    keys: Vec<u64>,
    ptrs: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}
impl SmallMap {
    fn new(n: usize) -> Self {
        let cap = (n * 2 + 2).next_power_of_two().max(16);
        SmallMap { keys: vec![0; cap], ptrs: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    fn insert(&mut self, raw: &'static [u8], v: u32) {
        let h = hash_bytes(raw);
        let mut i = (h as usize) & self.mask;
        loop {
            if self.keys[i] == 0 || (self.keys[i] == h && unpack(self.ptrs[i]) == raw) {
                self.keys[i] = h;
                self.ptrs[i] = pack(raw);
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, raw: &[u8]) -> Option<u32> {
        let h = hash_bytes(raw);
        let mut i = (h as usize) & self.mask;
        loop {
            let k = unsafe { *self.keys.get_unchecked(i) };
            if k == h && unpack(unsafe { *self.ptrs.get_unchecked(i) }) == raw {
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
    compact_events: bool,
    small_counts: bool,
    click_lo: i64,
    day0: i64,
    ndays: usize,
    prod: SmallMap, // product hash -> category rank
    unknown_rank: u32,
    camp: SmallMap, // campaign hash -> idx
    camp_ids: Vec<&'static [u8]>,
    camp_ch: Vec<&'static [u8]>,
    cat_names: Vec<&'static [u8]>, // by rank
}

fn read_table(path: &str) -> Vec<[&'static [u8]; 2]> {
    let d = load_file(path);
    let mut rows = vec![];
    for_lines(d, true, |l| {
        if let Some(f) = split::<2>(l) {
            rows.push(f);
        }
    });
    rows
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C)]
struct Raw {
    eid: u64,
    source: u64,
    uh: u64,
    uptr: u64,
    data: u64,
}

struct Ev {
    key: u64,
    data: AtomicU64,
}

struct WideEv {
    key: u64,
    ptr: u64,
    data: AtomicU64,
}

struct RoutedEvent<'a> {
    key: u64,
    ptr: u64,
    data: &'a AtomicU64,
}
#[derive(Clone, Copy)]
struct Ck {
    key: u64,
    timed: TimedClick,
    ptr: u64,
}

#[derive(Clone, Copy, Default, PartialOrd, Ord, PartialEq, Eq)]
struct TimedEvent(u64);

impl TimedEvent {
    #[inline(always)]
    fn new(ts: i64, price: u32, cat: u32, ws: i64) -> Self {
        let delta = (ts - ws) as u64;
        if delta <= TS_MASK && cat <= u8::MAX as u32 {
            // Timestamp first so compact histories can sort plain words.
            Self((delta << 32) | (u64::from(cat) << 24) | u64::from(price))
        } else {
            let wide = Box::into_raw(Box::new((ts, price, cat))) as u64;
            Self(WIDE_EVENT | wide)
        }
    }

    #[inline(always)]
    fn parts(self, ws: i64) -> (i64, u32, u32) {
        if self.0 & WIDE_EVENT == 0 {
            (ws + (self.0 >> 32) as i64, (self.0 & PRICE_MASK) as u32, (self.0 >> 24) as u8 as u32)
        } else {
            // Only `new` constructs wide values, and they remain valid
            // for the lifetime of the process, like the CSV mappings.
            unsafe { *((self.0 & !WIDE_EVENT) as *const (i64, u32, u32)) }
        }
    }
}

#[derive(Clone, Copy, Default)]
struct TimedClick {
    data: u64,
    cid: u64,
}

impl TimedClick {
    #[inline(always)]
    fn new(ts: i64, camp: u32, cid: u64, base: i64) -> Self {
        let dt = (ts - base) as u64;
        let data = if dt <= TS_MASK {
            (dt << 32) | u64::from(camp)
        } else {
            WIDE_EVENT | Box::into_raw(Box::new((ts, camp))) as u64
        };
        Self { data, cid }
    }

    #[inline(always)]
    fn parts(self, base: i64) -> (i64, u32) {
        if self.data & WIDE_EVENT == 0 {
            (base + (self.data >> 32) as i64, self.data as u32)
        } else {
            unsafe { *((self.data & !WIDE_EVENT) as *const (i64, u32)) }
        }
    }

    #[inline(always)]
    fn ts(self, base: i64) -> i64 { self.parts(base).0 }

    #[inline(always)]
    fn camp(self) -> usize { self.parts(0).1 as usize }
}

#[inline(always)]
fn parse_event(line: &[u8], cx: &Ctx, dates: &mut DateCache) -> Option<Raw> {
    parse_event_fields::<false>(split::<7>(line)?, cx, dates)
}

#[inline(always)]
fn parse_event_fields<const BMI2: bool>(f: [&[u8]; 7], cx: &Ctx, dates: &mut DateCache) -> Option<Raw> {
    let [eid, user, ts, ing, ty, prod, price] = f;
    if eid.is_empty() {
        return None;
    }
    let ingest = (ing.len() == 20 && ing[19] == b'Z').then_some(ing);
    let mut r = Raw {
        eid: hash_bytes(eid),
        source: event_ref(eid, ingest),
        uh: 0, uptr: 0, data: INVALID_EVENT,
    };
    if user.is_empty() || prod.is_empty() {
        return Some(r);
    }
    let purchase = match ty {
        b"view" | b"cart" => false,
        b"purchase" => true,
        _ => return Some(r),
    };
    let utc = ts.len() == 20 && ts[19] == b'Z';
    let t = match if utc { parse_iso::<true>(ts, dates) } else { parse_row_timestamp::<BMI2>(ts, dates) } {
        Some(t) if t >= cx.ws && t < cx.we => t,
        _ => return Some(r),
    };
    let Some(ingest) = ingest else { return Some(r) };
    // A validated UTC timestamp and a valid F1 ingest on the same
    // calendar date are necessarily less than 24 hours apart.
    if !(utc && ts[..10] == ingest[..10]) && !timely(ingest, t, dates) {
        return Some(r);
    }
    let (price, cat) = if purchase {
        let price = match parse_price(price) {
            Some(p) => p,
            None => return Some(r),
        };
        (price, cx.prod.get(prod).unwrap_or(cx.unknown_rank))
    } else {
        (0, 0)
    };
    r.data = TimedEvent::new(t, price, cat, cx.ws).0;
    r.uh = user_code_impl::<BMI2>(user);
    if r.uh & EXACT_USER == 0 {
        r.uptr = pack(user);
    }
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
    tags: Vec<u64>,
    slots: Vec<[u64; 2]>,
    wide: Vec<Slot>,
    mask: usize,
}

#[inline(always)]
fn zero_bytes(word: u64) -> u64 {
    const LOW: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    !(((word & LOW) + LOW) | word | LOW)
}

#[inline(always)]
fn link_tag(hash: u64) -> u64 {
    ((hash >> 40) as u8).max(1) as u64
}

impl LinkTable {
    fn alias_table(aliases: usize) -> Self {
        let cap = (aliases * 4 / 3 + 1).next_power_of_two().max(8);
        Self { tags: vec![0; cap / 8], slots: vec![[0; 2]; cap], wide: Vec::new(), mask: cap / 8 - 1 }
    }

    fn insert(&mut self, key: u64, kptr: u64, pk: u64, pptr: u64) {
        let hash = user_hash(key);
        let mut i = hash as usize & self.mask;
        while zero_bytes(self.tags[i]) == 0 {
            i = (i + 1) & self.mask;
        }
        let bit = zero_bytes(self.tags[i]).trailing_zeros();
        self.tags[i] |= link_tag(hash) << (bit & !7);
        // Only keys whose full bytes fit in the compact encoding may
        // omit source pointers. All other alias pairs retain both IDs.
        let value = if key & pk & EXACT_USER != 0 {
            pk
        } else {
            self.wide.push(Slot { key, kptr, pk, pptr });
            self.wide.len() as u64
        };
        self.slots[i * 8 + (bit / 8) as usize] = [key, value];
    }

    #[inline(always)]
    fn get(&self, code: u64, hash: u64, ptr: u64) -> Option<(u64, u64)> {
        let mut i = (hash as usize) & self.mask;
        let lookup = link_tag(hash) * 0x0101_0101_0101_0101;
        loop {
            let control = unsafe { *self.tags.get_unchecked(i) };
            let mut matches = zero_bytes(control ^ lookup);
            while matches != 0 {
                let &[key, value] = unsafe { self.slots.get_unchecked(i * 8 + (matches.trailing_zeros() / 8) as usize) };
                if key == code {
                    if value & EXACT_USER != 0 {
                        return Some((value, 0));
                    }
                    let entry = unsafe { self.wide.get_unchecked(value as usize - 1) };
                    if same_user(code, entry.kptr, ptr) {
                        return Some((entry.pk, entry.pptr));
                    }
                }
                matches &= matches - 1;
            }
            if zero_bytes(control) != 0 {
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

#[derive(Clone, Copy)]
struct LinkNode {
    code: u64,
    ptr: u64,
}

fn build_links(files: &[&'static [u8]]) -> LinkTable {
    let mut pairs: Vec<(u64, u64, u64, u64)> = vec![];
    for d in files {
        for_lines(d, true, |line| {
            if let Some([a, b]) = split::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    pairs.push((user_code(a), pack(a), user_code(b), pack(b)));
                }
            }
        });
    }
    if pairs.is_empty() {
        return LinkTable::alias_table(0);
    }
    let cap = (pairs.len() * 4 + 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut slots = vec![0u32; cap];
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    let mut nodes: Vec<LinkNode> = Vec::with_capacity(pairs.len() * 2);
    for &(ha, pa, hb, pb) in &pairs {
        let mut ids = [0u32; 2];
        for (k, (h, p)) in [(ha, pa), (hb, pb)].into_iter().enumerate() {
            let mut i = (user_hash(h) as usize) & mask;
            loop {
                let slot = slots[i];
                if slot == 0 {
                    let n = parent.len() as u32;
                    slots[i] = n + 1;
                    parent.push(n);
                    nodes.push(LinkNode { code: h, ptr: p });
                    ids[k] = n;
                    break;
                }
                let node = nodes[(slot - 1) as usize];
                if node.code == h && same_user(h, node.ptr, p) {
                    ids[k] = slot - 1;
                    break;
                }
                i = (i + 1) & mask;
            }
        }
        let ra = find(&mut parent, ids[0]);
        let rb = find(&mut parent, ids[1]);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(pairs);
    drop(slots);
    let mut best = vec![u32::MAX; parent.len()];
    let mut components = 0;
    for (i, node) in nodes.iter().enumerate() {
        let r = find(&mut parent, i as u32) as usize;
        parent[i] = r as u32;
        let b = best[r];
        components += usize::from(b == u32::MAX);
        if b == u32::MAX || unpack(node.ptr) < unpack(nodes[b as usize].ptr) {
            best[r] = i as u32;
        }
    }
    // Only aliases need a lookup entry. A canonical ID already maps to
    // itself, and omitting it keeps the table smaller for the hot probes.
    let mut out = LinkTable::alias_table(nodes.len() - components);
    for (i, node) in nodes.iter().enumerate() {
        let root = parent[i] as usize;
        if best[root] as usize != i {
            let canonical = nodes[best[root] as usize];
            out.insert(node.code, node.ptr, canonical.code, canonical.ptr);
        }
    }
    out
}

// ---------------------------------------------------------------- phase states

struct P1 {
    dates: DateCache,
    pending_events: Vec<Raw>,
    pending_clicks: Vec<Ck>,
    people: Vec<[u64; 2]>,
    wide_people: Vec<Slot>,
    ids: Vec<Vec<EventIndex>>,
    overflow_destinations: Vec<u64>,
    ev: Vec<Vec<Ev>>,
    wide: Vec<Vec<WideEv>>,
    ck: Vec<Vec<Ck>>,
}

#[derive(Clone, Copy)]
struct EventIndex {
    source: u64,
    // The shard uses the top eight hash bits. Within that shard the
    // low 32 bits are only a probe filter, never proof of ID equality.
    eid: u32,
    destination: u32,
}

const DEST_INDEX_BITS: u32 = 22;
const DEST_WIDE: u32 = 1 << 30;
const DEST_OVERFLOW: u32 = 1 << 31;

impl P1 {
    fn new(event_hint: usize, click_hint: usize) -> Self {
        Self {
            dates: DateCache::new(),
            pending_events: Vec::new(),
            pending_clicks: Vec::new(),
            people: vec![[0; 2]; PEOPLE_CACHE],
            wide_people: vec![Slot::default(); WIDE_PEOPLE_CACHE],
            ids: (0..B1).map(|_| Vec::with_capacity(event_hint / B1)).collect(),
            overflow_destinations: Vec::new(),
            ev: (0..B2).map(|_| Vec::with_capacity(event_hint / B2)).collect(),
            wide: (0..B2).map(|_| Vec::with_capacity(event_hint / (B2 * 8))).collect(),
            ck: (0..B2).map(|_| Vec::with_capacity(click_hint / B2)).collect(),
        }
    }

    #[inline(always)]
    fn person(&mut self, code: u64, ptr: u64, links: &LinkTable) -> (usize, u64, u64) {
        let hash = user_hash(code);
        if code & EXACT_USER == 0 {
            return self.wide_person(code, ptr, hash, links);
        }
        let entry = unsafe { self.people.get_unchecked_mut((hash as usize) & (PEOPLE_CACHE - 1)) };
        let (canonical, ptr) = if entry[0] == code {
            (entry[1], 0)
        } else {
            let (pk, pptr) = links.get(code, hash, ptr).unwrap_or((code, 0));
            if pk & EXACT_USER != 0 {
                *entry = [code, pk];
            }
            (pk, pptr)
        };
        let p_hash = if canonical == code { hash } else { user_hash(canonical) };
        ((p_hash >> 56) as usize, canonical, ptr)
    }

    #[cold]
    fn wide_person(&mut self, code: u64, ptr: u64, hash: u64, links: &LinkTable) -> (usize, u64, u64) {
        let i = (hash as usize) & (WIDE_PEOPLE_CACHE - 1);
        let entry = &mut self.wide_people[i];
        if entry.key != code || !same_user(code, entry.kptr, ptr) {
            let (pk, pptr) = links.get(code, hash, ptr).unwrap_or((code, ptr));
            *entry = Slot { key: code, kptr: ptr, pk, pptr };
        }
        let p_hash = if entry.pk == code { hash } else { user_hash(entry.pk) };
        ((p_hash >> 56) as usize, entry.pk, entry.pptr)
    }

    #[inline(always)]
    fn destination(&mut self, bucket: usize, index: usize, wide: bool) -> u32 {
        if index < 1 << DEST_INDEX_BITS {
            ((bucket as u32) << DEST_INDEX_BITS) | index as u32 | if wide { DEST_WIDE } else { 0 }
        } else {
            let tag = u32::try_from(self.overflow_destinations.len()).expect("too many overflow destinations");
            assert!(tag < DEST_OVERFLOW - 1);
            self.overflow_destinations.push(((bucket as u64) << 32) | index as u64 | if wide { EXACT_USER } else { 0 });
            DEST_OVERFLOW | tag
        }
    }

    #[inline(always)]
    fn route_event(&mut self, row: Raw, links: Option<&LinkTable>) {
        let destination = if row.data == INVALID_EVENT {
            u32::MAX
        } else if let Some(links) = links {
            let (bucket, key, ptr) = self.person(row.uh, row.uptr, links);
            let data = AtomicU64::new(row.data);
            if key & EXACT_USER != 0 {
                let ev = unsafe { self.ev.get_unchecked_mut(bucket) };
                let index = ev.len();
                ev.push(Ev { key, data });
                self.destination(bucket, index, false)
            } else {
                let ev = unsafe { self.wide.get_unchecked_mut(bucket) };
                let index = ev.len();
                ev.push(WideEv { key, ptr, data });
                self.destination(bucket, index, true)
            }
        } else {
            self.pending_events.push(row);
            return;
        };
        let b = (row.eid >> 56) as usize;
        unsafe { self.ids.get_unchecked_mut(b) }.push(EventIndex { eid: row.eid as u32, source: row.source, destination });
    }

    #[inline(always)]
    fn route_click(&mut self, mut click: Ck, links: Option<&LinkTable>) {
        if let Some(links) = links {
            let (bucket, key, ptr) = self.person(click.key, click.ptr, links);
            click.key = key;
            click.ptr = ptr;
            unsafe { self.ck.get_unchecked_mut(bucket) }.push(click);
        } else {
            self.pending_clicks.push(click);
        }
    }

    fn route_pending(&mut self, links: &LinkTable) {
        for r in std::mem::take(&mut self.pending_events) {
            self.route_event(r, Some(links));
        }
        for c in std::mem::take(&mut self.pending_clicks) {
            self.route_click(c, Some(links));
        }
    }

    #[inline]
    fn retire(&self, destination: u32) {
        if destination != u32::MAX {
            let (bucket, row, wide) = if destination & DEST_OVERFLOW == 0 {
                (((destination & !DEST_WIDE) >> DEST_INDEX_BITS) as usize,
                 (destination & ((1 << DEST_INDEX_BITS) - 1)) as usize, destination & DEST_WIDE != 0)
            } else {
                let dest = self.overflow_destinations[(destination & !DEST_OVERFLOW) as usize];
                (((dest & !EXACT_USER) >> 32) as usize, dest as u32 as usize, dest & EXACT_USER != 0)
            };
            let field = if wide {
                &self.wide[bucket][row].data
            } else {
                &self.ev[bucket][row].data
            };
            field.store(INVALID_EVENT, Ordering::Relaxed);
        }
    }

    fn count(&self, bucket: usize) -> usize {
        self.ev[bucket].len() + self.wide[bucket].len()
    }

    #[inline]
    fn live_events(&self, bucket: usize) -> impl Iterator<Item = RoutedEvent<'_>> {
        let short = self.ev[bucket].iter().map(|e| RoutedEvent { key: e.key, ptr: 0, data: &e.data });
        let wide = self.wide[bucket].iter().map(|e| RoutedEvent { key: e.key, ptr: e.ptr, data: &e.data });
        short.chain(wide).filter(|r| r.data.load(Ordering::Relaxed) != INVALID_EVENT)
    }
}

fn parse_click(line: &[u8], cx: &Ctx, dates: &mut DateCache) -> Option<Ck> {
    parse_click_fields::<false>(split::<4>(line)?, cx, dates)
}

#[inline(always)]
fn parse_click_fields<const BMI2: bool>(f: [&[u8]; 4], cx: &Ctx, dates: &mut DateCache) -> Option<Ck> {
    let [cid, user, camp, ts] = f;
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = match parse_row_timestamp::<BMI2>(ts, dates) {
        Some(t) if t >= cx.click_lo && t < cx.we => t,
        _ => return None,
    };
    let ci = cx.camp.get(camp)?;
    let key = user_code_impl::<BMI2>(user);
    let ptr = if key & EXACT_USER == 0 { pack(user) } else { 0 };
    Some(Ck { key, timed: TimedClick::new(t, ci, pack(cid), cx.click_lo), ptr })
}

const PEOPLE_CACHE: usize = 16384;
const WIDE_PEOPLE_CACHE: usize = 1024;

fn dedup_bucket(b: usize, p1: &[P1], tab: &mut Vec<u32>) {
    let n: usize = p1.iter().map(|w| w.ids[b].len()).sum();
    if n == 0 {
        return;
    }
    let cap = (2 * n).next_power_of_two().max(16);
    let mask = cap - 1;
    tab.clear();
    tab.resize(cap, 0);

    // Encode producer and row in the slot, avoiding a copy of the full
    // bucket. The remaining bits serve as a hash tag to skip cold loads.
    let largest = p1.iter().map(|w| w.ids[b].len()).max().unwrap();
    let stride = largest.next_power_of_two();
    let row_bits = stride.trailing_zeros();
    let address_count = u32::try_from(stride * p1.len()).expect("dedup bucket too large");
    let index_mask = u32::MAX >> address_count.leading_zeros();
    for (producer, w) in p1.iter().enumerate() {
        let recs = &w.ids[b];
        let base = producer * stride;
        for (i, r) in recs.iter().enumerate() {
            #[cfg(target_arch = "x86_64")]
            if i + 16 < recs.len() {
                let next = (recs[i + 16].eid as usize) & mask;
                unsafe { std::arch::x86_64::_mm_prefetch(tab.as_ptr().add(next).cast(), std::arch::x86_64::_MM_HINT_T0); }
            }
            let e = r.eid;
            let tag = e & !index_mask;
            let slot_value = tag | ((base + i) as u32 + 1);
            let mut s = (e as usize) & mask;
            loop {
                let j = tab[s];
                if j == 0 {
                    tab[s] = slot_value;
                    break;
                }
                if j & !index_mask == tag {
                    let other = ((j & index_mask) - 1) as usize;
                    let older_worker = &p1[other >> row_bits];
                    let older = &older_worker.ids[b][other & (stride - 1)];
                    if older.eid == e {
                        let (other_id, other_ingest) = event_fields(older.source);
                        let (id, ingest) = event_fields(r.source);
                        if id != other_id {
                            s = (s + 1) & mask;
                            continue;
                        }
                        if ingest < other_ingest {
                            older_worker.retire(older.destination);
                            tab[s] = slot_value;
                        } else {
                            w.retire(r.destination);
                        }
                        break;
                    }
                }
                s = (s + 1) & mask;
            }
        }
    }
}

// ---------------------------------------------------------------- phase 3: sessions

#[derive(Debug, PartialEq, Eq)]
struct Sess {
    start: i64,
    rev: i64,
    purch: bool,
    run: u32,
}
#[derive(Debug, PartialEq, Eq)]
struct Run {
    day: i64,
    first: u32,
    end: u32,
    events: i64,
    rev: i64,
    top: u32,
}
struct UserRow {
    ptr: u64,
    day: i32,
    sessions: i64,
    events: i64,
    rev: i64,
    top: u32,
}

#[derive(Clone, Copy, Default)]
struct TimeBin {
    first: i64,
    last: i64,
    events: u32,
    rev: i64,
    run: u32,
}

#[derive(Default)]
struct Binned {
    bins: Vec<TimeBin>,
    active: Vec<u64>,
    purchases: Vec<(usize, u32, u32)>,
    cat_totals: Vec<i64>,
}

type PqWriter = SerializedFileWriter<BufWriter<File>>;

struct W3 {
    id: usize,
    rows: Vec<UserRow>,
    acc: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
    writer: Option<PqWriter>,
    nfiles: usize,
    sess: Vec<Sess>,
    runs: Vec<Run>,
    daycats: Vec<(u32, i64)>,
    assists: AssistSweep,
    groups: Vec<Group>,
    index: Vec<u32>,
    ev_ids: Vec<u32>,
    ck_ids: Vec<u32>,
    ordered_ev: Vec<TimedEvent>,
    ordered_ck: Vec<TimedClick>,
    binned: Binned,
    seen: Vec<u32>,
    stamp: u32,
}

const USER_SCHEMA: &str = "message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (STRING); }";
const USER_SCHEMA_SMALL: &str = "message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); REQUIRED INT32 sessions; REQUIRED INT32 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (STRING); }";
const CAMP_SCHEMA: &str = "message campaign_daily { REQUIRED BINARY campaign_id (STRING); REQUIRED BINARY channel (STRING); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }";

#[inline]
fn parquet_string(value: &'static [u8]) -> ByteArray {
    ByteArray::from(Bytes::from_static(value))
}

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
        let schema = if cx.small_counts { USER_SCHEMA_SMALL } else { USER_SCHEMA };
        st.writer = Some(new_writer(&format!("{}/user_daily/part-{}.parquet", out_dir, st.id), schema));
        st.nfiles += 1;
    }
    let w = st.writer.as_mut().unwrap();
    let mut rg = w.next_row_group().unwrap();
    {
        let mut c = rg.next_column().unwrap().unwrap();
        let mut names = Vec::with_capacity(st.rows.len() * 9);
        for row in &st.rows {
            if row.ptr & EXACT_USER != 0 {
                let (first, last) = short_name(row.ptr);
                names.extend_from_slice(&first);
                if last != 0 {
                    names.push(last);
                }
            }
        }
        let names = Bytes::from(names);
        let mut start = 0;
        let v: Vec<ByteArray> = st.rows.iter().map(|r| {
            if r.ptr & EXACT_USER == 0 {
                parquet_string(unpack(r.ptr))
            } else {
                let end = start + 8 + usize::from((r.ptr >> 56) & 127 != 0);
                let value = ByteArray::from(names.slice(start..end));
                start = end;
                value
            }
        }).collect();
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
        let value = |r: &UserRow| match sel {
            0 => r.sessions,
            1 => r.events,
            _ => r.rev,
        };
        if cx.small_counts && sel < 2 {
            let v: Vec<i32> = st.rows.iter().map(|r| value(r) as i32).collect();
            c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
        } else {
            let v: Vec<i64> = st.rows.iter().map(value).collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
        }
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
                vals.push(parquet_string(cx.cat_names[r.top as usize]));
            }
        }
        c.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).unwrap();
        c.close().unwrap();
    }
    rg.close().unwrap();
    st.rows.clear();
}

fn sorted_sessions(
    evs: &[TimedEvent],
    ws: i64,
    sess: &mut Vec<Sess>,
    runs: &mut Vec<Run>,
    daycats: &mut Vec<(u32, i64)>,
) {
    sess.clear();
    runs.clear();
    daycats.clear();
    let mut prev = i64::MIN;
    for &e in evs {
        let (ts, price, cat) = e.parts(ws);
        if sess.is_empty() || ts - prev > GAP_MS {
            let day = floor_div(ts, DAY_MS);
            let new_day = match runs.last() {
                Some(r) => r.day != day,
                None => true,
            };
            if new_day {
                if let Some(r) = runs.last_mut() {
                    r.top = top_cat(daycats);
                }
                daycats.clear();
                runs.push(Run { day, first: sess.len() as u32, end: 0, events: 0, rev: 0, top: NO_CAT });
            }
            let run = (runs.len() - 1) as u32;
            sess.push(Sess { start: ts, rev: 0, purch: false, run });
        }
        prev = ts;
        let run = runs.last_mut().unwrap();
        run.events += 1;
        run.end = sess.len() as u32;
        let price = i64::from(price);
        if price != 0 {
            run.rev += price;
            let s = sess.last_mut().unwrap();
            s.rev += price;
            s.purch = true;
            match daycats.iter_mut().find(|x| x.0 == cat) {
                Some(x) => x.1 += price,
                None => daycats.push((cat, price)),
            }
        }
    }
    if let Some(r) = runs.last_mut() {
        r.top = top_cat(daycats);
    }
}

impl Binned {
    fn load_sessions(
        &mut self, evs: &[TimedEvent], ws: i64, bins: usize, categories: usize,
        sess: &mut Vec<Sess>, runs: &mut Vec<Run>,
    ) {
        self.bins.resize(bins, TimeBin::default());
        self.active.clear();
        self.active.resize(bins.div_ceil(64), 0);
        self.purchases.clear();
        sess.clear();
        runs.clear();
        for &e in evs {
            let (ts, price, category) = e.parts(ws);
            let i = ((ts - ws) / GAP_MS) as usize;
            let b = &mut self.bins[i];
            let word = &mut self.active[i >> 6];
            let bit = 1u64 << (i & 63);
            if *word & bit == 0 {
                *word |= bit;
                *b = TimeBin { first: ts, last: ts, events: 0, rev: 0, run: 0 };
            } else {
                b.first = b.first.min(ts);
                b.last = b.last.max(ts);
            }
            b.events += 1;
            if price != 0 {
                b.rev += i64::from(price);
                self.purchases.push((i, category, price));
            }
        }

        // No session can split inside a half-open 30-minute bin. Only
        // the gap from one nonempty bin's last event to the next bin's
        // first event can create a break, at the exact same cutoff.
        let mut prev = 0;
        for (block, &active) in self.active.iter().enumerate() {
            let mut active = active;
            while active != 0 {
                let i = (block << 6) | active.trailing_zeros() as usize;
                active &= active - 1;
                let b = &mut self.bins[i];
                if sess.is_empty() || b.first - prev > GAP_MS {
                    let day = floor_div(b.first, DAY_MS);
                    if runs.last().is_none_or(|r| r.day != day) {
                        runs.push(Run { day, first: sess.len() as u32, end: 0, events: 0, rev: 0, top: NO_CAT });
                    }
                    sess.push(Sess { start: b.first, rev: 0, purch: false, run: (runs.len() - 1) as u32 });
                }
                b.run = (runs.len() - 1) as u32;
                prev = b.last;
                let r = runs.last_mut().unwrap();
                r.events += i64::from(b.events);
                r.rev += b.rev;
                r.end = sess.len() as u32;
                if b.rev != 0 {
                    let s = sess.last_mut().unwrap();
                    s.rev += b.rev;
                    s.purch = true;
                }
            }
        }
        self.cat_totals.clear();
        self.cat_totals.resize(runs.len() * categories, 0);
        for &(bin, category, price) in &self.purchases {
            self.cat_totals[self.bins[bin].run as usize * categories + category as usize] += i64::from(price);
        }
        for (run, totals) in runs.iter_mut().zip(self.cat_totals.chunks_exact(categories)) {
            let mut best = 0;
            for (i, &total) in totals.iter().enumerate() {
                if total > best {
                    run.top = i as u32;
                    best = total;
                }
            }
        }
    }
}

fn process_person(ptr: u64, evs: &mut [TimedEvent], clicks: &mut [TimedClick], st: &mut W3, cx: &Ctx) {
    let ncamp = cx.camp_ids.len();
    let nd = cx.ndays;
    if let [event] = evs {
        let (start, price, category) = event.parts(cx.ws);
        let day = floor_div(start, DAY_MS);
        credit_single_session(
            UserRow {
                ptr, day: day as i32, sessions: 1, events: 1,
                rev: i64::from(price), top: if price == 0 { NO_CAT } else { category },
            },
            start, clicks, st, cx,
        );
        return;
    }
    let bins = ((cx.we - cx.ws - 1) / GAP_MS + 1) as usize;
    if evs.len() >= 384 && bins <= 2048 && cx.cat_names.len() <= 256 {
        st.binned.load_sessions(evs, cx.ws, bins, cx.cat_names.len(), &mut st.sess, &mut st.runs);
    } else {
        sort_events(evs, cx.ws, cx.compact_events);
        sorted_sessions(evs, cx.ws, &mut st.sess, &mut st.runs, &mut st.daycats);
    }
    if st.sess.len() == 1 {
        let s = &st.sess[0];
        let r = &st.runs[0];
        credit_single_session(
            UserRow { ptr, day: r.day as i32, sessions: 1, events: r.events, rev: r.rev, top: r.top },
            s.start, clicks, st, cx,
        );
        return;
    }
    // Reverse ID order within a timestamp, so the last eligible click
    // wins both the timestamp and byte-wise ID tie-break.
    let base = cx.click_lo;
    clicks.sort_unstable_by(|a, b| a.ts(base).cmp(&b.ts(base)).then_with(|| unpack(b.cid).cmp(unpack(a.cid))));
    // Small histories can credit and assist in one pass. Larger histories
    // use the interval union below to bound repeated work.
    let nsess = st.sess.len();
    let inline_assists = nsess.saturating_mul(clicks.len()) <= 256;
    let mut hi = 0usize;
    let mut lo = 0usize;
    for s in &st.sess {
        let day = (st.runs[s.run as usize].day - cx.day0) as usize;
        while hi < clicks.len() && clicks[hi].ts(base) <= s.start {
            hi += 1;
        }
        let camp = if hi > 0 && s.start - clicks[hi - 1].ts(base) <= WEEK_MS {
            if inline_assists {
                while s.start - clicks[lo].ts(base) > WEEK_MS {
                    lo += 1;
                }
                st.stamp = st.stamp.wrapping_add(1);
                if st.stamp == 0 {
                    st.seen.fill(0);
                    st.stamp = 1;
                }
                for c in &clicks[lo..hi] {
                    let campaign = c.camp();
                    if st.seen[campaign] != st.stamp {
                        st.seen[campaign] = st.stamp;
                        st.acc[campaign * nd + day][3] += 1;
                    }
                }
            }
            clicks[hi - 1].camp()
        } else {
            ncamp
        };
        let a = &mut st.acc[camp * nd + day];
        a[0] += 1;
        if s.purch {
            a[1] += 1;
        }
        a[2] += s.rev;
    }
    // assists
    if !inline_assists {
        let runs = &st.runs;
        let acc = &mut st.acc;
        st.assists.visit(&st.sess, clicks, base, |camp, a, b| {
            add_assist(runs, acc, cx, camp, a, b);
        });
    }
    // user rows
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

#[inline]
fn credit_single_session(row: UserRow, start: i64, clicks: &[TimedClick], st: &mut W3, cx: &Ctx) {
    let day = (i64::from(row.day) - cx.day0) as usize;
    let camp = single_session_clicks(
        start, clicks, cx.click_lo, cx.camp_ids.len(), cx.ndays, day,
        &mut st.seen, &mut st.stamp, &mut st.acc,
    );
    let a = &mut st.acc[camp * cx.ndays + day];
    a[0] += 1;
    a[1] += i64::from(row.rev != 0);
    a[2] += row.rev;
    st.rows.push(row);
}

// A shopper with one session needs no click sort. Scan for distinct
// assisting campaigns and the exact last-touch winner together.
#[inline]
fn single_session_clicks(
    start: i64, clicks: &[TimedClick], base: i64, organic: usize, nd: usize, day: usize,
    seen: &mut [u32], stamp: &mut u32, acc: &mut [[i64; 4]],
) -> usize {
    if clicks.is_empty() {
        return organic;
    }
    *stamp = stamp.wrapping_add(1);
    if *stamp == 0 {
        seen.fill(0);
        *stamp = 1;
    }
    let lower = start - WEEK_MS;
    let mut best_ts = i64::MIN;
    let mut best_id = 0;
    let mut best_campaign = organic;
    for &click in clicks {
        let (t, camp) = click.parts(base);
        if t < lower || t > start {
            continue;
        }
        let camp = camp as usize;
        if seen[camp] != *stamp {
            seen[camp] = *stamp;
            acc[camp * nd + day][3] += 1;
        }
        if best_campaign == organic || t > best_ts || (t == best_ts && unpack(click.cid) < unpack(best_id)) {
            best_ts = t;
            best_id = click.cid;
            best_campaign = camp;
        }
    }
    best_campaign
}

#[derive(Clone, Copy, Default)]
struct Span {
    stamp: u32,
    lo: usize,
    hi: usize,
}

struct AssistSweep {
    spans: Vec<Span>,
    active: Vec<usize>,
    stamp: u32,
}

impl AssistSweep {
    fn new(campaigns: usize) -> Self {
        Self { spans: vec![Span::default(); campaigns], active: Vec::new(), stamp: 0 }
    }

    fn visit(&mut self, sessions: &[Sess], clicks: &[TimedClick], base: i64, mut emit: impl FnMut(usize, usize, usize)) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.spans.fill(Span::default());
            self.stamp = 1;
        }
        self.active.clear();
        let (mut lo, mut hi) = (0, 0);
        for &click in clicks {
            let (t, camp) = click.parts(base);
            while lo < sessions.len() && sessions[lo].start < t {
                lo += 1;
            }
            if lo == sessions.len() {
                break;
            }
            hi = hi.max(lo);
            while hi < sessions.len() && sessions[hi].start <= t + WEEK_MS {
                hi += 1;
            }
            if lo == hi {
                continue;
            }
            // Both bounds advance monotonically in click-time order.
            // Keep one pending union per campaign and emit it only when
            // the next interval cannot touch it.
            let camp = camp as usize;
            let span = &mut self.spans[camp];
            if span.stamp != self.stamp {
                self.active.push(camp);
                *span = Span { stamp: self.stamp, lo, hi };
            } else if lo <= span.hi {
                span.hi = hi;
            } else {
                emit(camp, span.lo, span.hi);
                *span = Span { stamp: self.stamp, lo, hi };
            }
        }
        for &camp in &self.active {
            let span = &self.spans[camp];
            emit(camp, span.lo, span.hi);
        }
    }
}

#[inline(always)]
fn add_assist(runs: &[Run], acc: &mut [[i64; 4]], cx: &Ctx, camp: usize, a: usize, b: usize) {
    let nd = cx.ndays;
    for r in runs {
        let lo = a.max(r.first as usize);
        let hi = b.min(r.end as usize);
        if lo < hi {
            acc[camp * nd + (r.day - cx.day0) as usize][3] += (hi - lo) as i64;
        }
    }
}

fn top_cat(c: &[(u32, i64)]) -> u32 {
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
    ptr: u64,
    ec: u32,
    cc: u32,
    es: u32,
    cs: u32,
}

fn sort_events(events: &mut [TimedEvent], ws: i64, compact: bool) {
    if compact {
        events.sort_unstable();
    } else {
        events.sort_unstable_by_key(|e| e.parts(ws).0);
    }
}

fn phase3_bucket(b: usize, p1: &[P1], st: &mut W3, cx: &Ctx, out_dir: &str) {
    let evs = || p1.iter().flat_map(|w| w.live_events(b));
    let clicks = || p1.iter().flat_map(|w| &w.ck[b]);
    let n: usize = p1.iter().map(|w| w.count(b)).sum();
    if n == 0 { return; }
    let cap = (2*n).next_power_of_two().max(16);
    let mask = cap - 1;
    st.index.clear(); st.index.resize(cap, u32::MAX);
    st.groups.clear(); st.ev_ids.clear(); st.ck_ids.clear();
    for e in evs() {
        let mut j = (user_hash(e.key) as usize) & mask;
        loop {
            let gid = st.index[j];
            if gid == u32::MAX {
                let id = st.groups.len() as u32;
                st.groups.push(Group {key:e.key, ptr:e.ptr, ec:1, cc:0, es:0, cs:0});
                st.index[j] = id;
                st.ev_ids.push(id);
                break;
            }
            if st.groups[gid as usize].key == e.key && same_user(e.key, st.groups[gid as usize].ptr, e.ptr) {
                st.groups[gid as usize].ec += 1;
                st.ev_ids.push(gid);
                break;
            }
            j = (j + 1) & mask;
        }
    }
    for ck in clicks() {
        let mut j = (user_hash(ck.key) as usize) & mask;
        loop {
            let gid = st.index[j];
            if gid == u32::MAX { st.ck_ids.push(gid); break; }
            if st.groups[gid as usize].key == ck.key && same_user(ck.key, st.groups[gid as usize].ptr, ck.ptr) {
                st.groups[gid as usize].cc += 1;
                st.ck_ids.push(gid);
                break;
            }
            j = (j+1)&mask;
        }
    }
    let mut es = 0u32; let mut cs = 0u32;
    for g in &mut st.groups {
        g.es = es; es += g.ec; g.ec = g.es;
        g.cs = cs; cs += g.cc; g.cc = g.cs;
    }
    let (es, cs) = (es as usize, cs as usize);
    if st.ordered_ev.len() < es {
        st.ordered_ev.resize(es, TimedEvent::default());
    }
    if st.ordered_ck.len() < cs {
        st.ordered_ck.resize(cs, TimedClick::default());
    }
    for (e, &gid) in evs().zip(&st.ev_ids) {
        let g = &mut st.groups[gid as usize];
        st.ordered_ev[g.ec as usize] = TimedEvent(e.data.load(Ordering::Relaxed)); g.ec += 1;
    }
    for (ck, &gid) in clicks().zip(&st.ck_ids) {
        if gid != u32::MAX {
            let g = &mut st.groups[gid as usize];
            st.ordered_ck[g.cc as usize] = ck.timed; g.cc += 1;
        }
    }
    let mut ev = std::mem::take(&mut st.ordered_ev);
    let mut ck = std::mem::take(&mut st.ordered_ck);
    let groups = std::mem::take(&mut st.groups);
    for g in &groups {
        let events = &mut ev[g.es as usize..g.ec as usize];
        let clicks = &mut ck[g.cs as usize..g.cc as usize];
        let name = if g.key & EXACT_USER == 0 { g.ptr } else { g.key };
        process_person(name, events, clicks, st, cx);
    }
    st.ordered_ev = ev; st.ordered_ck = ck; st.groups = groups;
    if st.rows.len() >= 200_000 { flush_user_rows(st,cx,out_dir); }
}

// ---------------------------------------------------------------- main

fn main() {
    let overall=std::time::Instant::now();
    let profile = std::env::var_os("PIPELINE_PROFILE").is_some_and(|v| v == "1");
    let data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map_or(4, |n| n.get()));
    let nthreads = nthreads.max(1);
    #[cfg(target_arch = "x86_64")]
    let (avx512, avx2) = {
        let vector = std::arch::is_x86_feature_detected!("bmi2")
            && std::arch::is_x86_feature_detected!("popcnt")
            && std::arch::is_x86_feature_detected!("ssse3")
            && std::arch::is_x86_feature_detected!("sse4.1");
        let prefer_avx2 = std::env::var_os("PIPELINE_AVX2").is_some_and(|v| v == "1");
        (
            vector && !prefer_avx2 && std::arch::is_x86_feature_detected!("avx512bw"),
            vector && std::arch::is_x86_feature_detected!("avx2"),
        )
    };

    let manifest = std::fs::read_to_string(format!("{}/manifest.json", data)).expect("manifest");
    let mut dates = DateCache::new();
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes(), &mut dates).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes(), &mut dates).expect("window_end");

    // products, campaigns
    let prows = read_table(&format!("{}/products.csv", data));
    let mut cats: Vec<&[u8]> = prows.iter().map(|r| r[1]).collect();
    cats.sort();
    cats.dedup();
    if !cats.contains(&&b"unknown"[..]) {
        cats.push(b"unknown");
        cats.sort();
    }
    let rank_of = |c: &[u8]| cats.binary_search(&c).unwrap() as u32;
    let mut prod = SmallMap::new(prows.len());
    for r in &prows {
        prod.insert(r[0], rank_of(r[1]));
    }
    let unknown_rank = rank_of(b"unknown");
    let crows = read_table(&format!("{}/campaigns.csv", data));
    let mut camp = SmallMap::new(crows.len());
    let mut camp_ids = vec![];
    let mut camp_ch = vec![];
    for r in &crows {
        if camp.get(r[0]).is_some() {
            continue;
        }
        camp.insert(r[0], camp_ids.len() as u32);
        camp_ids.push(r[0]);
        camp_ch.push(r[1]);
    }
    let day0 = floor_div(ws, DAY_MS);
    let ndays = (floor_div(we - 1, DAY_MS) - day0 + 1).max(1) as usize;
    let ev_files = load_dir(&format!("{}/events", data));
    let event_bytes = ev_files.iter().map(|x| x.len()).sum::<usize>();
    let cx = Ctx {
        ws,
        we,
        compact_events: we - ws <= 1 << 31 && cats.len() <= 256,
        // Every retained event contains at least its 20-byte ingestion
        // timestamp. Thus this also bounds both per-person count columns.
        small_counts: event_bytes / 20 <= i32::MAX as usize,
        click_lo: ws - WEEK_MS,
        day0,
        ndays,
        prod,
        unknown_rank,
        camp,
        camp_ids,
        camp_ch,
        cat_names: cats,
    };

    let ck_files = load_dir(&format!("{}/clicks", data));
    let ln_files = load_dir(&format!("{}/links", data));
    let ev_chunks = make_chunks(&ev_files);
    let ck_chunks = make_chunks(&ck_files);
    // Capacity hints only. All arrays still grow as necessary for arbitrary
    // IDs, part sizes, and people with disproportionately many events.
    let event_hint = event_bytes / 72 / nthreads;
    let click_hint = ck_files.iter().map(|x| x.len()).sum::<usize>() / 46 / nthreads;

    std::fs::create_dir_all(format!("{}/user_daily", out)).unwrap();
    std::fs::create_dir_all(format!("{}/campaign_daily", out)).unwrap();

    if profile { eprintln!("setup {:?}", overall.elapsed()); }
    let stage = std::time::Instant::now();
    // Parse clicks and events while union-find is running. Once aliases
    // are known, place the compact payload directly in its person shard.
    let links = OnceLock::new();
    let nck = ck_chunks.len();
    let mut p1 = thread::scope(|sc| {
        sc.spawn(|| {
            let started = profile.then(std::time::Instant::now);
            let table = build_links(&ln_files);
            let _ = links.set(table);
            if let Some(t) = started { eprintln!("links {:?}", t.elapsed()); }
        });
        par_run(
            nthreads,
            nck + ev_chunks.len(),
            None,
            |_| P1::new(event_hint, click_hint),
            |st, ti| {
                let lt = links.get();
                if let Some(lt) = lt {
                    if !st.pending_events.is_empty() || !st.pending_clicks.is_empty() {
                        st.route_pending(lt);
                    }
                }
                if ti < nck {
                    let (fi, a, b) = ck_chunks[ti];
                    let data = &ck_files[fi][a..b];
                    #[cfg(target_arch = "x86_64")]
                    {
                        if avx512 {
                            unsafe { parse_clicks_fast(data, a == 0, &cx, st, lt); }
                            return;
                        }
                        if avx2 {
                            unsafe { parse_clicks_avx2(data, a == 0, &cx, st, lt); }
                            return;
                        }
                    }
                    for_lines(data, a == 0, |line| {
                        if let Some(click) = parse_click(line, &cx, &mut st.dates) {
                            st.route_click(click, lt);
                        }
                    });
                    return;
                }
                let (fi, a, b) = ev_chunks[ti - nck];
                let data = &ev_files[fi][a..b];
                #[cfg(target_arch = "x86_64")]
                {
                    if avx512 {
                        unsafe { parse_events_fast(data, a == 0, &cx, st, lt); }
                        return;
                    }
                    if avx2 {
                        unsafe { parse_events_avx2(data, a == 0, &cx, st, lt); }
                        return;
                    }
                }
                for_lines(data, a == 0, |l| {
                    if let Some(r) = parse_event(l, &cx, &mut st.dates) {
                        st.route_event(r, lt);
                    }
                });
            },
            |st| {
                if !st.pending_events.is_empty() || !st.pending_clicks.is_empty() {
                    st.route_pending(links.wait());
                }
            },
        )
    });
    drop(links);

    if profile { eprintln!("parse and route {:?}", stage.elapsed()); }
    let stage = std::time::Instant::now();
    // Event-ID shards touch only the small index plus the bits for
    // superseded candidates. No second full payload-routing pass.
    par_run(
        nthreads,
        B1,
        None,
        |_| Vec::new(),
        |st, b| dedup_bucket(b, &p1, st),
        |_| {},
    );
    for w in &mut p1 {
        w.ids.clear();
    }

    if profile { eprintln!("dedup {:?}", stage.elapsed()); }
    let stage = std::time::Instant::now();
    // phase 3: sessions, attribution, output
    let mut sizes: Vec<(usize, usize)> = (0..B2).map(|b| (p1.iter().map(|w| w.count(b)).sum::<usize>(), b)).collect();
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
            sess: Vec::new(),
            runs: Vec::new(),
            daycats: Vec::new(),
            assists: AssistSweep::new(nc1),
            groups: Vec::new(),
            index: Vec::new(),
            ev_ids: Vec::new(),
            ck_ids: Vec::new(),
            ordered_ev: Vec::new(),
            ordered_ck: Vec::new(),
            binned: Binned::default(),
            seen: vec![0; nc1],
            stamp: 0,
        },
        |st, b| phase3_bucket(b, &p1, st, &cx, &out),
        |st| {
            flush_user_rows(st, &cx, &out);
            if let Some(wr) = st.writer.take() {
                wr.close().unwrap();
            }
        },
    );
    drop(p1);

    if profile { eprintln!("sessions {:?}", stage.elapsed()); }
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
        let schema = if cx.small_counts { USER_SCHEMA_SMALL } else { USER_SCHEMA };
        let wr = new_writer(&format!("{}/user_daily/part-0.parquet", out), schema);
        wr.close().unwrap();
    }

    if profile { eprintln!("reduce {:?}", stage.elapsed()); }
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
                cid.push(parquet_string(cx.camp_ids[c]));
                chn.push(parquet_string(cx.camp_ch[c]));
            } else {
                cid.push(parquet_string(b"organic"));
                chn.push(parquet_string(b"organic"));
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
    if profile { eprintln!("total {:?}", overall.elapsed()); }
}

#[cfg(test)]
mod tests {
    use super::{event_fields, event_ref, pack, parse_price, parse_ts, short_name, sort_events, split, split_scalar, unpack, user_code, DateCache, TimedEvent, EXACT_USER};
    #[cfg(target_arch = "x86_64")]
    use super::split_masks;

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn vector_rows_match_scalar_at_cutoffs_and_file_tail() {
        if !(std::arch::is_x86_feature_detected!("bmi2") && std::arch::is_x86_feature_detected!("popcnt")) {
            return;
        }
        #[target_feature(enable = "avx2,bmi2,popcnt")]
        unsafe fn scan_avx2(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
            let mut rows = Vec::new();
            super::scan_rows_avx2::<4>(data, true, |fields| {
                rows.push(fields.iter().map(|f| f.to_vec()).collect());
            });
            rows
        }
        #[target_feature(enable = "avx512bw,bmi2,popcnt")]
        unsafe fn scan_avx512(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
            let mut rows = Vec::new();
            super::scan_rows_avx512::<4>(data, true, |fields| {
                rows.push(fields.iter().map(|f| f.to_vec()).collect());
            });
            rows
        }
        let mut input = b"header,with,four,columns\n".to_vec();
        for n in 0..260 {
            input.extend_from_slice(format!("\"{}\",,a,\"\"\n", "x".repeat(n)).as_bytes());
            input.extend_from_slice(format!("\"{},,a,\"\"\n", "y".repeat(n)).as_bytes());
        }
        input.extend_from_slice(b"end,\"last\",,field");
        let expected: Vec<Vec<Vec<u8>>> = input.split(|&b| b == b'\n').skip(1)
            .filter_map(split_scalar::<4>)
            .map(|fields| fields.iter().map(|f| f.to_vec()).collect())
            .collect();
        if std::arch::is_x86_feature_detected!("avx2") {
            assert_eq!(unsafe { scan_avx2(&input) }, expected);
        }
        if std::arch::is_x86_feature_detected!("avx512bw") {
            assert_eq!(unsafe { scan_avx512(&input) }, expected);
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn vector_epoch_validates_every_byte() {
        if !(std::arch::is_x86_feature_detected!("ssse3") && std::arch::is_x86_feature_detected!("sse4.1")) {
            return;
        }
        let mut cache = DateCache::new();
        for mut input in [*b"0000000000000", *b"1791071890000", *b"9999999999999"] {
            for i in 0..input.len() {
                let old = input[i];
                for byte in 0..=255 {
                    input[i] = byte;
                    assert_eq!(super::parse_row_timestamp::<true>(&input, &mut cache), parse_ts(&input, &mut cache));
                }
                input[i] = old;
            }
        }
    }

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
        for len in [0, 1, 32_766, 32_767, 65_534, 65_535, 65_536] {
            let value = vec![b'a'; len];
            assert_eq!(unpack(pack(&value)), value);
        }
    }

    #[test]
    fn event_refs_preserve_ids_and_ingestion_order() {
        let ingest = b"2026-01-01T10:11:12Z";
        for n in [1, 20, 254, 255, 256, 65_536] {
            let mut row = vec![b'e'; n];
            row.push(b',');
            row.extend_from_slice(ingest);
            let (id, ts) = event_fields(event_ref(&row[..n], Some(&row[n + 1..])));
            assert_eq!(id, &row[..n]);
            assert_eq!(ts, ingest);
        }
        let (_, ts) = event_fields(event_ref(b"id", None));
        assert!(ts > &b"9999-12-31T23:59:59Z"[..]);
    }

    #[test]
    fn timestamp_fast_paths_agree_across_representations() {
        let mut cache = DateCache::new();
        for (epoch, utc, offset) in [
            (1791071890000, &b"2026-10-03T23:58:10Z"[..], &b"2026-10-03T19:58:10-04:00"[..]),
            (1835395200000, &b"2028-02-29T00:00:00Z"[..], &b"2028-02-29T05:30:00+05:30"[..]),
        ] {
            assert_eq!(parse_ts(utc, &mut cache), Some(epoch));
            assert_eq!(parse_ts(offset, &mut cache), Some(epoch));
            assert_eq!(parse_ts(epoch.to_string().as_bytes(), &mut cache), Some(epoch));
            assert_eq!(parse_ts(format!("0000000000{epoch}").as_bytes(), &mut cache), Some(epoch));
        }
        for ts in [&b"2027-02-29T00:00:00Z"[..], b"2028-02-29T00:00:60Z", b"2028-02-29T24:00:00Z", b"2028-02-29T00:00:00+24:00", b"2028-02-29T00:00:00+00:60", b"2028-02-29T00:00:00Z\r", b"17910:1890000"] {
            assert_eq!(parse_ts(ts, &mut cache), None);
        }
        assert_eq!(parse_price(b"00000010000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"5\r"), None);
    }

    #[test]
    fn short_person_keys_preserve_every_byte() {
        let base = *b"u91abcdef";
        let code = user_code(&base);
        assert_ne!(code & EXACT_USER, 0);
        assert_ne!(code, user_code(&base[..8]));
        assert_eq!(short_name(user_code(&base[..8])), (*b"u91abcde", 0));
        for i in 0..base.len() {
            let mut observed = std::collections::HashSet::new();
            let mut id = base;
            for byte in 1..128 {
                id[i] = byte;
                let key = user_code(&id);
                assert_ne!(key & EXACT_USER, 0);
                assert!(observed.insert(key));
                assert_eq!(short_name(key), (id[..8].try_into().unwrap(), id[8]));
            }
        }
        assert_eq!(user_code(b"u91abcde\0") & EXACT_USER, 0);
        assert_eq!(user_code("Å91abcdef".as_bytes()) & EXACT_USER, 0);
    }

    #[test]
    fn large_history_sort_keeps_payloads_with_their_timestamps() {
        let ws = -86_400_000i64;
        let mut seed = 24601u32;
        let mut events: Vec<_> = (0..18_000).map(|i| {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            TimedEvent::new(ws + (seed >> 1) as i64, i, 3, ws)
        }).collect();
        let mut expected = events.clone();
        expected.sort_by_key(|e| e.parts(ws));
        sort_events(&mut events, ws, true);
        assert!(events.iter().zip(expected).all(|(a, b)| a.parts(ws) == b.parts(ws)));

        let mut wide = [
            TimedEvent::new(ws + (1 << 31), 10_000_000, 255, ws),
            TimedEvent::new(ws + 8, 13, 256, ws),
            TimedEvent::new(ws, 123, 0, ws),
        ];
        sort_events(&mut wide, ws, false);
        assert_eq!(wide.map(|e| e.parts(ws)), [(ws, 123, 0), (ws + 8, 13, 256), (ws + (1 << 31), 10_000_000, 255)]);
    }

    #[test]
    fn binned_sessions_match_sorted_sessions_at_exact_boundaries() {
        use super::{sorted_sessions, Binned, DAY_MS, GAP_MS};

        let ws = -DAY_MS - 47;
        let offsets = [
            (0, 0, 0),
            (GAP_MS - 1, 0, 0),
            (GAP_MS, 1, 2),
            (GAP_MS, 0, 2),
            (2 * GAP_MS, 1, 5),
            (2 * GAP_MS, 0, 5),
            (3 * GAP_MS + 1, 1, 5),
            (DAY_MS - 1, 0, 2),
            (DAY_MS + 47, 1, 2),
            (DAY_MS + 47 + GAP_MS, 0, 0),
            (DAY_MS + 48 + 2 * GAP_MS, 1, 11),
        ];
        let sorted: Vec<_> = offsets.into_iter().map(|(ts, rank, price)| {
            TimedEvent::new(ws + ts, price, rank, ws)
        }).collect();
        let mut unsorted = sorted.clone();
        unsorted.reverse();
        let (mut exp_sess, mut exp_runs) = (Vec::new(), Vec::new());
        sorted_sessions(&sorted, ws, &mut exp_sess, &mut exp_runs, &mut Vec::new());
        let (mut got_sess, mut got_runs) = (Vec::new(), Vec::new());
        let mut binner = Binned::default();
        binner.load_sessions(&unsorted, ws, 100, 2, &mut got_sess, &mut got_runs);
        assert_eq!(exp_sess, got_sess);
        assert_eq!(exp_runs, got_runs);
        assert_eq!(got_runs.iter().map(|r| (r.day, r.top)).collect::<Vec<_>>(), vec![(-2, 0), (-1, 1), (0, 1)]);
        let fewer = &sorted[..4];
        sorted_sessions(fewer, ws, &mut exp_sess, &mut exp_runs, &mut Vec::new());
        binner.load_sessions(fewer, ws, 80, 2, &mut got_sess, &mut got_runs);
        assert_eq!(exp_sess, got_sess);
        assert_eq!(exp_runs, got_runs);
    }

    #[test]
    fn chronological_assist_sweep_matches_direct_eligibility() {
        use super::{AssistSweep, Sess, TimedClick, WEEK_MS};
        let base = -2 * WEEK_MS;
        let mut sweep = AssistSweep::new(9);
        for seed in 0..20u32 {
            let mut rng = seed;
            let mut next = || {
                rng = rng.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                rng
            };
            let mut sessions: Vec<_> = (0..23).map(|_| Sess {
                start: i64::from(next()), rev: 0, purch: false, run: 0,
            }).collect();
            sessions.sort_unstable_by_key(|s| s.start);
            let mut clicks: Vec<_> = (0..450).map(|_| {
                TimedClick::new(i64::from(next()) + base, next() % 9, 0, base)
            }).collect();
            // Both inclusive edges, one click just outside, and duplicates.
            let t = sessions[0].start;
            for (ts, camp) in [(t, 3), (t, 3), (t + 1, 3), (t - WEEK_MS, 4), (t - WEEK_MS - 1, 5)] {
                clicks.push(TimedClick::new(ts, camp, 0, base));
            }
            clicks.sort_unstable_by_key(|c| c.ts(base));
            let mut observed = vec![vec![0; sessions.len()]; 9];
            sweep.visit(&sessions, &clicks, base, |camp, lo, hi| {
                for v in &mut observed[camp][lo..hi] { *v += 1; }
            });
            for (camp, hits) in observed.iter().enumerate() {
                for (i, s) in sessions.iter().enumerate() {
                    let wants = clicks.iter().any(|c| {
                        let t = c.ts(base);
                        c.camp() == camp && t <= s.start && s.start - t <= WEEK_MS
                    });
                    assert_eq!(hits[i], u8::from(wants), "seed={seed} campaign={camp} time={}", s.start);
                }
            }
        }
    }

    #[test]
    fn one_session_needs_no_click_order_for_ties_or_assists() {
        use super::{single_session_clicks, TimedClick, WEEK_MS};
        let base = -WEEK_MS - 1;
        let click = |t, c, id| TimedClick::new(t, c, pack(id), base);
        let clicks = [
            click(0, 1, b"id-z"), click(1, 4, b"future"), click(-WEEK_MS, 0, b"incl"),
            click(-WEEK_MS - 1, 4, b"outside"), click(0, 2, b"id-a"),
            click(-123, 1, b"another"), click(0, 2, b"id-a"),
        ];
        let mut sums = [[0; 4]; 6];
        let mut seen = [u32::MAX; 6];
        let mut stamp = u32::MAX;
        assert_eq!(single_session_clicks(0, &clicks, base, 5, 1, 0, &mut seen, &mut stamp, &mut sums), 2);
        assert_eq!(sums.map(|a| a[3]), [1, 1, 1, 0, 0, 0]);
    }

    #[test]
    fn compact_link_table_preserves_canonical_names() {
        use super::{build_links, user_hash};

        let source = b"cookie_a,cookie_b\nz12345678,b1234567\nb1234567,c12345678\nc12345678,alpha\nalpha,z12345678\nb1234567,z12345678\nself,self\nw12345678,\"\xc3\xa9_cookie\"\n\xc3\xa9_cookie,y12345678\n,discarded\n";
        let links = build_links(&[source, source]);
        let name = |id: &[u8]| -> Vec<u8> {
            let code = user_code(id);
            let (code, ptr) = links.get(code, user_hash(code), pack(id)).unwrap_or((code, pack(id)));
            if code & EXACT_USER == 0 {
                unpack(ptr).to_vec()
            } else {
                let (first, last) = short_name(code);
                let mut bytes = first.to_vec();
                if last != 0 { bytes.push(last); }
                bytes
            }
        };
        for (id, canonical) in [
            (&b"z12345678"[..], &b"alpha"[..]),
            (b"b1234567", b"alpha"),
            (b"c12345678", b"alpha"),
            (b"self", b"self"),
            (b"discarded", b"discarded"),
            (b"\xc3\xa9_cookie", b"w12345678"),
            (b"y12345678", b"w12345678"),
        ] {
            assert_eq!(name(id), canonical, "{id:?}");
        }
    }

    #[test]
    fn compact_event_index_checks_bytes_and_retires_only_losing_copies() {
        use super::{build_links, dedup_bucket, P1, Raw, B2, INVALID_EVENT};

        let links = build_links(&[]);
        let mut worker = P1::new(0, 0);
        for (line, person, data) in [
            (&b"A,2026-01-01T00:00:01Z"[..], &b"uolduser1"[..], 1),
            (b"A,2026-01-01T00:00:00Z", b"unewuser1", 2),
            (b"B,2026-01-01T00:00:01Z", b"udistinct", 3),
            (b"C,2026-01-01T00:00:00Z", b"ubaduser1", INVALID_EVENT),
            (b"C,2026-01-01T00:00:01Z", b"ubaduser2", 4),
        ] {
            worker.route_event(Raw {
                eid: 7,  // Force a hash collision between the distinct event IDs.
                source: event_ref(&line[..1], Some(&line[2..])),
                uh: user_code(person),
                uptr: pack(person),
                data,
            }, Some(&links));
        }
        dedup_bucket(0, std::slice::from_ref(&worker), &mut Vec::new());
        let mut live: Vec<_> = (0..B2).flat_map(|b| worker.live_events(b))
            .map(|e| (e.key, e.data.load(std::sync::atomic::Ordering::Relaxed)))
            .collect();
        live.sort_unstable();
        let mut expected = vec![(user_code(b"unewuser1"), 2), (user_code(b"udistinct"), 3)];
        expected.sort_unstable();
        assert_eq!(live, expected);
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
        #[cfg(target_arch = "x86_64")]
        #[target_feature(enable = "bmi2,popcnt")]
        unsafe fn split_vector<const N: usize>(line: &[u8], c: u128, q: u128) -> Option<[&[u8]; N]> {
            super::split_masks_bmi2(line, c, q)
        }
        #[cfg(target_arch = "x86_64")]
        let vector = std::arch::is_x86_feature_detected!("bmi2") && std::arch::is_x86_feature_detected!("popcnt");
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
                #[cfg(target_arch = "x86_64")]
                {
                    let mask = |byte| line.iter().enumerate().fold(0, |mask, (i, &b)| {
                        mask | (u128::from(b == byte) << i)
                    });
                    let (commas, quotes) = (mask(b','), mask(b'"'));
                    assert_eq!(split_masks::<2>(&line, commas, quotes).map(|v| v.to_vec()), reference(&line, 2), "{line:?}");
                    assert_eq!(split_masks::<4>(&line, commas, quotes).map(|v| v.to_vec()), reference(&line, 4), "{line:?}");
                    assert_eq!(split_masks::<7>(&line, commas, quotes).map(|v| v.to_vec()), reference(&line, 7), "{line:?}");
                    if vector {
                        assert_eq!(unsafe { split_vector::<2>(&line, commas, quotes) }.map(|v| v.to_vec()), reference(&line, 2), "{line:?}");
                        assert_eq!(unsafe { split_vector::<4>(&line, commas, quotes) }.map(|v| v.to_vec()), reference(&line, 4), "{line:?}");
                        assert_eq!(unsafe { split_vector::<7>(&line, commas, quotes) }.map(|v| v.to_vec()), reference(&line, 7), "{line:?}");
                    }
                }
            }
        }
    }
}

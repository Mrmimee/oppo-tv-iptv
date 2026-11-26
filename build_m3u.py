#!/usr/bin/env python3
"""Fetch, speed-test, rank, and merge CCTV, Chinese Satellite TV, BBC & International News M3U playlists.

Features:
- Aggregates multiple active IPv4 & IPv6 IPTV sources.
- Normalizes channel names (CCTV-1..17, 5+, 卫视) so players like MyTV group all routes under each channel.
- Performs real stream & HLS segment download testing (not just HTTP 200 header checks) to eliminate dead/laggy links.
- Ranks verified routes per channel by real throughput (KB/s) and startup latency, keeping top N routes per channel.
"""

import concurrent.futures
import re
import socket
import ssl
import time
import urllib.parse
import urllib.request
from pathlib import Path

OUT_FILE = Path(__file__).parent / "tv_channels.m3u"

MAX_PER_SOURCE_PER_CHANNEL = 4
MAX_ROUTES_PER_CHANNEL = 6
MIN_SPEED_KBS = 120.0       # Minimum real throughput (KB/s) for smooth playback
FALLBACK_SPEED_KBS = 50.0   # Fallback minimum if a channel has no faster route
PROBE_BYTES = 256 * 1024    # Read up to 256 KB per stream to measure real speed
PROBE_DURATION = 1.6        # Max seconds to spend reading payload per stream
CONNECT_TIMEOUT = 3.5       # Seconds

SOURCES = {
    # Chinese CCTV & Satellite (IPv4)
    "guovin_v4": "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv4/result.m3u",
    "suxuang_v4": "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "vbskycn_v4": "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u",
    "yang_gather": "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "kimentanm": "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    # Chinese CCTV & Satellite (IPv6)
    "guovin_v6": "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv6/result.m3u",
    "fanmingming_v6": "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u",
    "suxuang_v6": "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv6.m3u",
    # International
    "uk": "https://iptv-org.github.io/iptv/countries/uk.m3u",
    "news": "https://iptv-org.github.io/iptv/categories/news.m3u",
}

CN_IPV4_SOURCES = ("guovin_v4", "suxuang_v4", "vbskycn_v4", "yang_gather", "kimentanm")
CN_IPV6_SOURCES = ("guovin_v6", "fanmingming_v6", "suxuang_v6")

INTL_NEWS_KEYWORDS = (
    "bbc",
    "sky news",
    "cnn",
    "bloomberg",
    "al jazeera",
    "euronews",
    "dw english",
    "france 24 english",
    "nhk world",
    "cna",
    "cgtn",
    "abc news",
    "nbc news",
    "cbs news",
    "reuters",
)

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 9; OPPOCNM652) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36",
    "Accept": "*/*",
}

WEISHI_NAMES = [
    "湖南卫视", "浙江卫视", "东方卫视", "江苏卫视", "北京卫视",
    "深圳卫视", "广东卫视", "安徽卫视", "山东卫视", "黑龙江卫视",
    "辽宁卫视", "天津卫视", "河北卫视", "河南卫视", "湖北卫视",
    "四川卫视", "重庆卫视", "东南卫视", "江西卫视", "贵州卫视",
    "云南卫视", "陕西卫视", "山西卫视", "吉林卫视", "广西卫视",
    "海南卫视", "青海卫视", "宁夏卫视", "甘肃卫视", "内蒙古卫视",
    "新疆卫视", "西藏卫视", "兵团卫视", "厦门卫视", "延边卫视",
    "康巴卫视", "安多卫视", "农林卫视", "三沙卫视", "海峡卫视",
    "大湾区卫视", "凤凰卫视中文台", "凤凰卫视资讯台", "凤凰卫视香港台",
]


def has_ipv6() -> bool:
    try:
        s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        s.settimeout(1.5)
        s.connect(("2400:3200::1", 53))
        s.close()
        return True
    except Exception:
        return False


def is_ipv6_url(url: str) -> bool:
    host = urllib.parse.urlparse(url).hostname or ""
    return ":" in host


def fetch(url: str, timeout: float = 15.0) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def parse_m3u(text: str):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    entries = []
    i = 0
    while i < len(lines):
        if lines[i].startswith("#EXTINF:"):
            extinf = lines[i]
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            if j < len(lines):
                url = lines[j]
                name = extinf.rsplit(",", 1)[-1].strip()
                grp_m = re.search(r'group-title="([^"]*)"', extinf)
                group = grp_m.group(1) if grp_m else ""
                logo_m = re.search(r'tvg-logo="([^"]*)"', extinf)
                logo = logo_m.group(1) if logo_m else ""
                tvg_id_m = re.search(r'tvg-id="([^"]*)"', extinf)
                tvg_id = tvg_id_m.group(1) if tvg_id_m else ""
                entries.append(
                    {
                        "name": name,
                        "group": group,
                        "logo": logo,
                        "tvg_id": tvg_id,
                        "url": url,
                    }
                )
            i = j + 1
        else:
            i += 1
    return entries


def normalize_cn_channel(name: str, group: str):
    """Return (target_group, canonical_name) or (None, None) if not CCTV/卫视."""
    raw = name.strip()
    up = raw.upper().replace(" ", "").replace("－", "-").replace("—", "-")

    # 1. CCTV channels
    if up.startswith("CCTV") or "央视" in group:
        # CCTV-5+
        if re.match(r"^CCTV-?5(\+|PLUS)", up):
            return "央视", "CCTV-5+"
        # CCTV-4K / CCTV-8K / CCTV-16-4K
        m4k = re.match(r"^CCTV-?(4K|8K)", up)
        if m4k:
            return "央视", f"CCTV-{m4k.group(1)}"
        m16_4k = re.match(r"^CCTV-?16.*4K", up)
        if m16_4k:
            return "央视", "CCTV-16-4K"
        # CCTV-1 .. CCTV-17
        m = re.match(r"^CCTV-?(\d{1,2})", up)
        if m:
            num = int(m.group(1))
            if 1 <= num <= 17:
                return "央视", f"CCTV-{num}"
        # Special CCTV channels (CGTN / 风云 / 兵器 / 怀旧 / 第一剧场 etc.)
        if up.startswith("CGTN"):
            clean = re.sub(r"(HD|高清|标清|超清|FHD|1080P|IPV[46]).*$", "", raw, flags=re.I).strip()
            return "央视", clean
        return None, None

    # 2. Satellite TV (卫视)
    if "卫视" in raw or "卫视" in group:
        for ws in WEISHI_NAMES:
            if ws in raw:
                return "卫视", ws
        if "凤凰中文" in raw:
            return "卫视", "凤凰卫视中文台"
        if "凤凰资讯" in raw:
            return "卫视", "凤凰卫视资讯台"
        if "凤凰香港" in raw:
            return "卫视", "凤凰卫视香港台"
    return None, None


def _read_stream_speed(url: str, timeout: float = CONNECT_TIMEOUT):
    """Download up to PROBE_BYTES or PROBE_DURATION seconds and return (bytes_read, elapsed, ttfb, first_chunk)."""
    t0 = time.monotonic()
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
        if not (200 <= resp.status < 400):
            return None
        final_url = resp.geturl()
        ctype = (resp.headers.get("Content-Type") or "").lower()
        t_resp = time.monotonic()
        ttfb = t_resp - t0

        chunks = []
        total = 0
        while total < PROBE_BYTES and (time.monotonic() - t_resp) < PROBE_DURATION:
            chunk = resp.read(32768)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)

        elapsed = max(time.monotonic() - t_resp, 0.01)
        data = b"".join(chunks)
        return {
            "final_url": final_url,
            "ctype": ctype,
            "ttfb": ttfb,
            "bytes": total,
            "elapsed": elapsed,
            "data": data,
        }


def _extract_hls_next_url(m3u8_text: str, base_url: str):
    """Return the first media segment or variant playlist URL from an HLS playlist."""
    lines = [l.strip() for l in m3u8_text.splitlines() if l.strip()]
    # Prefer the first non-comment URL line
    for line in lines:
        if not line.startswith("#"):
            return urllib.parse.urljoin(base_url, line)
    return None


def probe_stream(item: dict):
    """Verify real video playability and measure throughput (KB/s) and latency (ms)."""
    url = item["url"]
    try:
        res = _read_stream_speed(url)
        if not res or res["bytes"] == 0:
            return None

        head_text = res["data"][:512].lstrip()
        # Reject HTML error pages
        if head_text.lower().startswith((b"<html", b"<!doctype", b"<head", b"<?xml")):
            return None

        is_hls = (
            head_text.startswith(b"#EXTM3U")
            or "mpegurl" in res["ctype"]
            or ".m3u8" in url.lower()
        )

        if is_hls:
            text = res["data"].decode("utf-8", errors="ignore")
            if "#EXTM3U" not in text:
                return None

            # Follow up to 2 levels (master -> variant -> first media segment)
            cur_url = res["final_url"]
            cur_text = text
            seg_url = None
            for _ in range(2):
                next_url = _extract_hls_next_url(cur_text, cur_url)
                if not next_url:
                    return None
                if "#EXT-X-STREAM-INF" in cur_text or ".m3u8" in next_url.lower():
                    sub = _read_stream_speed(next_url, timeout=CONNECT_TIMEOUT)
                    if not sub or sub["bytes"] == 0:
                        return None
                    sub_head = sub["data"][:512].lstrip()
                    if sub_head.startswith(b"#EXTM3U"):
                        cur_url = sub["final_url"]
                        cur_text = sub["data"].decode("utf-8", errors="ignore")
                        continue
                    # Otherwise it was already a media segment
                    seg_res = sub
                    seg_url = next_url
                    break
                else:
                    seg_url = next_url
                    seg_res = _read_stream_speed(seg_url, timeout=CONNECT_TIMEOUT)
                    break
            else:
                return None

            if not seg_res or seg_res["bytes"] < 16 * 1024:
                return None
            seg_head = seg_res["data"][:256].lstrip().lower()
            if seg_head.startswith((b"<html", b"<!doctype", b"error", b"404")):
                return None

            speed_kbs = (seg_res["bytes"] / 1024.0) / seg_res["elapsed"]
            latency_ms = int((res["ttfb"] + seg_res["ttfb"]) * 1000)
        else:
            # Direct MPEG-TS / FLV / progressive video stream
            if res["bytes"] < 24 * 1024:
                return None
            speed_kbs = (res["bytes"] / 1024.0) / res["elapsed"]
            latency_ms = int(res["ttfb"] * 1000)

        return {
            **item,
            "speed_kbs": round(speed_kbs, 1),
            "latency_ms": latency_ms,
            "is_v6": is_ipv6_url(url),
        }
    except Exception:
        return None


def channel_sort_key(group: str, name: str):
    if group == "央视":
        if name == "CCTV-5+":
            return (0, 5.5, name)
        m = re.match(r"^CCTV-(\d+)$", name)
        if m:
            return (0, float(m.group(1)), name)
        if name == "CCTV-4K":
            return (0, 20.0, name)
        if name == "CCTV-8K":
            return (0, 21.0, name)
        if name == "CCTV-16-4K":
            return (0, 16.5, name)
        return (1, 99.0, name)
    if group == "卫视":
        try:
            return (0, float(WEISHI_NAMES.index(name)), name)
        except ValueError:
            return (1, 999.0, name)
    return (0, 0.0, name)


def route_score(r: dict) -> float:
    """Higher is better: prioritize high throughput and low startup latency."""
    # Cap speed contribution so a 20MB/s link with 3s latency doesn't beat a 3MB/s link with 150ms latency
    eff_speed = min(r["speed_kbs"], 3000.0)
    latency_penalty = r["latency_ms"] * 0.6
    # Slight preference for IPv4 because Clash TUN on TV handles IPv4 most reliably
    v4_bonus = 200.0 if not r["is_v6"] else 0.0
    return eff_speed - latency_penalty + v4_bonus


def make_extinf(item: dict, group: str) -> str:
    attrs = [f'tvg-name="{item["name"]}"']
    if item["tvg_id"]:
        attrs.append(f'tvg-id="{item["tvg_id"]}"')
    if item["logo"]:
        attrs.append(f'tvg-logo="{item["logo"]}"')
    attrs.append(f'group-title="{group}"')
    return f'#EXTINF:-1 {" ".join(attrs)},{item["name"]}'


def main():
    v6_ok = has_ipv6()
    print(f"Environment IPv6 reachable: {v6_ok}")

    seen_urls = set()
    # candidates[group][canonical_name] = [item, ...]
    candidates = {
        "央视": {},
        "卫视": {},
        "BBC & 英国频道": {},
        "国际新闻": {},
    }

    source_ch_counts = {}

    def add_candidate(group_name: str, canon_name: str, item: dict, source_key: str = ""):
        u = item["url"]
        if not u.startswith(("http://", "https://")) or u in seen_urls:
            return
        if not v6_ok and is_ipv6_url(u):
            return
        sc_key = (source_key, group_name, canon_name)
        if source_ch_counts.get(sc_key, 0) >= MAX_PER_SOURCE_PER_CHANNEL:
            return
        bucket = candidates[group_name].setdefault(canon_name, [])
        seen_urls.add(u)
        source_ch_counts[sc_key] = source_ch_counts.get(sc_key, 0) + 1
        new_item = dict(item)
        new_item["name"] = canon_name
        if not new_item["tvg_id"]:
            new_item["tvg_id"] = canon_name
        bucket.append(new_item)

    # 1. Fetch Chinese CCTV & Satellite sources in parallel
    cn_keys = list(CN_IPV4_SOURCES) + (list(CN_IPV6_SOURCES) if v6_ok else [])
    fetched_texts = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(SOURCES)) as ex:
        fut_map = {ex.submit(fetch, SOURCES[k], 15.0): k for k in list(cn_keys) + ["uk", "news"]}
        for fut in concurrent.futures.as_completed(fut_map):
            k = fut_map[fut]
            try:
                txt = fut.result()
                fetched_texts[k] = txt
                print(f"[ok] fetched {k}: {len(txt)} bytes")
            except Exception as e:
                print(f"[warn] {k} failed: {e}")

    for k in cn_keys:
        txt = fetched_texts.get(k)
        if not txt:
            continue
        for item in parse_m3u(txt):
            target, canon = normalize_cn_channel(item["name"], item["group"])
            if target and canon:
                add_candidate(target, canon, item, k)

    # 2. UK / BBC channels
    if "uk" in fetched_texts:
        for item in parse_m3u(fetched_texts["uk"]):
            low = item["name"].lower()
            if any(w in low for w in ("bbc", "itv", "sky", "gb news", "channel 4")):
                add_candidate("BBC & 英国频道", item["name"], item, "uk")

    # 3. International News
    if "news" in fetched_texts:
        for item in parse_m3u(fetched_texts["news"]):
            low = item["name"].lower()
            if any(w in low for w in INTL_NEWS_KEYWORDS):
                grp = "BBC & 英国频道" if "bbc" in low else "国际新闻"
                add_candidate(grp, item["name"], item, "news")

    # Flatten all candidates for concurrent real-stream probing
    all_tasks = []
    for grp, ch_map in candidates.items():
        for ch_name, items in ch_map.items():
            for it in items:
                all_tasks.append((grp, ch_name, it))

    print(f"Probing real stream playback & speed for {len(all_tasks)} candidates...")
    t_start = time.monotonic()

    verified = {k: {} for k in candidates}

    def _run(task):
        grp, ch_name, it = task
        res = probe_stream(it)
        return (grp, ch_name, res) if res else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as ex:
        for out in ex.map(_run, all_tasks):
            if out:
                grp, ch_name, res = out
                verified[grp].setdefault(ch_name, []).append(res)

    print(f"Probing finished in {time.monotonic() - t_start:.1f}s")

    # Select best routes per channel
    out_lines = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']
    summary = {}
    sample_stats = []

    for grp in ("央视", "卫视", "BBC & 英国频道", "国际新闻"):
        ch_map = verified[grp]
        sorted_names = sorted(ch_map.keys(), key=lambda n: channel_sort_key(grp, n))
        kept_channels = 0
        kept_routes = 0

        for ch_name in sorted_names:
            routes = ch_map[ch_name]
            # Prefer routes meeting MIN_SPEED_KBS; fall back to FALLBACK_SPEED_KBS if none
            fast = [r for r in routes if r["speed_kbs"] >= MIN_SPEED_KBS]
            if not fast:
                fast = [r for r in routes if r["speed_kbs"] >= FALLBACK_SPEED_KBS]
            if not fast:
                continue

            # Rank by score (speed + low latency + IPv4 preference)
            fast.sort(key=route_score, reverse=True)
            best = fast[:MAX_ROUTES_PER_CHANNEL]

            # Share the best available logo across routes of the same channel
            best_logo = next((r["logo"] for r in best if r["logo"]), "")
            for r in best:
                if not r["logo"] and best_logo:
                    r["logo"] = best_logo
                out_lines.append(make_extinf(r, grp))
                out_lines.append(r["url"])

            kept_channels += 1
            kept_routes += len(best)
            if len(sample_stats) < 15 and grp in ("央视", "卫视"):
                top = best[0]
                sample_stats.append(
                    f"  {ch_name}: {len(best)} 条线路 (最优 {top['speed_kbs']:.0f} KB/s, {top['latency_ms']}ms, {'IPv6' if top['is_v6'] else 'IPv4'})"
                )

        summary[grp] = (kept_channels, kept_routes)

    OUT_FILE.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print(f"\nWrote {OUT_FILE}")
    for grp, (chs, rts) in summary.items():
        print(f"  {grp}: {chs} 个频道, {rts} 条测速可用线路")
    if sample_stats:
        print("\n抽样频道最优线路:")
        for s in sample_stats:
            print(s)


if __name__ == "__main__":
    main()

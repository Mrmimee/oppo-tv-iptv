#!/usr/bin/env python3
"""Fetch, verify, and merge CCTV, Chinese Satellite TV, BBC & International News M3U playlists."""

import concurrent.futures
import re
import urllib.request
from pathlib import Path

OUT_FILE = Path(__file__).parent / "tv_channels.m3u"

SOURCES = {
    "guovin": "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
    "uk": "https://iptv-org.github.io/iptv/countries/uk.m3u",
    "news": "https://iptv-org.github.io/iptv/categories/news.m3u",
}

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


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
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


def is_alive(item: dict) -> bool:
    url = item["url"]
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3.5) as resp:
            return 200 <= resp.status < 400
    except Exception:
        return False


def make_extinf(item: dict, group: str) -> str:
    attrs = [f'tvg-name="{item["name"]}"']
    if item["tvg_id"]:
        attrs.append(f'tvg-id="{item["tvg_id"]}"')
    if item["logo"]:
        attrs.append(f'tvg-logo="{item["logo"]}"')
    attrs.append(f'group-title="{group}"')
    return f'#EXTINF:-1 {" ".join(attrs)},{item["name"]}'


def main():
    seen_urls = set()
    candidates = {
        "央视": [],
        "卫视": [],
        "BBC & 英国频道": [],
        "国际新闻": [],
    }

    def add_candidate(group_name: str, item: dict):
        u = item["url"]
        if not u.startswith(("http://", "https://")) or u in seen_urls:
            return
        seen_urls.add(u)
        candidates[group_name].append(item)

    # 1. Guovin iptv-api (daily speed-tested CCTV & Satellite)
    channel_counts = {}
    try:
        for item in parse_m3u(fetch(SOURCES["guovin"])):
            g, n = item["group"], item["name"]
            target = None
            if "央视" in g or n.upper().startswith("CCTV"):
                target = "央视"
            elif "卫视" in g or "卫视" in n:
                target = "卫视"
            if target:
                key = (target, n.upper())
                if channel_counts.get(key, 0) < 4:
                    add_candidate(target, item)
                    channel_counts[key] = channel_counts.get(key, 0) + 1
    except Exception as e:
        print(f"[warn] guovin failed: {e}")

    # 2. UK / BBC channels
    try:
        for item in parse_m3u(fetch(SOURCES["uk"])):
            low = item["name"].lower()
            if "bbc" in low or "itv" in low or "sky" in low or "gb news" in low or "channel 4" in low:
                add_candidate("BBC & 英国频道", item)
    except Exception as e:
        print(f"[warn] uk failed: {e}")

    # 3. International News
    try:
        for item in parse_m3u(fetch(SOURCES["news"])):
            low = item["name"].lower()
            if any(k in low for k in INTL_NEWS_KEYWORDS):
                if "bbc" in low:
                    add_candidate("BBC & 英国频道", item)
                else:
                    add_candidate("国际新闻", item)
    except Exception as e:
        print(f"[warn] news failed: {e}")

    # 4. Concurrent liveness check to remove 403 / dead / timeout streams
    all_items = [(grp, item) for grp, items in candidates.items() for item in items]
    print(f"Checking liveness of {len(all_items)} candidate streams...")

    def check_pair(pair):
        grp, item = pair
        return (grp, item) if is_alive(item) else None

    groups = {k: [] for k in candidates}
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
        for res in ex.map(check_pair, all_items):
            if res:
                grp, item = res
                groups[grp].append(item)

    out_lines = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']
    for grp, items in groups.items():
        for item in items:
            out_lines.append(make_extinf(item, grp))
            out_lines.append(item["url"])

    OUT_FILE.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_FILE}")
    for grp, items in groups.items():
        print(f"  {grp}: {len(items)} live streams")


if __name__ == "__main__":
    main()

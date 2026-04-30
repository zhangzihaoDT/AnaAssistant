import argparse
import html
import json
import random
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote


def _require_playwright() -> None:
    try:
        import playwright  # noqa: F401
    except Exception:
        raise RuntimeError(
            "缺少依赖：playwright。请先执行：pip install playwright && playwright install chromium"
        )


def _build_url(keyword: str) -> str:
    return f"https://www.xiaohongshu.com/search_result?keyword={quote(keyword)}&source=web_explore_feed"


@dataclass(frozen=True)
class NoteSample:
    note_id: Optional[str]
    url: str
    text: str

    def to_dict(self) -> Dict[str, Any]:
        return {"note_id": self.note_id, "url": self.url, "text": self.text}


def _walk(obj: Any) -> Iterable[Any]:
    stack = [obj]
    while stack:
        cur = stack.pop()
        yield cur
        if isinstance(cur, dict):
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                stack.append(v)


def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > 200:
        s = s[:200].strip()
    return s


def _title_from_mixed_line(s: str) -> str:
    s = _clean_text(s)
    if not s:
        return ""

    tokens = [t for t in s.split(" ") if t]
    if not tokens:
        return ""

    def is_like_token(x: str) -> bool:
        return x in {"赞"} or bool(re.fullmatch(r"[\d\.]+万?", x))

    def is_time_token(x: str) -> bool:
        return bool(
            re.fullmatch(
                r"(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}|\d+天前|昨天|今天|前天|刚刚|\d{1,2}:\d{2})",
                x,
            )
        )

    def looks_mixed(parts: List[str]) -> bool:
        if not parts:
            return False
        if is_like_token(parts[-1]) or is_time_token(parts[-1]):
            return True
        if len(parts) >= 2 and (is_like_token(parts[-2]) or is_time_token(parts[-2])):
            return True
        return False

    while tokens and is_like_token(tokens[-1]):
        tokens.pop()
    while tokens and is_time_token(tokens[-1]):
        tokens.pop()

    if len(tokens) >= 2:
        title = " ".join(tokens[:-1]).strip()
        return title

    return tokens[0]


def _guess_text_from_node(node: Dict[str, Any]) -> str:
    for k in (
        "title",
        "name",
        "desc",
        "description",
        "content",
        "noteTitle",
        "noteDesc",
        "displayTitle",
        "displayName",
    ):
        v = node.get(k)
        if isinstance(v, str):
            v = _clean_text(v)
            if v:
                return v

    for k, v in node.items():
        if not isinstance(v, str):
            continue
        if k.lower() in {"href", "url", "link", "image", "img", "cover", "avatar"}:
            continue
        v2 = _clean_text(v)
        if 4 <= len(v2) <= 80 and not v2.startswith("http"):
            return v2

    return ""


def _looks_like_title(s: str) -> bool:
    if not s:
        return False
    if re.fullmatch(r"\d+", s):
        return False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return False
    if re.fullmatch(r"\d{2}-\d{2}", s):
        return False
    if "验证码" in s or "安全验证" in s or "登录" in s:
        return False
    if "小红书" in s or "RED" in s.upper():
        return False
    return True


def _normalize_key(s: str) -> str:
    return re.sub(r"[\s\-_]+", "", s).lower()


def _best_title_from_subtree(obj: Any) -> str:
    keys = (
        "title",
        "noteTitle",
        "displayTitle",
        "desc",
        "noteDesc",
        "description",
        "content",
    )
    cands: List[str] = []
    for node in _walk(obj):
        if not isinstance(node, dict):
            continue
        for k in keys:
            v = node.get(k)
            if not isinstance(v, str):
                continue
            v2 = _clean_text(v)
            if v2 and _looks_like_title(v2):
                cands.append(v2)

    if not cands:
        return ""
    cands = list(dict.fromkeys(cands))
    cands.sort(key=lambda x: (len(x), x))
    return _title_from_mixed_line(cands[-1])


def _extract_title_from_next_data(html_text: str, note_id: Optional[str]) -> str:
    m = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html_text,
        flags=re.DOTALL,
    )
    if not m:
        return ""
    raw = html.unescape(m.group(1)).strip()
    if not raw:
        return ""
    try:
        data = json.loads(raw)
    except Exception:
        return ""

    if note_id:
        for node in _walk(data):
            if not isinstance(node, dict):
                continue
            nid = node.get("noteId") or node.get("id")
            if nid == note_id:
                t = _best_title_from_subtree(node)
                if t:
                    return t

    for node in _walk(data):
        if not isinstance(node, dict):
            continue
        t = _guess_text_from_node(node)
        if not t:
            continue
        if len(t) < 4 or not _looks_like_title(t):
            continue
        return _title_from_mixed_line(t)

    return ""



def _extract_candidates_from_next_data(html_text: str) -> List[NoteSample]:
    m = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html_text,
        flags=re.DOTALL,
    )
    if not m:
        return []
    raw = html.unescape(m.group(1)).strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []

    href_re = re.compile(r"^/explore/([0-9a-fA-F]+)")
    candidates: Dict[str, NoteSample] = {}

    for node in _walk(data):
        if not isinstance(node, dict):
            continue
        href = node.get("href") or node.get("url") or node.get("link")
        if isinstance(href, str):
            m2 = href_re.search(href)
            if m2:
                note_id = m2.group(1)
                text = _best_title_from_subtree(node) or _guess_text_from_node(node)
                full_url = (
                    href
                    if href.startswith("http")
                    else f"https://www.xiaohongshu.com{href}"
                )
                if full_url not in candidates:
                    candidates[full_url] = NoteSample(note_id=note_id, url=full_url, text=text)

        if "noteId" in node or "id" in node:
            note_id = node.get("noteId") or node.get("id")
            if isinstance(note_id, str) and re.fullmatch(r"[0-9a-fA-F]{8,}", note_id):
                title = _best_title_from_subtree(node) or _guess_text_from_node(node)
                full_url = f"https://www.xiaohongshu.com/explore/{note_id}"
                if full_url not in candidates:
                    candidates[full_url] = NoteSample(
                        note_id=note_id, url=full_url, text=title
                    )

    return list(candidates.values())


def _extract_candidates_from_dom(page: Any) -> List[NoteSample]:
    items = page.eval_on_selector_all(
        'a[href^="/explore/"]',
        """els => els.map(el => ({
            href: el.getAttribute("href") || "",
            text: ((el.innerText || el.textContent || "")).slice(0, 300),
            title: (el.getAttribute("title") || ""),
            ariaLabel: (el.getAttribute("aria-label") || ""),
            imgAlt: ((el.querySelector("img") && el.querySelector("img").getAttribute("alt")) || ""),
            parentText: ((el.parentElement && (el.parentElement.innerText || el.parentElement.textContent)) || "").slice(0, 300)
        }))""",
    )
    href_re = re.compile(r"^/explore/([0-9a-fA-F]+)")
    out: Dict[str, NoteSample] = {}
    for it in items or []:
        if not isinstance(it, dict):
            continue
        href = it.get("href")
        if not isinstance(href, str):
            continue
        m = href_re.search(href)
        if not m:
            continue
        note_id = m.group(1)
        url = f"https://www.xiaohongshu.com{href}"
        text_parts: List[str] = []
        for k in ("text", "title", "ariaLabel", "imgAlt", "parentText"):
            v = it.get(k)
            if isinstance(v, str):
                v2 = _clean_text(v)
                if v2 and v2 not in text_parts:
                    text_parts.append(v2)
        text = _title_from_mixed_line(text_parts[0]) if text_parts else ""
        if url not in out:
            out[url] = NoteSample(note_id=note_id, url=url, text=text)
    return list(out.values())


def _sample(items: List[NoteSample], n: int, seed: Optional[int]) -> List[NoteSample]:
    if n <= 0:
        return []
    if len(items) <= n:
        return items
    rng = random.Random(seed)
    return rng.sample(items, n)


def _resolve_title_from_note_page(
    context: Any, url: str, note_id: Optional[str], timeout_ms: int
) -> str:
    page = context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=max(1000, timeout_ms // 2))
        except Exception:
            pass

        html_text = page.content()
        next_title = _extract_title_from_next_data(html_text, note_id)
        if next_title:
            return _title_from_mixed_line(next_title)

        try:
            meta_title = page.eval_on_selector(
                'meta[property="og:title"], meta[name="og:title"]',
                "el => el.getAttribute('content') || ''",
            )
        except Exception:
            meta_title = ""
        if isinstance(meta_title, str):
            meta_title = _clean_text(meta_title)
            if meta_title:
                return _title_from_mixed_line(meta_title)

        try:
            t = page.title()
        except Exception:
            t = ""
        if isinstance(t, str):
            t = _clean_text(t)
            if t:
                return _title_from_mixed_line(t)

        try:
            dom_title = page.eval_on_selector(
                "h1, h2, [data-testid*='title' i]",
                "el => (el.innerText || el.textContent || '')",
            )
        except Exception:
            dom_title = ""
        if isinstance(dom_title, str):
            dom_title = _clean_text(dom_title)
            if dom_title:
                return _title_from_mixed_line(dom_title)

        return ""
    finally:
        page.close()


def main(argv: Optional[List[str]] = None) -> int:
    _require_playwright()
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    p = argparse.ArgumentParser()
    p.add_argument("--keyword", default="智己ls8")
    p.add_argument("--n", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scroll-times", type=int, default=6)
    p.add_argument("--scroll-px", type=int, default=2200)
    p.add_argument("--min-wait-ms", type=int, default=900)
    p.add_argument("--max-wait-ms", type=int, default=1800)
    p.add_argument("--no-new-rounds-stop", type=int, default=6)
    p.add_argument("--max-candidates", type=int, default=400)
    p.add_argument("--timeout-ms", type=int, default=30000)
    p.add_argument("--headed", action="store_true")
    p.add_argument("--user-data-dir", default="")
    p.add_argument("--only-ls8", action="store_true")
    p.add_argument("--title-regex", default="")
    p.add_argument("--resolve-detail", action="store_true")
    p.add_argument("--force-detail", action="store_true")
    p.add_argument("--max-detail-pages", type=int, default=60)
    p.add_argument("--detail-timeout-ms", type=int, default=20000)
    p.add_argument("--debug-dir", default="")
    p.add_argument("--out", default="")
    args = p.parse_args(argv)

    url = _build_url(args.keyword)
    rng = random.Random(args.seed)
    title_pattern = args.title_regex.strip()
    if args.only_ls8 and not title_pattern:
        title_pattern = r"(?i)ls\s*-?\s*8"
    title_re = re.compile(title_pattern) if title_pattern else None

    with sync_playwright() as pw:
        if args.user_data_dir:
            context = pw.chromium.launch_persistent_context(
                args.user_data_dir,
                headless=not args.headed,
                locale="zh-CN",
                viewport={"width": 1280, "height": 900},
            )
            page = context.pages[0] if context.pages else context.new_page()
        else:
            browser = pw.chromium.launch(headless=not args.headed)
            context = browser.new_context(
                locale="zh-CN",
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=args.timeout_ms)
        except PlaywrightTimeoutError:
            pass

        debug_dir = Path(args.debug_dir) if args.debug_dir else None
        if debug_dir:
            debug_dir.mkdir(parents=True, exist_ok=True)
        candidates_map: Dict[str, NoteSample] = {}
        no_new_rounds = 0
        for i in range(max(1, max(0, args.scroll_times) + 1)):
            html_text = page.content()
            if debug_dir and i == 0:
                (debug_dir / "search.html").write_text(html_text, encoding="utf-8")

            batch = _extract_candidates_from_next_data(html_text)
            if not batch:
                batch = _extract_candidates_from_dom(page)

            before = len(candidates_map)
            for c in batch:
                candidates_map.setdefault(c.url, c)
            after = len(candidates_map)

            if args.max_candidates > 0 and after >= args.max_candidates:
                break

            if after == before:
                no_new_rounds += 1
            else:
                no_new_rounds = 0

            if i >= max(0, args.scroll_times):
                break
            if args.no_new_rounds_stop > 0 and no_new_rounds >= args.no_new_rounds_stop:
                break

            page.mouse.wheel(0, max(400, args.scroll_px))
            page.wait_for_timeout(rng.randint(args.min_wait_ms, args.max_wait_ms))

        candidates = list(candidates_map.values())
        candidates.sort(key=lambda x: x.url)

        def is_match_title(t: str) -> bool:
            if not title_re:
                return True
            return bool(title_re.search(t) or title_re.search(_normalize_key(t)))

        filtered: List[NoteSample] = []
        unknown: List[NoteSample] = []
        for it in candidates:
            cleaned = _title_from_mixed_line(it.text) if it.text else ""
            if cleaned and is_match_title(cleaned):
                filtered.append(NoteSample(note_id=it.note_id, url=it.url, text=cleaned))
            elif args.resolve_detail and not cleaned:
                unknown.append(NoteSample(note_id=it.note_id, url=it.url, text=""))

        selected = filtered
        if args.n > 0 and len(selected) > args.n:
            selected = rng.sample(selected, args.n)

        if args.resolve_detail:
            resolved: List[NoteSample] = []
            budget = args.max_detail_pages if args.max_detail_pages > 0 else len(selected)
            opened = 0
            for idx, it in enumerate(selected):
                if idx < budget and (args.force_detail or not it.text or len(it.text) < 4):
                    title = _resolve_title_from_note_page(
                        context, it.url, it.note_id, args.detail_timeout_ms
                    )
                    opened += 1
                    if title and is_match_title(title):
                        resolved.append(NoteSample(note_id=it.note_id, url=it.url, text=title))
                    else:
                        resolved.append(it)
                    page.wait_for_timeout(rng.randint(args.min_wait_ms, args.max_wait_ms))
                else:
                    resolved.append(it)
            selected = resolved

            if title_re and unknown:
                remaining_budget = max(
                    0, (args.max_detail_pages if args.max_detail_pages > 0 else 0) - opened
                )
                if remaining_budget <= 0:
                    unknown = []
                for idx, it in enumerate(unknown):
                    if idx >= remaining_budget:
                        break
                    title = _resolve_title_from_note_page(
                        context, it.url, it.note_id, args.detail_timeout_ms
                    )
                    if title and is_match_title(title):
                        selected.append(NoteSample(note_id=it.note_id, url=it.url, text=title))
                    page.wait_for_timeout(rng.randint(args.min_wait_ms, args.max_wait_ms))

        payload = {
            "keyword": args.keyword,
            "url": url,
            "total_candidates": len(candidates),
            "title_regex": title_pattern or None,
            "matched_candidates": len(filtered),
            "sampled": [x.to_dict() for x in selected],
        }
        text_out = json.dumps(payload, ensure_ascii=False, indent=2)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(text_out + "\n")
        else:
            sys.stdout.write(text_out + "\n")

        context.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

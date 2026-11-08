#!/usr/bin/env python3
"""Build the static GitHub Pages site for aydb.

The site has three parts:

* index.html: the repository README rendered to HTML.
* docs/: the Markdown files under docs/ rendered to HTML, with the
  figures from docs/assets copied next to them.
* results.html: test and benchmark results collected by the CI workflow,
  with bar charts rendered as plain HTML and CSS.
* charts/*.svg: the SQLite comparison charts as images, embedded by the README.

Dependencies: Python 3 and the "markdown" package (pip install markdown).

Expected layout of the artifacts directory (what actions/download-artifact
produces when no artifact name is given, one sub-directory per artifact):

    <artifacts>/test-<os>/{clippy,tests,reliability}.txt
    <artifacts>/bench-<os>/bench/<name>.txt
    <artifacts>/bench-<os>/bench/status.txt   ("<name> exit=<n> seconds=<n>")

Missing files are reported as "no result"; they never abort the build.
"""

import argparse
import html
import math
import posixpath
import re
import shutil
import sys
import textwrap
from pathlib import Path

import markdown

OS_NAMES = ["windows-latest", "ubuntu-latest", "macos-latest"]

# Benchmarks in display order; the first two also feed the comparison tables.
BENCHES = [
    "standard_sql_bench",
    "sqlite_vs_aydb_bench",
    "concurrent_read_bench",
    "concurrent_write_bench",
    "tx_isolation_read_bench",
    "compaction_latency_bench",
    "pal_bench",
    "meta_wal_bench",
    "wisckey_bench",
    "lru_engine_bench",
]
COMPARED = ["standard_sql_bench", "sqlite_vs_aydb_bench"]

MERMAID_VERSION = "10.9.1"

CSS = """
:root {
  --bg: #ffffff; --fg: #1f2328; --muted: #59636e; --border: #d1d9e0;
  --accent: #0969da; --code-bg: #f6f8fa; --ok: #1a7f37; --bad: #cf222e;
  --warn: #9a6700; --row: #f6f8fa;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --border: #3d444d;
    --accent: #4493f8; --code-bg: #151b23; --ok: #3fb950; --bad: #f85149;
    --warn: #d29922; --row: #151b23;
  }
}
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0; background: var(--bg); color: var(--fg);
  font: 16px/1.6 system-ui, -apple-system, "Segoe UI", Roboto, Helvetica,
        Arial, sans-serif;
}
a { color: var(--accent); }
nav.site {
  display: flex; flex-wrap: wrap; gap: 4px 20px; padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}
nav.site a { text-decoration: none; font-weight: 600; }
nav.site a.current { color: var(--fg); }
main { max-width: 960px; margin: 0 auto; padding: 8px 16px 48px; }
h1, h2, h3, h4 { line-height: 1.25; margin: 1.6em 0 0.6em; }
h1 { font-size: 1.9em; border-bottom: 1px solid var(--border);
     padding-bottom: 0.3em; }
h2 { font-size: 1.45em; border-bottom: 1px solid var(--border);
     padding-bottom: 0.25em; }
h3 { font-size: 1.2em; }
img { max-width: 100%; height: auto; }
code, pre { font: 0.875em/1.45 ui-monospace, SFMono-Regular, Menlo,
            Consolas, "Liberation Mono", monospace; }
code { background: var(--code-bg); padding: 0.15em 0.35em;
       border-radius: 4px; }
pre { background: var(--code-bg); padding: 12px 14px; border-radius: 6px;
      overflow-x: auto; max-width: 100%; }
pre code { background: none; padding: 0; }
blockquote { margin: 1em 0; padding: 0 1em; color: var(--muted);
             border-left: 4px solid var(--border); }
hr { border: 0; border-top: 1px solid var(--border); margin: 2em 0; }
.table-wrap { overflow-x: auto; max-width: 100%; margin: 1em 0; }
table { border-collapse: collapse; }
th, td { border: 1px solid var(--border); padding: 6px 12px;
         text-align: left; vertical-align: top; }
tr:nth-child(even) td { background: var(--row); }
th { background: var(--code-bg); }
td.num { text-align: right; white-space: nowrap; }
.ok { color: var(--ok); font-weight: 600; }
.bad { color: var(--bad); font-weight: 600; }
.none { color: var(--muted); font-style: italic; }
.meta { color: var(--muted); }
.note { border: 1px solid var(--border); border-left: 4px solid var(--warn);
        border-radius: 6px; padding: 8px 16px; margin: 1em 0; }
details { margin: 0.5em 0; }
details > summary { cursor: pointer; font-weight: 600; }
details details { margin-left: 1em; }
pre.raw { max-height: 32em; overflow: auto; }
"""

CHART_CSS = """
:root { --s0: #2a78d6; --s1: #eb6834; --s2: #1baf7a; }
@media (prefers-color-scheme: dark) {
  :root { --s0: #3987e5; --s1: #d95926; --s2: #199e70; }
}
figure.chart { margin: 1.5em 0 2em; }
figure.chart figcaption { font-weight: 600; line-height: 1.35; }
figure.chart figcaption span { display: block; font-weight: 400;
                               font-size: 0.875em; color: var(--muted); }
.c-legend { display: flex; flex-wrap: wrap; gap: 2px 16px; margin: 8px 0;
            font-size: 0.875em; color: var(--muted); }
.c-legend i { display: inline-block; width: 10px; height: 10px;
              border-radius: 2px; margin-right: 6px; }
.c-row, .c-axis { display: grid; column-gap: 12px;
                  grid-template-columns: minmax(0, 36%) minmax(0, 1fr); }
.c-row { border-radius: 4px; }
.c-row:hover, .c-row:focus-visible { background: var(--row); }
.c-row:focus-visible { outline: 2px solid var(--accent); outline-offset: -2px; }
.c-label { align-self: center; font-size: 0.875em; line-height: 1.3;
           padding: 4px 0 4px 4px; }
.c-plot { position: relative; padding: 5px 0; }
.c-grid { position: absolute; top: 0; bottom: 0; width: 1px;
          background: var(--border); }
.c-grid.base { background: var(--muted); }
.c-track { position: relative; height: 10px; margin-top: 2px; }
.c-track:first-of-type { margin-top: 0; }
.c-bar { position: absolute; top: 0; bottom: 0; min-width: 2px; }
.c-bar.up { border-radius: 0 4px 4px 0; }
.c-bar.down { border-radius: 4px 0 0 4px; }
.c-bar.s0, .c-legend i.s0 { background: var(--s0); }
.c-bar.s1, .c-legend i.s1 { background: var(--s1); }
.c-bar.s2, .c-legend i.s2 { background: var(--s2); }
.c-ticks { position: relative; height: 1.6em; font-size: 0.75em;
           color: var(--muted); font-variant-numeric: tabular-nums; }
.c-ticks span { position: absolute; top: 2px; transform: translateX(-50%);
                white-space: nowrap; }
.c-ticks span.first { transform: none; }
.c-ticks span.last { transform: translateX(-100%); }
.c-tip { position: fixed; z-index: 10; max-width: min(420px, 92vw);
         padding: 6px 10px; border-radius: 6px; pointer-events: none;
         background: var(--bg); color: var(--fg);
         border: 1px solid var(--border); font-size: 0.8125em;
         white-space: pre-line; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.25); }
@media (max-width: 640px) {
  .c-row, .c-axis { grid-template-columns: minmax(0, 1fr); }
  .c-axis .c-label { display: none; }
  .c-label { padding-bottom: 0; }
}
"""

CHART_SCRIPT = """<script>
(() => {
  const tip = document.createElement("div");
  tip.className = "c-tip";
  tip.hidden = true;
  document.body.append(tip);
  const hide = () => { tip.hidden = true; };
  const show = (event) => {
    const row = event.target.closest(".c-row[data-tip]");
    if (!row) { hide(); return; }
    tip.textContent = row.dataset.tip;
    tip.hidden = false;
    const box = row.getBoundingClientRect();
    const size = tip.getBoundingClientRect();
    const left = Math.max(8, Math.min(box.right - size.width,
                                      innerWidth - size.width - 8));
    const above = box.top - size.height - 6;
    const top = above >= 8 ? above : box.bottom + 6;
    tip.style.left = left + "px";
    tip.style.top = top + "px";
  };
  document.addEventListener("pointerover", show);
  document.addEventListener("focusin", show);
  document.addEventListener("focusout", hide);
  document.addEventListener("scroll", hide, true);
})();
</script>"""

NAV_ITEMS = [
    ("overview", "Overview", "index.html"),
    ("results", "Results", "results.html"),
    ("docs", "Docs", "docs/index.html"),
]


# --------------------------------------------------------------------------
# HTML helpers
# --------------------------------------------------------------------------

def esc(text):
    return html.escape(str(text), quote=True)


def page(title, body, root, current, extra_head=""):
    """Wrap a body fragment in the shared layout. `root` is the relative
    path from the page to the site root ("" or "../" ...)."""
    nav = "".join(
        '<a href="{}{}"{}>{}</a>'.format(
            root, href, ' class="current"' if key == current else "", label
        )
        for key, label, href in NAV_ITEMS
    )
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        "<title>{}</title>\n<style>{}</style>\n{}</head>\n<body>\n"
        '<nav class="site">{}</nav>\n<main>\n{}\n</main>\n</body>\n</html>\n'
    ).format(esc(title), CSS, extra_head, nav, body)


MERMAID_SCRIPT = """<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@%s/dist/mermaid.esm.min.mjs";
const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;
document.querySelectorAll("pre > code.language-mermaid").forEach((code) => {
  const pre = code.parentElement;
  const div = document.createElement("pre");
  div.className = "mermaid";
  div.textContent = code.textContent;
  pre.replaceWith(div);
});
mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default" });
await mermaid.run({ querySelector: "pre.mermaid" });
</script>
""" % MERMAID_VERSION


def wrap_tables(fragment):
    """Put every table in a horizontally scrolling container."""
    fragment = re.sub(r"<table\b", '<div class="table-wrap"><table', fragment)
    return fragment.replace("</table>", "</table></div>")


# --------------------------------------------------------------------------
# Markdown rendering and link rewriting
# --------------------------------------------------------------------------

def github_slug(value, separator):
    """Heading anchors compatible with GitHub: lower case, drop punctuation,
    one hyphen per space."""
    value = value.strip().lower()
    value = re.sub(r"[^\w\- ]", "", value)
    return value.replace(" ", separator)


def render_markdown(text):
    md = markdown.Markdown(
        extensions=["tables", "fenced_code", "sane_lists", "toc"],
        extension_configs={"toc": {"slugify": github_slug}},
    )
    return md.convert(text)


def first_heading(text, fallback):
    match = re.search(r"^#\s+(.+?)\s*$", text, re.M)
    if not match:
        return fallback
    return re.sub(r"[`*_]", "", match.group(1))


class LinkRewriter:
    """Rewrites repo-relative href/src values found in rendered HTML.

    Links to Markdown files under docs/ (and README.md) point to the
    rendered pages, links into docs/assets stay relative to the copied
    assets, and anything else points to the GitHub blob (or tree) URL.
    """

    ATTR = re.compile(r'\b(href|src)="([^"]*)"')

    def __init__(self, repo_root, repo_url, branch):
        self.repo_root = repo_root
        self.repo_url = repo_url
        self.branch = branch

    def site_path(self, repo_path):
        """Return the site-relative path for a repo path, or None."""
        if repo_path == "README.md":
            return "index.html"
        if repo_path.startswith("docs/"):
            if repo_path.endswith(".md"):
                return repo_path[:-3] + ".html"
            if repo_path.startswith("docs/assets/"):
                return repo_path
        return None

    def rewrite(self, fragment, source_repo_path):
        """`source_repo_path` is the repo path of the Markdown file the
        fragment came from, e.g. "docs/phase1/00_OVERVIEW.md"."""
        source_dir = posixpath.dirname(source_repo_path)
        page_dir = posixpath.dirname(self.site_path(source_repo_path))

        def replace(match):
            attr, value = match.group(1), match.group(2)
            raw = html.unescape(value)
            if (not raw or raw.startswith("#") or raw.startswith("//")
                    or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", raw)):
                return match.group(0)
            path, sep, fragment_id = raw.partition("#")
            path = path.split("?")[0]
            repo_path = posixpath.normpath(posixpath.join(source_dir, path))
            if repo_path.startswith(".."):
                return match.group(0)
            target = self.site_path(repo_path)
            if target is not None:
                new = posixpath.relpath(target, page_dir or ".")
            else:
                full = self.repo_root / repo_path
                kind = "tree" if full.is_dir() else "blob"
                new = "{}/{}/{}/{}".format(
                    self.repo_url, kind, self.branch, repo_path)
            if sep:
                new += "#" + fragment_id
            return '{}="{}"'.format(attr, esc(new))

        return self.ATTR.sub(replace, fragment)


def build_markdown_pages(repo_root, out, repo_url, branch):
    rewriter = LinkRewriter(repo_root, repo_url, branch)
    doc_pages = []  # (repo_path, title)

    sources = [Path("README.md")]
    docs_dir = repo_root / "docs"
    sources += sorted(
        p.relative_to(repo_root) for p in docs_dir.rglob("*.md")
    )
    for rel in sources:
        repo_path = rel.as_posix()
        text = (repo_root / rel).read_text(encoding="utf-8")
        title = first_heading(text, rel.stem)
        body = render_markdown(text)
        body = rewriter.rewrite(body, repo_path)
        uses_mermaid = "language-mermaid" in body
        body = wrap_tables(body)
        target = out / rewriter.site_path(repo_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        depth = len(Path(rewriter.site_path(repo_path)).parts) - 1
        root = "../" * depth
        is_index = repo_path == "README.md"
        if is_index:
            body = status_banner(root) + body
        html_text = page(
            title, body, root, "overview" if is_index else "docs",
            MERMAID_SCRIPT if uses_mermaid else "",
        )
        target.write_text(html_text, encoding="utf-8")
        if not is_index:
            doc_pages.append((rewriter.site_path(repo_path), title))

    # Copy figures referenced by the documents.
    assets = docs_dir / "assets"
    if assets.is_dir():
        shutil.copytree(assets, out / "docs" / "assets", dirs_exist_ok=True)

    listing = "<h1>Documentation</h1>\n<ul>\n"
    for site_path, title in doc_pages:
        listing += '<li><a href="{}">{}</a> <span class="meta">({})</span></li>\n'.format(
            esc(posixpath.relpath(site_path, "docs")), esc(title),
            esc(site_path[len("docs/"):-len(".html")] + ".md"))
    listing += "</ul>\n"
    (out / "docs").mkdir(exist_ok=True)
    (out / "docs" / "index.html").write_text(
        page("Documentation", listing, "../", "docs"), encoding="utf-8")


def status_banner(root):
    return (
        '<p class="meta">Latest CI measurements: '
        '<a href="{}results.html">test and benchmark results</a>.</p>\n'
    ).format(root)


# --------------------------------------------------------------------------
# Result parsing
# --------------------------------------------------------------------------

def read_text(path):
    """Return the file's text, or None if it is missing or unreadable."""
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace").replace("\r", "")
    except OSError:
        return None


def parse_test_results(text):
    """Sum the `test result:` lines printed by cargo test."""
    passed = failed = ignored = 0
    found = 0
    for match in re.finditer(
            r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored",
            text, re.M):
        found += 1
        passed += int(match.group(2))
        failed += int(match.group(3))
        ignored += int(match.group(4))
    return {"found": found, "passed": passed, "failed": failed,
            "ignored": ignored}


def classify_tests(text):
    """Return (state, detail). state is one of ok, bad, none."""
    if text is None:
        return "none", "no result"
    counts = parse_test_results(text)
    if counts["found"] == 0:
        return "bad", "no test result lines (build or harness failure)"
    detail = "{} passed, {} failed, {} ignored".format(
        counts["passed"], counts["failed"], counts["ignored"])
    if counts["failed"] or re.search(r"^error(\[|:)", text, re.M):
        return "bad", detail
    return "ok", detail


def classify_clippy(text):
    if text is None:
        return "none", "no result"
    if re.search(r"^(error|warning)\b", text, re.M):
        count = len(re.findall(r"^(?:error|warning)(?:\[|:)", text, re.M))
        return "bad", "{} diagnostic(s)".format(count)
    if "Finished" in text:
        return "ok", "no warnings"
    return "bad", "did not finish"


def classify_reliability(text):
    if text is None:
        return "none", "no result"
    if re.search(r"panicked at|^error(\[|:)|\bFAIL\b", text, re.M):
        return "bad", "failures or panic in output"
    rows = len(re.findall(r"\bPASS\b", text))
    if "SUITE" not in text:
        return "bad", "no suite output"
    return "ok", "{} checks passed".format(rows) if rows else "completed"


def parse_bench_status(text):
    """Parse lines like `name exit=0 seconds=12` into a dict."""
    result = {}
    if text is None:
        return result
    for line in text.splitlines():
        match = re.match(r"^(\S+) exit=(\d+) seconds=(\d+)", line)
        if match:
            result[match.group(1)] = (int(match.group(2)), int(match.group(3)))
    return result


def split_row(line):
    return [cell.strip() for cell in line.split("|")]


def parse_tables(text):
    """Extract ASCII tables from benchmark output.

    Returns a list of (section, header_cells, rows) where rows are lists of
    cells and the section is the title found between two `====` lines.
    A table is a header row containing `|`, a dashed separator line, and the
    `|` rows that follow it.
    """
    lines = text.splitlines()
    tables = []
    section = ""
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("====") and i + 1 < len(lines):
            nxt = lines[i + 1]
            # A title sits between two `====` lines; a table header that
            # follows a closing `====` line is not a title.
            closed = any(l.startswith("====") for l in lines[i + 2:i + 4])
            if nxt.strip() and "|" not in nxt and closed:
                section = nxt.strip()
        if ("|" in line and i + 1 < len(lines)
                and re.match(r"^-{10,}\s*$", lines[i + 1])):
            header = split_row(line)
            rows = []
            j = i + 2
            while j < len(lines) and "|" in lines[j]:
                rows.append(split_row(lines[j]))
                j += 1
            tables.append((section, header, rows))
            i = j
            continue
        i += 1
    return tables


# --------------------------------------------------------------------------
# Results page
# --------------------------------------------------------------------------

def state_cell(state, text):
    return '<span class="{}">{}</span>'.format(state, esc(text))


def collect(artifacts):
    """Load all results into {os: {...}}."""
    data = {}
    for os_name in OS_NAMES:
        test_dir = artifacts / ("test-" + os_name)
        bench_dir = artifacts / ("bench-" + os_name) / "bench"
        entry = {
            "test_dir": test_dir.is_dir(),
            "bench_dir": bench_dir.is_dir(),
            "clippy": read_text(test_dir / "clippy.txt"),
            "tests": read_text(test_dir / "tests.txt"),
            "reliability": read_text(test_dir / "reliability.txt"),
            "status": parse_bench_status(read_text(bench_dir / "status.txt")),
            "bench": {},
        }
        for name in BENCHES:
            entry["bench"][name] = read_text(bench_dir / (name + ".txt"))
        data[os_name] = entry
    return data


def os_summary_table(os_name, entry):
    rows = []
    for label, text, classify in (
        ("Clippy (-D warnings)", entry["clippy"], classify_clippy),
        ("cargo test --all-targets", entry["tests"], classify_tests),
        ("Reliability benchmark", entry["reliability"], classify_reliability),
    ):
        state, detail = classify(text)
        rows.append((label, state, detail))
    for name in BENCHES:
        text = entry["bench"][name]
        status = entry["status"].get(name)
        if text is None:
            rows.append((name, "none", "no result"))
        elif status is None:
            rows.append((name, "ok", "output present, exit status not recorded"))
        elif status[0] == 0:
            rows.append((name, "ok", "completed in {} s".format(status[1])))
        else:
            rows.append((name, "bad",
                         "exit status {} after {} s".format(*status)))
    out = ["<h3>{}</h3>".format(esc(os_name))]
    if not entry["test_dir"] and not entry["bench_dir"]:
        out.append('<p class="none">No artifacts were produced for this '
                   "operating system (no result).</p>")
    out.append('<div class="table-wrap"><table><thead><tr><th>Check</th>'
               "<th>Status</th><th>Detail</th></tr></thead><tbody>")
    labels = {"ok": "pass", "bad": "fail", "none": "no result"}
    for label, state, detail in rows:
        out.append("<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>".format(
            esc(label), state_cell(state, labels[state]), esc(detail)))
    out.append("</tbody></table></div>")
    return "\n".join(out)


def merged_tables(data, bench):
    """Merge one benchmark's tables across operating systems.

    Tables are matched by (section, index within section) and kept in the
    order in which they first appear. Returns (order, merged), where
    merged[key]["rows"][label][os_name] holds the cells after the label.
    """
    order = []
    merged = {}
    for os_name in OS_NAMES:
        text = data[os_name]["bench"][bench]
        seen = {}
        for section, header, rows in (parse_tables(text) if text else []):
            index = seen.get(section, 0)
            seen[section] = index + 1
            key = (section, index)
            if key not in merged:
                merged[key] = {"header": header, "rows": {}, "labels": []}
                order.append(key)
            table = merged[key]
            for cells in rows:
                if not cells:
                    continue
                label = cells[0]
                if label not in table["rows"]:
                    table["rows"][label] = {}
                    table["labels"].append(label)
                table["rows"][label][os_name] = cells[1:]
    return order, merged


def comparison_tables(data):
    """Cross-OS tables for the SQLite-vs-aydb benchmarks."""
    out = []
    for bench in COMPARED:
        order, merged = merged_tables(data, bench)
        out.append("<h3><code>{}</code></h3>".format(esc(bench)))
        if not order:
            out.append('<p class="none">No parsable table rows (no result).</p>')
            continue
        for key in order:
            table = merged[key]
            section = key[0] or "(untitled)"
            columns = " / ".join(table["header"][1:])
            out.append("<h4>{}</h4>".format(esc(section)))
            out.append('<p class="meta">Each cell: {}</p>'.format(esc(columns)))
            out.append('<div class="table-wrap"><table><thead><tr><th>Row</th>')
            for os_name in OS_NAMES:
                out.append("<th>{}</th>".format(esc(os_name)))
            out.append("</tr></thead><tbody>")
            for label in table["labels"]:
                out.append("<tr><td>{}</td>".format(esc(label)))
                for os_name in OS_NAMES:
                    cells = table["rows"][label].get(os_name)
                    if cells is None:
                        out.append('<td class="none">no result</td>')
                    else:
                        out.append('<td class="num">{}</td>'.format(
                            esc(" / ".join(cells))))
                out.append("</tr>")
            out.append("</tbody></table></div>")
    return "\n".join(out)


# --------------------------------------------------------------------------
# Charts
# --------------------------------------------------------------------------

def first_number(cell):
    """Return the first number in a table cell, or None."""
    match = re.search(r"\d+(?:\.\d+)?", cell or "")
    return float(match.group()) if match else None


def ratio_scale(values):
    """Log2 axis around 1x. Returns (position, base, ticks, tick_label)."""
    lo_exp = min(-1, math.floor(math.log2(min(values))))
    hi_exp = max(1, math.ceil(math.log2(max(values))))
    span = hi_exp - lo_exp
    step = max(1, math.ceil(span / 6))

    def position(value):
        return (math.log2(value) - lo_exp) / span * 100

    ticks = [2.0 ** e for e in range(lo_exp, hi_exp + 1) if e % step == 0]
    return position, 1.0, ticks, lambda value: "{:g}x".format(value)


def compact_number(value):
    for limit, suffix in ((1e6, "M"), (1e3, "K")):
        if value >= limit:
            return "{:g}{}".format(value / limit, suffix)
    return "{:g}".format(value)


def linear_scale(high):
    """Zero-based axis up to a rounded maximum of at least `high`."""
    magnitude = 10.0 ** math.floor(math.log10(high))
    step = next(m * magnitude for m in (0.2, 0.5, 1, 2, 5, 10)
                if high / (m * magnitude) <= 5)
    top = math.ceil(high / step) * step

    def position(value):
        return value / top * 100

    ticks = [i * step for i in range(int(round(top / step)) + 1)]
    return position, 0.0, ticks, compact_number


def render_chart(title, note, rows, scale):
    """Render grouped horizontal bars, one track per operating system.

    `rows` is a list of (label, {os_name: (value, detail)}); `scale` is the
    result of ratio_scale or linear_scale.
    """
    out = ['<figure class="chart"><figcaption>{}<span>{}</span></figcaption>'
           .format(esc(title), esc(note))]
    out.append('<div class="c-legend">{}</div>'.format("".join(
        '<span><i class="s{}"></i>{}</span>'.format(i, esc(os_name))
        for i, os_name in enumerate(OS_NAMES))))
    position, base, ticks, tick_label = scale
    grid = "".join(
        '<div class="c-grid{}" style="left:{:.3f}%"></div>'.format(
            " base" if tick == base else "", position(tick))
        for tick in ticks)
    for label, values in rows:
        lines = [label]
        tracks = []
        for i, os_name in enumerate(OS_NAMES):
            if os_name not in values:
                lines.append("{}: no result".format(os_name))
                tracks.append('<div class="c-track"></div>')
                continue
            value, detail = values[os_name]
            lines.append("{}: {}".format(os_name, detail))
            start, end = sorted((position(base), position(value)))
            tracks.append(
                '<div class="c-track"><div class="c-bar s{} {}" '
                'style="left:{:.3f}%;width:{:.3f}%"></div></div>'.format(
                    i, "up" if value >= base else "down", start, end - start))
        tip = esc("\n".join(lines))
        out.append(
            '<div class="c-row" tabindex="0" role="img" aria-label="{0}" '
            'data-tip="{0}"><div class="c-label">{1}</div>'
            '<div class="c-plot">{2}{3}</div></div>'.format(
                tip, esc(label), grid, "".join(tracks)))
    tick_spans = []
    for tick in ticks:
        edge = (' class="first"' if position(tick) < 0.5 else
                ' class="last"' if position(tick) > 99.5 else "")
        tick_spans.append('<span{} style="left:{:.3f}%">{}</span>'.format(
            edge, position(tick), esc(tick_label(tick))))
    out.append('<div class="c-axis"><div class="c-label"></div>'
               '<div class="c-ticks">{}</div></div></figure>'.format(
                   "".join(tick_spans)))
    return "".join(out)


def ratio_chart(title, note, rows):
    """Render a ratio chart, or a "no result" line when `rows` has no data."""
    rows = [(label, values) for label, values in rows if values]
    if not rows:
        return '<p class="none">{}: no result</p>'.format(esc(title))
    values = [value for _, per_os in rows for value, _ in per_os.values()]
    return render_chart(title, note, rows, ratio_scale(values))


def reported_ratio_rows(table):
    """Rows of a table whose last column is a reported ratio such as 0.83x."""
    rows = []
    for label in table["labels"]:
        values = {}
        for os_name, cells in table["rows"][label].items():
            ratio = first_number(cells[-1]) if len(cells) >= 2 else None
            if ratio:
                values[os_name] = (ratio, "{:.2f}x ({})".format(
                    ratio, " vs ".join(cells[:-1])))
        rows.append((label, values))
    return rows


def computed_ratio(table, base_prefix, other_prefix):
    """{os_name: (ratio, detail)} from the first column of two table rows.

    The rows are found by label prefix; the ratio is other / base.
    """
    def find(prefix, os_name):
        for label in table["labels"]:
            cells = table["rows"][label].get(os_name)
            if label.startswith(prefix) and cells:
                return cells[0]
        return None

    values = {}
    for os_name in OS_NAMES:
        base_cell = find(base_prefix, os_name)
        other_cell = find(other_prefix, os_name)
        base, other = first_number(base_cell), first_number(other_cell)
        if base and other:
            values[os_name] = (other / base, "{:.2f}x ({} vs {})".format(
                other / base, base_cell, other_cell))
    return values


SQLITE_NOTE = ("aydb relative to SQLite, log scale. Right of 1x: aydb is faster. "
               "Left of 1x: SQLite is faster.")

# Image names are part of the README, so they do not follow the section titles.
SVG_SLUGS = [
    (r"^SUITE 1\b", "speedtest"),
    (r"^SUITE 2\b.*\b256 B\b", "ycsb-256"),
    (r"^SUITE 2\b.*\b2048 B\b", "ycsb-2048"),
    (r"^SUITE 3\b", "tpcb"),
    (r"^SUITE 4\b", "sstable"),
]
SCENARIOS_TITLE = "Application scenarios (sqlite_vs_aydb_bench)"


def sqlite_chart_specs(data):
    """Return (slug, title, rows) for each SQLite comparison chart."""
    specs = []
    order, merged = merged_tables(data, "standard_sql_bench")
    for key in order:
        table = merged[key]
        if re.search(r"ratio|speedup", table["header"][-1], re.I):
            slug = next((name for pattern, name in SVG_SLUGS
                         if re.search(pattern, key[0])), None)
            specs.append((slug, key[0], reported_ratio_rows(table)))
    order, merged = merged_tables(data, "sqlite_vs_aydb_bench")
    rows = []
    for key in order:
        label = re.sub(r"^BENCHMARK \d+ \(Phase 2\):\s*", "", key[0])
        rows.append((label, computed_ratio(merged[key], "Vanilla", "aydb")))
    specs.append(("scenarios", SCENARIOS_TITLE, rows))
    return specs


def sqlite_charts(data):
    return "\n".join(ratio_chart(title, SQLITE_NOTE, rows)
                     for _slug, title, rows in sqlite_chart_specs(data))


SVG_STYLE = """
svg { --bg: #ffffff; --fg: #1f2328; --muted: #59636e; --grid: #d1d9e0;
      --s0: #2a78d6; --s1: #eb6834; --s2: #1baf7a; }
@media (prefers-color-scheme: dark) {
  svg { --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --grid: #3d444d;
        --s0: #3987e5; --s1: #d95926; --s2: #199e70; }
}
text { font-family: system-ui, -apple-system, "Segoe UI", Roboto, Helvetica,
       Arial, sans-serif; fill: var(--fg); }
.title { font-size: 16px; font-weight: 600; }
.note, .tick, .legend, .foot { fill: var(--muted); }
.note, .legend { font-size: 13px; }
.label { font-size: 13px; }
.tick, .foot { font-size: 11px; }
.tick { font-variant-numeric: tabular-nums; }
.s0 { fill: var(--s0); }
.s1 { fill: var(--s1); }
.s2 { fill: var(--s2); }
"""


def svg_bar(x_start, x_end, y, height, grows_right, css_class):
    """A bar with a rounded end at its data value and a square baseline."""
    width = max(x_end - x_start, 2)
    radius = min(4, width)
    if grows_right:
        path = ("M{x:.1f} {y}h{w:.1f}a{r:.1f} {r:.1f} 0 0 1 {r:.1f} {r:.1f}"
                "v{v:.1f}a{r:.1f} {r:.1f} 0 0 1 -{r:.1f} {r:.1f}h-{w:.1f}z")
    else:
        path = ("M{e:.1f} {y}h-{w:.1f}a{r:.1f} {r:.1f} 0 0 0 -{r:.1f} {r:.1f}"
                "v{v:.1f}a{r:.1f} {r:.1f} 0 0 0 {r:.1f} {r:.1f}h{w:.1f}z")
    return '<path class="{}" d="{}"/>'.format(css_class, path.format(
        x=x_start, e=x_end, y=y, w=width - radius, r=radius,
        v=max(height - 2 * radius, 0)))


def render_svg_chart(title, note, rows, footer):
    """Render a ratio chart as a standalone image with its own background."""
    width, pad, label_width = 960, 20, 340
    plot_x, plot_width = pad + label_width + 12, width - (pad + label_width + 12) - pad - 8
    bar, gap, row_pad = 10, 2, 7
    row_height = 3 * bar + 2 * gap + 2 * row_pad
    rows = [(label, values) for label, values in rows if values]
    title_lines = textwrap.wrap(title, 104)
    parts = []
    y = pad + 12
    for line in title_lines:
        parts.append('<text class="title" x="{}" y="{}">{}</text>'.format(pad, y, esc(line)))
        y += 21
    parts.append('<text class="note" x="{}" y="{}">{}</text>'.format(pad, y - 1, esc(note)))
    y += 22
    legend_x = pad
    for index, os_name in enumerate(OS_NAMES):
        parts.append('<rect class="s{}" x="{}" y="{}" width="10" height="10" rx="2"/>'.format(
            index, legend_x, y - 9))
        parts.append('<text class="legend" x="{}" y="{}">{}</text>'.format(
            legend_x + 16, y, esc(os_name)))
        legend_x += 150
    y += 14
    if not rows:
        parts.append('<text class="note" x="{}" y="{}">no result</text>'.format(pad, y + 16))
        y += 30
    else:
        values = [value for _, per_os in rows for value, _ in per_os.values()]
        position, base, ticks, tick_label = ratio_scale(values)
        x_of = lambda value: plot_x + position(value) / 100 * plot_width
        top, bottom = y, y + row_height * len(rows)
        for tick in ticks:
            parts.append(
                '<line x1="{x:.1f}" x2="{x:.1f}" y1="{a}" y2="{b}" stroke="var({c})" '
                'stroke-width="1"/>'.format(x=x_of(tick), a=top, b=bottom,
                                            c="--muted" if tick == base else "--grid"))
        for label, per_os in rows:
            lines = textwrap.wrap(label, 46)
            text_y = y + row_height / 2 + 4 - (len(lines) - 1) * 8
            for line in lines:
                parts.append('<text class="label" x="{}" y="{:.1f}">{}</text>'.format(
                    pad, text_y, esc(line)))
                text_y += 16
            for index, os_name in enumerate(OS_NAMES):
                if os_name not in per_os:
                    continue
                value = per_os[os_name][0]
                bar_y = y + row_pad + index * (bar + gap)
                start, end = sorted((x_of(base), x_of(value)))
                parts.append(svg_bar(start, end, bar_y, bar, value >= base,
                                     "s{}".format(index)))
            y += row_height
        for tick in ticks:
            anchor = ("start" if position(tick) < 0.5 else
                      "end" if position(tick) > 99.5 else "middle")
            parts.append('<text class="tick" text-anchor="{}" x="{:.1f}" y="{}">{}</text>'.format(
                anchor, x_of(tick), y + 15, esc(tick_label(tick))))
        y += 22
    y += 18
    parts.append('<text class="foot" x="{}" y="{}">{}</text>'.format(pad, y, esc(footer)))
    height = y + pad - 6
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" '
        'height="{h}" role="img" aria-label="{a}"><title>{a}</title><style>{s}</style>'
        '<rect width="{w}" height="{h}" rx="6" fill="var(--bg)"/>{p}</svg>\n'
    ).format(w=width, h=height, a=esc(title), s=SVG_STYLE, p="".join(parts))


def write_svg_charts(data, out, sha):
    """Write one image per SQLite comparison chart. Every name is always written,
    so that the README never links to a missing file."""
    footer = "Commit {} · one CI run per OS on shared runners · details: results page".format(
        sha[:7] if sha else "unknown")
    specs = {slug: (title, rows) for slug, title, rows in sqlite_chart_specs(data) if slug}
    directory = out / "charts"
    directory.mkdir(exist_ok=True)
    for slug in [name for _, name in SVG_SLUGS] + ["scenarios"]:
        title, rows = specs.get(slug, (slug, []))
        (directory / (slug + ".svg")).write_text(
            render_svg_chart(title, SQLITE_NOTE, rows, footer), encoding="utf-8")


def concurrent_read_charts(data):
    """One chart per workload: reads/s at each thread count."""
    workloads = {}
    for os_name in OS_NAMES:
        text = data[os_name]["bench"]["concurrent_read_bench"]
        for _section, header, rows in (parse_tables(text) if text else []):
            if len(header) < 3 or header[1] != "threads":
                continue
            for cells in rows:
                reads = first_number(cells[2]) if len(cells) >= 3 else None
                if reads:
                    per_thread = workloads.setdefault(cells[0], {})
                    per_thread.setdefault(cells[1], {})[os_name] = (
                        reads, "{:,.0f} reads/s".format(reads))
    title = "Concurrent reads: {}"
    if not workloads:
        return '<p class="none">{}: no result</p>'.format(esc(title.format("all")))
    high = max(value for rows in workloads.values() for per_os in rows.values()
               for value, _ in per_os.values())
    scale = linear_scale(high)
    out = []
    for workload, rows in workloads.items():
        out.append(render_chart(
            title.format(workload), "Reads per second by thread count.",
            [("{} thread{}".format(t, "" if t == "1" else "s"), values)
             for t, values in rows.items()], scale))
    return "\n".join(out)


def pal_charts(data):
    """aydb OsFile against std::fs::File in pal_bench benchmark 2."""
    order, merged = merged_tables(data, "pal_bench")
    tables = [merged[key] for key in order if key[0].startswith("BENCHMARK 2")]
    note = ("aydb OsFile relative to std::fs::File, log scale. "
            "Right of 1x: OsFile is faster.")
    out = []
    if tables:
        rows = [("{} thread{}".format(label, "" if label == "1" else "s"), values)
                for label, values in reported_ratio_rows(tables[0])]
        out.append(ratio_chart("Hot-cache 4 KB reads", note, rows))
    if len(tables) > 1:
        out.append(ratio_chart(
            "4 KB reads while another thread writes and flushes", note,
            [("8 readers", computed_ratio(tables[1], "std::fs::File", "aydb"))]))
    if not out:
        out.append('<p class="none">pal_bench: no result</p>')
    return "\n".join(out)


def charts_section(data):
    return "\n".join([
        "<h2>Benchmark charts</h2>",
        '<p class="meta">Each chart shows one run per operating system. Hover '
        "or focus a row for its values. The tables and raw output below hold "
        "every number.</p>",
        "<h3>aydb compared with SQLite</h3>",
        sqlite_charts(data),
        "<h3>Read scaling</h3>",
        concurrent_read_charts(data),
        "<h3>Positional reads: aydb OsFile compared with std::fs::File</h3>",
        pal_charts(data),
    ])


def raw_outputs(data):
    out = []
    sections = [("clippy", "Clippy"), ("tests", "cargo test"),
                ("reliability", "Reliability benchmark")]
    items = [(key, label, lambda e, k=key: e[k]) for key, label in sections]
    items += [(name, name, lambda e, n=name: e["bench"][n]) for name in BENCHES]
    for _key, label, getter in items:
        out.append("<details><summary><code>{}</code></summary>".format(esc(label)))
        for os_name in OS_NAMES:
            text = getter(data[os_name])
            if text is None:
                out.append('<p class="none">{}: no result</p>'.format(esc(os_name)))
                continue
            out.append("<details><summary>{}</summary>"
                       '<pre class="raw">{}</pre></details>'.format(
                           esc(os_name), esc(text)))
        out.append("</details>")
    return "\n".join(out)


def build_results(artifacts, out, meta, repo_url):
    data = collect(artifacts)
    sha = meta["sha"]
    short = sha[:7] if sha else "unknown"
    commit = ('<a href="{}/commit/{}"><code>{}</code></a>'.format(
        esc(repo_url), esc(sha), esc(short)) if sha else "unknown")
    run = ('<a href="{}">workflow run</a>'.format(esc(meta["run_url"]))
           if meta["run_url"] else "unknown")
    body = [
        "<h1>Latest test and benchmark results</h1>",
        '<p class="meta">Commit: {} &middot; Generated: {} &middot; {}</p>'.format(
            commit, esc(meta["timestamp"] or "unknown"), run),
        '<div class="note"><p>These numbers come from shared GitHub-hosted '
        "runners. Other jobs share the same hosts, so results are noisy and "
        "vary between runs. They are not comparable with the table in the "
        "README, which was measured on different hardware. SQLite in these "
        "benchmarks is the system library of each operating system "
        "(winsqlite3 on Windows, the distribution package on Linux, the OS "
        "library on macOS), so SQLite versions and build options differ "
        "between columns.</p></div>",
        "<h2>Status per operating system</h2>",
    ]
    for os_name in OS_NAMES:
        body.append(os_summary_table(os_name, data[os_name]))
    body.append(charts_section(data))
    body.append("<h2>Benchmark comparison across operating systems</h2>")
    body.append(comparison_tables(data))
    body.append("<h2>Raw output</h2>")
    body.append(raw_outputs(data))
    body.append(CHART_SCRIPT)
    html_text = page("Test and benchmark results", "\n".join(body), "",
                     "results", "<style>{}</style>\n".format(CHART_CSS))
    (out / "results.html").write_text(html_text, encoding="utf-8")
    write_svg_charts(data, out, sha)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--artifacts", required=True, type=Path,
                        help="directory holding the downloaded CI artifacts")
    parser.add_argument("--repo", required=True, type=Path,
                        help="repository root (README.md and docs/)")
    parser.add_argument("--out", required=True, type=Path,
                        help="output directory (recreated)")
    parser.add_argument("--sha", default="", help="commit SHA")
    parser.add_argument("--run-url", default="", help="workflow run URL")
    parser.add_argument("--timestamp", default="", help="build timestamp")
    parser.add_argument("--repo-url",
                        default="https://github.com/toyoshim-i/aydb",
                        help="repository URL used for source links")
    parser.add_argument("--branch", default="main",
                        help="branch used in GitHub blob URLs")
    args = parser.parse_args(argv)

    repo_root = args.repo.resolve()
    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    repo_url = args.repo_url.rstrip("/")
    build_markdown_pages(repo_root, out, repo_url, args.branch)
    meta = {"sha": args.sha, "run_url": args.run_url,
            "timestamp": args.timestamp}
    build_results(args.artifacts, out, meta, repo_url)
    (out / ".nojekyll").write_text("", encoding="utf-8")
    print("site written to {}".format(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

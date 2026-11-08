#!/usr/bin/env python3
"""Build the static GitHub Pages site for aydb.

The site has three parts:

* index.html: the repository README rendered to HTML.
* docs/: the Markdown files under docs/ rendered to HTML, with the
  figures from docs/assets copied next to them.
* results.html: test and benchmark results collected by the CI workflow.

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
import posixpath
import re
import shutil
import sys
from pathlib import Path

import markdown

OS_NAMES = ["windows-latest", "ubuntu-latest", "macos-latest"]

# Benchmarks in display order; the first two also feed the comparison tables.
BENCHES = [
    "standard_sql_bench",
    "sqlite_vs_aydb_bench",
    "concurrent_read_bench",
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


def comparison_tables(data):
    """Cross-OS tables for the SQLite-vs-aydb benchmarks."""
    out = []
    for bench in COMPARED:
        per_os = {}
        for os_name in OS_NAMES:
            text = data[os_name]["bench"][bench]
            per_os[os_name] = parse_tables(text) if text else []
        # Merge by (section, table index within section), keeping the order
        # in which tables first appear.
        order = []
        merged = {}
        for os_name in OS_NAMES:
            seen = {}
            for section, header, rows in per_os[os_name]:
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
    body.append("<h2>Benchmark comparison across operating systems</h2>")
    body.append(comparison_tables(data))
    body.append("<h2>Raw output</h2>")
    body.append(raw_outputs(data))
    html_text = page("Test and benchmark results", "\n".join(body), "",
                     "results")
    (out / "results.html").write_text(html_text, encoding="utf-8")


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

#!/usr/bin/env python3
"""Stdlib-only unit tests for tools/build_site.py."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_site


SAMPLE_STANDARD_SQL = """\
==========================================================================================
SUITE 1: SQLite "Speedtest1"-Style Workload (Selected Tests 100-240, 5,000 Rows, 120B)
==========================================================================================
Speedtest1 Sub-Benchmark                 |     Vanilla SQLite |        aydb VTab |      Speedup
------------------------------------------------------------------------------------------
100 - 5000 INSERTs in a transaction      |            8.12 ms |          6.40 ms |        1.27x
110 - 5000 SELECTs with WHERE PK = ?     |            4.00 ms |          2.00 ms |        2.00x
------------------------------------------------------------------------------------------
TOTAL SPEEDTEST1 SUITE TIME              |           12.12 ms |          8.40 ms |        1.44x

==========================================================================================
SUITE 2: YCSB-Style Workloads A-F (Zipfian theta=0.99, Value Size = 256 B — Inline Storage)
==========================================================================================
YCSB Workload (256 B)                    |     Vanilla SQLite |        aydb VTab |        Ratio
------------------------------------------------------------------------------------------
Workload A (50% Read / 50% Update)       |         25000 ops/s |      30000 ops/s |        1.20x

==========================================================================================
SUITE 2: YCSB-Style Workloads A-F (Zipfian theta=0.99, Value Size = 2048 B — WiscKey)
==========================================================================================
YCSB Workload (2048 B)                   |     Vanilla SQLite |        aydb VTab |        Ratio
------------------------------------------------------------------------------------------
Workload E (Short Ranges)                |          1000 ops/s |        850 ops/s |        0.85x

==========================================================================================
SUITE 3: TPC-B-Style (pgbench-like) 4-Table OLTP Workload (5 SQL Statements per Tx)
==========================================================================================
Durability Mode                    | Vanilla SQLite(WAL) |          aydb VTab |        Ratio
------------------------------------------------------------------------------------------
Buffered (NORMAL / wal_buf.db)     |          4200 tps   |         5040 tps   |        1.20x

==========================================================================================
SUITE 4: On-Disk SSTable Evaluation (memtable_capacity_bytes = 128 KB, idx_*.db SSTables)
==========================================================================================
On-Disk SSTable Workload (128 KB MemTable)   | Vanilla SQLite   | aydb (SSTables)  |      Ratio
----------------------------------------------------------------------------------------------
Sequential 10k Inserts                       |         15.0 ms  |         12.0 ms  |      1.25x
"""

SAMPLE_SQLITE_VS_AYDB = """\
==========================================================================================
BENCHMARK 1 (Phase 2): Small Synchronous Config Updates via SQL (300 Commits, 128B Value)
==========================================================================================
SQL Engine & Mode                  |   Throughput |  P50 Lat(us) |  P99 Lat(us) |  Disk Writes
------------------------------------------------------------------------------------------
Vanilla SQLite (WAL, sync=FULL)    |     1200 /s  |     800.0 us |    1200.0 us |    1200.0 KB
aydb VTab (wal_sync)               |     2400 /s  |     400.0 us |     600.0 us |    1200.0 KB
"""

SAMPLE_CONCURRENT_READ = """\
Workload               | threads |      Reads/s |    P50 Lat
------------------------------------------------------------------------------------------
Hot-Cache Point Read   |       1 |   150000 /s  |     5.0 us
Hot-Cache Point Read   |       4 |   520000 /s  |     6.1 us
"""

SAMPLE_PAL = """\
==========================================================================================
BENCHMARK 2: Hot Cached 4 KB Reads
==========================================================================================
Threads | std::fs::File |     aydb OsFile | Speedup
------------------------------------------------------------------------------------------
1       |     100000 /s |      200000 /s  |   2.00x
"""


class TestBuildSite(unittest.TestCase):
    def test_validate_http_url(self):
        self.assertEqual(build_site.validate_http_url("", "test"), "")
        self.assertEqual(
            build_site.validate_http_url("https://github.com/org/repo", "repo"),
            "https://github.com/org/repo",
        )
        self.assertEqual(
            build_site.validate_http_url("http://localhost:8080/x", "run"),
            "http://localhost:8080/x",
        )
        for bad in ["javascript:alert(1)", "file:///etc/passwd", "github.com/org/repo"]:
            with self.assertRaises(ValueError):
                build_site.validate_http_url(bad, "bad")

    def test_parse_tables_preserves_summary_rows(self):
        tables = build_site.parse_tables(SAMPLE_STANDARD_SQL)
        self.assertEqual(len(tables), 5)
        section, header, rows = tables[0]
        self.assertIn("SUITE 1", section)
        self.assertEqual(header[0], "Speedtest1 Sub-Benchmark")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[2][0], "TOTAL SPEEDTEST1 SUITE TIME")

    def test_classifiers_and_status_parsers(self):
        self.assertEqual(build_site.classify_fmt("   \n"), ("ok", "formatted"))
        self.assertEqual(build_site.classify_fmt("Diff at line 1"), ("bad", "formatting differences or errors"))
        self.assertEqual(build_site.classify_clippy("    Finished `dev` profile"), ("ok", "no warnings"))
        self.assertEqual(build_site.classify_clippy("warning: unused var"), ("bad", "1 diagnostic(s)"))
        self.assertEqual(
            build_site.classify_tests("test result: ok. 771 passed; 0 failed; 0 ignored"),
            ("ok", "771 passed, 0 failed, 0 ignored"),
        )
        self.assertEqual(
            build_site.classify_reliability("SUITE 1\n3A | PASS | ok"),
            ("ok", "1 checks passed"),
        )
        self.assertEqual(
            build_site.parse_bench_status("standard_sql_bench exit=0 seconds=14\n"),
            {"standard_sql_bench": (0, 14)},
        )
        self.assertEqual(
            build_site.parse_test_status("fmt exit=0\ntests exit=1\n"),
            {"fmt": 0, "tests": 1},
        )

    def test_collect_and_build_results_outputs_html_and_svgs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = root / "artifacts"
            out = root / "site"
            out.mkdir()
            test_dir = artifacts / "test-ubuntu-latest"
            bench_dir = artifacts / "bench-ubuntu-latest" / "bench"
            test_dir.mkdir(parents=True)
            bench_dir.mkdir(parents=True)

            (test_dir / "fmt.txt").write_text("", encoding="utf-8")
            (test_dir / "clippy.txt").write_text("    Finished `dev` profile\n", encoding="utf-8")
            (test_dir / "tests.txt").write_text(
                "test result: ok. 771 passed; 0 failed; 0 ignored\n", encoding="utf-8"
            )
            (test_dir / "reliability.txt").write_text("SUITE 1\n3A | PASS\n", encoding="utf-8")
            (test_dir / "test-status.txt").write_text(
                "fmt exit=0\nclippy exit=0\ntests exit=0\nreliability exit=0\n", encoding="utf-8"
            )
            (bench_dir / "status.txt").write_text(
                "standard_sql_bench exit=0 seconds=10\n"
                "sqlite_vs_aydb_bench exit=0 seconds=5\n"
                "concurrent_read_bench exit=0 seconds=3\n"
                "pal_bench exit=0 seconds=2\n",
                encoding="utf-8",
            )
            (bench_dir / "standard_sql_bench.txt").write_text(SAMPLE_STANDARD_SQL, encoding="utf-8")
            (bench_dir / "sqlite_vs_aydb_bench.txt").write_text(SAMPLE_SQLITE_VS_AYDB, encoding="utf-8")
            (bench_dir / "concurrent_read_bench.txt").write_text(SAMPLE_CONCURRENT_READ, encoding="utf-8")
            (bench_dir / "pal_bench.txt").write_text(SAMPLE_PAL, encoding="utf-8")

            meta = {"sha": "1234567890abcdef", "run_url": "https://github.com/org/repo/actions/runs/1", "timestamp": "2026-11-27T00:00:00Z"}
            build_site.build_results(artifacts, out, meta, "https://github.com/org/repo")

            results_html = (out / "results.html").read_text(encoding="utf-8")
            self.assertIn("771 passed, 0 failed, 0 ignored", results_html)
            self.assertIn("no result", results_html)
            for slug in ["speedtest", "ycsb-256", "ycsb-2048", "tpcb", "sstable", "scenarios"]:
                svg_path = out / "charts" / f"{slug}.svg"
                self.assertTrue(svg_path.is_file(), f"missing {svg_path}")
                self.assertIn("<svg", svg_path.read_text(encoding="utf-8"))

    def test_link_rewriter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "docs" / "phase1").mkdir(parents=True)
            (root / "docs" / "assets").mkdir(parents=True)
            (root / "src").mkdir()
            (root / "README.md").write_text("", encoding="utf-8")
            (root / "docs" / "phase1" / "00_OVERVIEW.md").write_text("", encoding="utf-8")
            (root / "docs" / "phase1" / "01_PAL.md").write_text("", encoding="utf-8")
            (root / "docs" / "assets" / "fig.png").write_bytes(b"png")
            (root / "src" / "lib.rs").write_text("", encoding="utf-8")

            rewriter = build_site.LinkRewriter(root, "https://github.com/org/repo", "main")
            fragment = (
                '<a href="../../README.md">root</a>'
                '<a href="01_PAL.md#sec">pal</a>'
                '<img src="../assets/fig.png">'
                '<a href="../../src/lib.rs#L10">src</a>'
                '<a href="../../src">dir</a>'
                '<a href="https://example.com">ext</a>'
                '<a href="mailto:a@b.c">mail</a>'
                '<a href="//cdn.example/x">proto</a>'
                '<a href="#local">local</a>'
                '<a href="../../../outside.txt">escape</a>'
            )
            rewritten = rewriter.rewrite(fragment, "docs/phase1/00_OVERVIEW.md")
            self.assertIn('href="../../index.html"', rewritten)
            self.assertIn('href="01_PAL.html#sec"', rewritten)
            self.assertIn('src="../assets/fig.png"', rewritten)
            self.assertIn('href="https://github.com/org/repo/blob/main/src/lib.rs#L10"', rewritten)
            self.assertIn('href="https://github.com/org/repo/tree/main/src"', rewritten)
            self.assertIn('href="https://example.com"', rewritten)
            self.assertIn('href="mailto:a@b.c"', rewritten)
            self.assertIn('href="//cdn.example/x"', rewritten)
            self.assertIn('href="#local"', rewritten)
            self.assertIn('href="../../../outside.txt"', rewritten)

    def test_os_summary_table(self):
        entry_ok = {
            "test_dir": True,
            "bench_dir": True,
            "fmt": " \n",
            "clippy": "Finished",
            "tests": "test result: ok. 771 passed; 0 failed; 0 ignored",
            "reliability": "SUITE 1\n3A | PASS | ok",
            "test_status": {"fmt": 0, "clippy": 0, "tests": 0, "reliability": 0},
            "status": {name: (0, 2) for name in build_site.BENCHES},
            "bench": {name: "ok" for name in build_site.BENCHES},
        }
        html_ok = build_site.os_summary_table("ubuntu-latest", entry_ok)
        self.assertIn("771 passed, 0 failed, 0 ignored", html_ok)
        self.assertIn("completed in 2 s", html_ok)
        self.assertNotIn('>fail<', html_ok)

        # Non-zero exit overrides classifier; missing exit or missing text marks fail; missing bench is no result.
        entry_bad = {
            "test_dir": True,
            "bench_dir": False,
            "fmt": "",
            "clippy": "Finished",
            "tests": None,
            "reliability": "SUITE 1\n3A | PASS | ok",
            "test_status": {"fmt": 1, "clippy": 0, "tests": 0},
            "status": {"standard_sql_bench": (0, 5)},
            "bench": {
                "standard_sql_bench": None,
                "sqlite_vs_aydb_bench": "output",
                **{n: None for n in build_site.BENCHES[2:]},
            },
        }
        html_bad = build_site.os_summary_table("windows-latest", entry_bad)
        self.assertIn("exit status 1 (formatted)", html_bad)
        self.assertIn("exit status 0, output missing", html_bad)
        self.assertIn("exit status not recorded (1 checks passed)", html_bad)
        self.assertIn("exit status 0 after 5 s, output missing", html_bad)
        self.assertIn("output present, exit status not recorded", html_bad)
        self.assertIn("no result", html_bad)

    def test_main_refuses_ancestor_out_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            repo.mkdir()
            artifacts = root / "artifacts"
            artifacts.mkdir()
            with self.assertRaises(SystemExit):
                build_site.main([
                    "--artifacts", str(artifacts),
                    "--repo", str(repo),
                    "--out", str(root),
                ])


if __name__ == "__main__":
    unittest.main()

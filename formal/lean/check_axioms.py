#!/usr/bin/env python3
"""Verifies that all Lean 4 formal proofs have zero `sorry`/`admit`/custom axioms."""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ALLOWED_AXIOMS = {"propext", "Classical.choice", "Quot.sound"}
FORBIDDEN_PATTERNS = [
    ("sorry", re.compile(r"\bsorry\b")),
    ("admit", re.compile(r"\badmit\b")),
    ("axiom", re.compile(r"(?m)^\s*axiom\b")),
    ("partial", re.compile(r"(?m)^\s*(?:private\s+|protected\s+)?partial\b")),
    ("unsafe", re.compile(r"(?m)^\s*(?:private\s+|protected\s+)?unsafe\b")),
    ("implemented_by", re.compile(r"@\[.*?implemented_by")),
    ("extern", re.compile(r"@\[.*?extern")),
]


def strip_lean_comments(src: str) -> str:
    out = []
    i = 0
    n = len(src)
    block_depth = 0
    in_string = False
    while i < n:
        ch = src[i]
        nxt = src[i + 1] if i + 1 < n else ""
        if in_string:
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == '"':
                in_string = False
            i += 1
            continue
        if block_depth > 0:
            if ch == "/" and nxt == "-":
                block_depth += 1
                i += 2
            elif ch == "-" and nxt == "/":
                block_depth -= 1
                i += 2
            else:
                if ch == "\n":
                    out.append("\n")
                i += 1
            continue
        if ch == "/" and nxt == "-":
            block_depth = 1
            i += 2
        elif ch == "-" and nxt == "-":
            while i < n and src[i] != "\n":
                i += 1
        elif ch == '"':
            in_string = True
            i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def scan_sources() -> None:
    lean_files = sorted(
        p for p in ROOT.rglob("*.lean") if ".lake" not in p.parts
    )
    if not lean_files:
        raise RuntimeError(f"no .lean files found under {ROOT}")
    for path in lean_files:
        cleaned = strip_lean_comments(path.read_text(encoding="utf-8"))
        for label, pat in FORBIDDEN_PATTERNS:
            m = pat.search(cleaned)
            if m:
                line = cleaned[: m.start()].count("\n") + 1
                raise RuntimeError(
                    f"forbidden construct `{label}` in {path.relative_to(ROOT)}:{line}"
                )


def collect_declared_theorems() -> set[str]:
    declared: set[str] = set()
    ns_re = re.compile(r"^\s*namespace\s+([A-Za-z0-9_.']+)\s*$")
    end_re = re.compile(r"^\s*end\s+([A-Za-z0-9_.']+)\s*$")
    thm_re = re.compile(r"^\s*(?:@\[[^\]]*\]\s*)*theorem\s+([A-Za-z0-9_']+)")

    for path in sorted((ROOT / "AydbFormal").glob("*.lean")):
        if path.name == "AxiomCheck.lean":
            continue
        cleaned = strip_lean_comments(path.read_text(encoding="utf-8"))
        ns_stack: list[str] = []
        for line in cleaned.splitlines():
            if m := ns_re.match(line):
                ns_stack.extend(m.group(1).split("."))
            elif m := end_re.match(line):
                parts = m.group(1).split(".")
                if len(ns_stack) >= len(parts) and ns_stack[-len(parts) :] == parts:
                    del ns_stack[-len(parts) :]
            elif m := thm_re.match(line):
                full_name = ".".join([*ns_stack, m.group(1)])
                declared.add(full_name)
    return declared


def check_theorem_coverage(expected: list[str]) -> None:
    declared = collect_declared_theorems()
    missing = sorted(declared - set(expected))
    if missing:
        raise RuntimeError(
            f"AxiomCheck.lean is missing theorem coverage for: {missing}"
        )


def verify_axioms() -> int:
    axiom_file = ROOT / "AydbFormal" / "AxiomCheck.lean"
    expected = []
    for raw in axiom_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("#print axioms "):
            expected.append(line[len("#print axioms ") :].strip())
    if not expected:
        raise RuntimeError("AxiomCheck.lean lists zero theorems")
    check_theorem_coverage(expected)

    proc = subprocess.run(
        ["lake", "env", "lean", "AydbFormal/AxiomCheck.lean"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"AxiomCheck.lean failed with code {proc.returncode}")

    combined = proc.stdout + "\n" + proc.stderr
    dep_re = re.compile(
        r"^'([^']+)'\s+depends on axioms:\s*\[([^\]]*)\]", re.MULTILINE
    )
    no_ax_re = re.compile(
        r"^'([^']+)'\s+does not depend on any axioms", re.MULTILINE
    )
    observed = {}
    for m in dep_re.finditer(combined):
        axs = {a.strip() for a in m.group(2).split(",") if a.strip()}
        bad = axs - ALLOWED_AXIOMS
        if bad:
            raise RuntimeError(
                f"theorem {m.group(1)} depends on disallowed axioms: {sorted(bad)}"
            )
        observed[m.group(1)] = axs
    for m in no_ax_re.finditer(combined):
        observed[m.group(1)] = set()

    missing = [thm for thm in expected if thm not in observed]
    if missing:
        raise RuntimeError(f"missing #print axioms output for: {missing}")

    if "sorryAx" in combined:
        raise RuntimeError("sorryAx appeared in AxiomCheck output")
    return len(observed)


def main() -> int:
    scan_sources()
    count = verify_axioms()
    print(f"AXIOM_CHECK_OK: {count} theorems verified with zero sorry and standard axioms only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

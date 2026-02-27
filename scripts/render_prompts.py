#!/usr/bin/env python3
"""Render prompt templates into concrete prompt files."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


INCLUDE_RE = re.compile(r'{{\s*include\s+"([^"]+)"\s*}}')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if rendered output differs from files on disk",
    )
    return parser.parse_args()


def ensure_within_prompts(root: Path, path: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"include path escapes prompts directory: {path}") from exc


def render_template(path: Path, prompts_root: Path, stack: list[Path]) -> str:
    if path in stack:
        cycle = " -> ".join(str(item) for item in [*stack, path])
        raise ValueError(f"cyclic prompt include: {cycle}")
    if not path.exists():
        raise FileNotFoundError(f"missing prompt include: {path}")

    text = path.read_text(encoding="utf-8")
    next_stack = [*stack, path]

    def replace_include(match: re.Match[str]) -> str:
        include_rel = match.group(1)
        include_path = (prompts_root / include_rel).resolve()
        ensure_within_prompts(prompts_root.resolve(), include_path)
        rendered = render_template(include_path, prompts_root, next_stack)
        return rendered.rstrip("\n")

    rendered = INCLUDE_RE.sub(replace_include, text)
    if not rendered.endswith("\n"):
        rendered += "\n"
    return rendered


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    prompts_root = repo_root / "prompts"
    templates_root = prompts_root / "templates"

    templates = sorted(templates_root.glob("*.tmpl.md"))
    if not templates:
        print(f"no templates found in {templates_root}")
        return 0

    stale_outputs: list[Path] = []
    changed_outputs: list[Path] = []

    for template in templates:
        output_name = template.name.replace(".tmpl.md", ".md")
        if output_name == template.name:
            raise ValueError(f"template file name must end with .tmpl.md: {template}")

        output_path = prompts_root / output_name
        rendered = render_template(template, prompts_root, stack=[])
        current = output_path.read_text(encoding="utf-8") if output_path.exists() else None

        if current != rendered:
            stale_outputs.append(output_path)
            if not args.check:
                output_path.write_text(rendered, encoding="utf-8")
                changed_outputs.append(output_path)

    if args.check:
        if stale_outputs:
            for output in stale_outputs:
                print(f"stale prompt: {output.relative_to(repo_root)}")
            return 1
        print("all prompts are up to date")
        return 0

    if changed_outputs:
        for output in changed_outputs:
            print(f"wrote {output.relative_to(repo_root)}")
    else:
        print("no prompt changes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

import ast
from pathlib import Path

import pytest

SOURCE_ROOT = Path("src/procurement_parser")


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


@pytest.mark.parametrize("layer", ["domain", "application"])
def test_inner_layers_do_not_import_infrastructure_or_entrypoints(layer: str) -> None:
    violations = []
    for path in sorted((SOURCE_ROOT / layer).rglob("*.py")):
        for imported in _imports(path):
            if imported.startswith(
                (
                    "procurement_parser.infrastructure",
                    "procurement_parser.entrypoints",
                )
            ):
                violations.append(f"{path}: {imported}")

    assert violations == []

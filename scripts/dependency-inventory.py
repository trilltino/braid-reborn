"""Refresh direct declarations, exact resolved edges and source search evidence.

Uses Cargo's locked graph and official crates.io metadata for previously unseen
packages. Existing same-day latest-version observations remain observations,
never automatic upgrade requests. Run from any directory with Python 3.11+.
"""
import json
import re
import subprocess
import sys
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "docs/audit/dependencies.json"
previous = json.loads(OUTPUT.read_text())
latest = {row["dependency"]: row["latest_stable"] for row in previous}
graph = json.loads(subprocess.run(
    ["cargo", "metadata", "--all-features", "--locked", "--format-version", "1"],
    cwd=ROOT, check=True, capture_output=True, text=True,
).stdout)
packages = {package["id"]: package for package in graph["packages"]}
nodes = {node["id"]: node for node in graph["resolve"]["nodes"]}
lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
checksums = {(p["name"], p["version"], p.get("source")): p.get("checksum") for p in lock["package"]}
rows = []
for identity in graph["workspace_members"]:
    owner = packages[identity]
    directory = Path(owner["manifest_path"]).parent
    sources = [(file, file.read_text(encoding="utf-8").splitlines()) for file in directory.rglob("*.rs")]
    for dependency in owner["dependencies"]:
        name = dependency["name"]
        alias = dependency.get("rename") or name
        kind = dependency["kind"] or "normal"
        target = dependency["target"]
        selected = []
        for edge in nodes[identity]["deps"]:
            if edge["name"] != alias.replace("-", "_"):
                continue
            if any((k["kind"] or "normal") == kind and k["target"] == target for k in edge["dep_kinds"]):
                selected.append(packages[edge["pkg"]])
        if name not in latest and dependency["source"] is not None:
            request = urllib.request.Request(f"https://crates.io/api/v1/crates/{name}", headers={"User-Agent": "braid-reborn-dependency-inventory/1"})
            with urllib.request.urlopen(request, timeout=30) as response:
                metadata = json.load(response)
            latest[name] = metadata["crate"]["max_stable_version"]
        pattern = re.compile(r"\b" + re.escape(alias.replace("-", "_")) + r"\b")
        evidence = [f"{file.relative_to(ROOT).as_posix()}:{index+1}" for file, lines in sources
                    for index, line in enumerate(lines) if pattern.search(line)]
        rows.append({
            "owner": owner["name"], "dependency": name, "alias": alias,
            "kind": kind, "target": target or "all", "requirement": dependency["req"],
            "locked": sorted({p["version"] for p in selected}),
            "latest_stable": latest.get(name, "workspace"),
            "features": dependency["features"], "default_features": dependency["uses_default_features"],
            "optional": dependency["optional"], "licenses": sorted({p["license"] or "unrecorded" for p in selected}),
            "source_files": evidence,
            "locked_packages": [{"version": p["version"], "source": p["source"],
                                 "checksum": checksums.get((p["name"],p["version"],p["source"]))} for p in selected],
        })
rows.sort(key=lambda row: (row["owner"], row["kind"], row["dependency"], row["target"]))
OUTPUT.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
document = ROOT / "docs/dependency-audit.md"
intro = document.read_text().split("| Owner | Dependency |", 1)[0]
table = ["| Owner | Dependency | Locked / latest stable | Scope and features | Call-site evidence |",
         "| --- | --- | --- | --- | --- |"]
for row in rows:
    name = row["dependency"]
    local = row["latest_stable"] == "workspace"
    label = name if local else f"[{name}](https://crates.io/crates/{name})"
    versions = "workspace" if local else f"{', '.join(row['locked'])} / {row['latest_stable']}"
    features = ",".join(row["features"]) or ("default" if row["default_features"] else "no defaults")
    scope = f"{row['kind']}; {row['target']}; {features}" + ("; optional" if row["optional"] else "")
    evidence = row["source_files"][0].split("crates/",1)[-1] if row["source_files"] else "local/feature boundary"
    table.append(f"| {row['owner']} | {label} | {versions} | {scope} | {evidence} |")
document.write_text(intro + "\n".join(table) + "\n", encoding="utf-8")
print(f"Recorded {len(rows)} direct declarations across {len(graph['workspace_members'])} workspace members")

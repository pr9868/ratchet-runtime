"""Check version/parse consistency and record or verify the public source inventory."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--write-manifest", action="store_true")
args = parser.parse_args()
metadata = tomllib.loads((root / "pyproject.toml").read_text())
version = metadata["project"]["version"]
if metadata["project"]["name"] == "ratchet-runtime":
    import configparser
    config = configparser.ConfigParser()
    config.read(root / "setup.cfg")
    assert config["metadata"]["version"] == version
    assert (root / "VERSION").read_text().strip() == version
    version_source = root / "src/ratchet_runtime.py"
else:
    assert json.loads((root / ".claude-plugin/plugin.json").read_text())["version"] == version
    version_source = root / "skills/anchor-orchestrator/scripts/anchor_harness/__init__.py"
assert re.search(r'__version__ = "' + re.escape(version) + '"', version_source.read_text())
manifest_path = root / "docs/SOURCE-MANIFEST.json"
files = sorted(set(subprocess.check_output(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    cwd=root).decode().strip("\0").split("\0")))
records = {}
for name in files:
    path = root / name
    if path == manifest_path or not path.is_file():
        continue
    if path.suffix == ".py":
        ast.parse(path.read_text(), filename=name)
    records[name] = hashlib.sha256(path.read_bytes()).hexdigest()
value = {"version": version, "algorithm": "sha256", "files": records}
if args.write_manifest:
    manifest_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
else:
    assert json.loads(manifest_path.read_text()) == value, "source differs from recorded inventory"
subprocess.run(["git", "diff", "--check"], cwd=root, check=True)
print(f"{metadata['project']['name']} {version}: versions, syntax, {len(records)} source hashes and whitespace verified")

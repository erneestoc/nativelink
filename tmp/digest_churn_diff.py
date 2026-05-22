#!/usr/bin/env python3
# TEMPORARY DIAGNOSTIC TOOL (branch ec/pr2243-digest-churn-diag). NOT FOR MERGE.
#
# Diffs two NativeLink worker logs to find which actions' REAPI input-root
# digests churned between two builds with no source changes, and exactly which
# input file(s) changed.
#
# It consumes the `prepare_action_inputs_digest` log target emitted by the
# instrumented worker (nativelink-worker/src/running_actions_manager.rs on this
# branch). That target emits two line kinds per action, joined by the
# per-action `work_directory`:
#
#   1. digest line   -- from prepare_action_inputs():
#        input_root_digest = <hex>-<size>
#        input_file_count  = <n>
#        tree              = "<relpath>|<digest>[|x] <relpath>|<digest>[|x] ..."
#        work_directory    = <per-action exec dir>
#
#   2. identity line -- from inner_prepare_action() after the Command decodes:
#        output_identity   = "<path>:<path>:..."   (sorted, deduped)
#        operation_id      = <id>
#        work_directory    = <per-action exec dir>
#
# Within ONE build, work_directory uniquely identifies an action and ties the
# two lines together. ACROSS two builds, work_directory is NOT stable, so the
# tool re-keys each action by its stable `output_identity` (declared output
# paths are content-independent). An action whose input-root digest differs
# between build A and build B "churned"; the tool reports the count of churned
# actions and, per churned action, which input files' content digests changed.
#
# Usage:
#   python3 tmp/digest_churn_diff.py BUILD_A.log BUILD_B.log [--max-files N] [--show-stable]
#
# BUILD_A.log / BUILD_B.log are plain-text worker logs (stdout or file capture)
# from two builds. The tool extracts only `prepare_action_inputs_digest` lines,
# so unrelated log noise is fine.

import argparse
import re
import sys
from collections import defaultdict

# Matches `key=value` and `key="quoted value"` as emitted by the tracing
# logger. The worker uses both styles depending on whether the value contains
# spaces; `tree` and `output_identity` are space-bearing so they are quoted.
_FIELD_RE = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\S+)')


def _unquote(value):
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1].replace('\\"', '"').replace('\\\\', '\\')
    return value


def parse_fields(line):
    """Extract key=value fields from a single tracing log line."""
    return {k: _unquote(v) for k, v in _FIELD_RE.findall(line)}


def parse_tree(tree_str):
    """Parse the flattened `tree` field into {relpath: content_digest}.

    Each entry is `relpath|content_digest` or `relpath|content_digest|x`
    (the trailing `|x` flags an executable file). The executable bit is
    folded into the digest key so a change in the bit alone still shows up.
    """
    files = {}
    if not tree_str:
        return files
    for entry in tree_str.split(" "):
        if not entry:
            continue
        parts = entry.split("|")
        if len(parts) < 2:
            continue
        relpath = parts[0]
        digest = parts[1]
        if len(parts) >= 3 and parts[2] == "x":
            digest = digest + " (executable)"
        files[relpath] = digest
    return files


def load_build(path):
    """Parse one build log into {output_identity: action_record}.

    An action_record is a dict with: input_root_digest, files (dict),
    work_directory, operation_id. Lines are correlated by work_directory
    within the build, then the action is keyed by its stable output_identity.
    """
    # work_directory -> partial record assembled from the two line kinds.
    by_workdir = {}
    with open(path, "r", errors="replace") as handle:
        for line in handle:
            if "prepare_action_inputs_digest" not in line:
                continue
            fields = parse_fields(line)
            workdir = fields.get("work_directory")
            if not workdir:
                continue
            rec = by_workdir.setdefault(
                workdir,
                {
                    "input_root_digest": None,
                    "files": None,
                    "output_identity": None,
                    "operation_id": None,
                    "work_directory": workdir,
                },
            )
            if "input_root_digest" in fields:
                rec["input_root_digest"] = fields["input_root_digest"]
            if "tree" in fields:
                rec["files"] = parse_tree(fields["tree"])
            elif "input_file_count" in fields and rec["files"] is None:
                # digest line present but tree empty (action with no inputs).
                rec["files"] = {}
            if "output_identity" in fields:
                rec["output_identity"] = fields["output_identity"]
            if "operation_id" in fields:
                rec["operation_id"] = fields["operation_id"]

    by_identity = {}
    dropped_no_identity = 0
    dropped_no_digest = 0
    collisions = 0
    for rec in by_workdir.values():
        if rec["input_root_digest"] is None:
            dropped_no_digest += 1
            continue
        identity = rec["output_identity"]
        if not identity:
            dropped_no_identity += 1
            continue
        if identity in by_identity:
            # Two actions in one build sharing an output-path set is unusual
            # but possible (e.g. an action declaring no outputs). Keep the
            # first and count the rest so the numbers stay honest.
            collisions += 1
            continue
        by_identity[identity] = rec
    return by_identity, {
        "total_workdirs": len(by_workdir),
        "dropped_no_identity": dropped_no_identity,
        "dropped_no_digest": dropped_no_digest,
        "collisions": collisions,
        "actions": len(by_identity),
    }


def diff_files(files_a, files_b):
    """Return (changed, added, removed) for two {relpath: digest} maps."""
    changed = []
    for path in sorted(set(files_a) & set(files_b)):
        if files_a[path] != files_b[path]:
            changed.append((path, files_a[path], files_b[path]))
    added = sorted(set(files_b) - set(files_a))
    removed = sorted(set(files_a) - set(files_b))
    return changed, added, removed


def main():
    parser = argparse.ArgumentParser(
        description="Find churned REAPI input-root digests between two builds."
    )
    parser.add_argument("build_a", help="worker log from build 1")
    parser.add_argument("build_b", help="worker log from build 2")
    parser.add_argument(
        "--max-files",
        type=int,
        default=20,
        help="max changed/added/removed input files to print per action",
    )
    parser.add_argument(
        "--show-stable",
        action="store_true",
        help="also list actions whose input-root digest did NOT change",
    )
    args = parser.parse_args()

    build_a, stats_a = load_build(args.build_a)
    build_b, stats_b = load_build(args.build_b)

    print("== parse summary ==")
    for label, stats in (("build A", stats_a), ("build B", stats_b)):
        print(
            f"  {label}: {stats['actions']} actions "
            f"({stats['total_workdirs']} prepare calls; "
            f"{stats['dropped_no_digest']} missing digest line, "
            f"{stats['dropped_no_identity']} missing identity line, "
            f"{stats['collisions']} identity collisions)"
        )

    common = sorted(set(build_a) & set(build_b))
    only_a = sorted(set(build_a) - set(build_b))
    only_b = sorted(set(build_b) - set(build_a))

    churned = []
    stable = []
    for identity in common:
        rec_a = build_a[identity]
        rec_b = build_b[identity]
        if rec_a["input_root_digest"] != rec_b["input_root_digest"]:
            churned.append(identity)
        else:
            stable.append(identity)

    print()
    print("== churn summary ==")
    print(f"  actions present in both builds : {len(common)}")
    print(f"  input-root digest CHURNED      : {len(churned)}")
    print(f"  input-root digest stable       : {len(stable)}")
    print(f"  actions only in build A        : {len(only_a)}")
    print(f"  actions only in build B        : {len(only_b)}")
    if common:
        pct = 100.0 * len(churned) / len(common)
        print(f"  churn rate                     : {pct:.1f}%")

    print()
    print("== churned actions ==")
    for identity in churned:
        rec_a = build_a[identity]
        rec_b = build_b[identity]
        short_id = identity if len(identity) <= 100 else identity[:97] + "..."
        print(f"\n- action [{short_id}]")
        print(f"    operation_id A : {rec_a['operation_id']}")
        print(f"    operation_id B : {rec_b['operation_id']}")
        print(f"    input-root A   : {rec_a['input_root_digest']}")
        print(f"    input-root B   : {rec_b['input_root_digest']}")
        files_a = rec_a["files"] or {}
        files_b = rec_b["files"] or {}
        changed, added, removed = diff_files(files_a, files_b)
        if not changed and not added and not removed:
            print(
                "    NOTE: input-root digest differs but flattened trees are "
                "identical -- check for symlink/node-property differences "
                "not captured by the file-content flatten."
            )
        if changed:
            print(f"    changed input files ({len(changed)}):")
            for path, da, db in changed[: args.max_files]:
                print(f"      {path}")
                print(f"        A: {da}")
                print(f"        B: {db}")
            if len(changed) > args.max_files:
                print(f"      ... +{len(changed) - args.max_files} more")
        if added:
            print(f"    inputs added in B ({len(added)}):")
            for path in added[: args.max_files]:
                print(f"      + {path}")
            if len(added) > args.max_files:
                print(f"      ... +{len(added) - args.max_files} more")
        if removed:
            print(f"    inputs removed in B ({len(removed)}):")
            for path in removed[: args.max_files]:
                print(f"      - {path}")
            if len(removed) > args.max_files:
                print(f"      ... +{len(removed) - args.max_files} more")

    if args.show_stable and stable:
        print()
        print("== stable actions (input-root digest unchanged) ==")
        for identity in stable:
            short_id = identity if len(identity) <= 100 else identity[:97] + "..."
            print(f"  {short_id}")

    # Roll up which individual input paths churn most across all churned
    # actions -- the path that appears in the most churned trees is the most
    # likely non-deterministic producer to trace first.
    if churned:
        path_hits = defaultdict(int)
        for identity in churned:
            files_a = build_a[identity]["files"] or {}
            files_b = build_b[identity]["files"] or {}
            changed, _, _ = diff_files(files_a, files_b)
            for path, _, _ in changed:
                path_hits[path] += 1
        if path_hits:
            print()
            print("== most-churned input paths (across all churned actions) ==")
            ranked = sorted(path_hits.items(), key=lambda kv: (-kv[1], kv[0]))
            for path, hits in ranked[: args.max_files]:
                print(f"  {hits:5d}x  {path}")

    # Exit non-zero when churn is detected so the tool is CI/script friendly.
    return 1 if churned else 0


if __name__ == "__main__":
    sys.exit(main())

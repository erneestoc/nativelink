# Digest-churn diagnostic (branch `ec/pr2243-digest-churn-diag`)

**TEMPORARY. NOT FOR MERGE.** Same spirit as `ec/pr2243-cache-hit-timing`.

## What this diagnoses

The worker's `DirectoryCache` is keyed by the REAPI input-root digest. Two
builds with **no source changes** were measured to reuse ~0% of the cache:
every action's input-root digest differed. REAPI input-root digests are
content-addressed, so a digest that churns with no source changes means some
upstream action produced **non-deterministic output bytes** that flow
downstream as inputs.

This branch instruments the worker to log, per `prepare_action_inputs` call:

1. the **input-root digest** (the directory-cache key), and
2. a **flattened input tree** (`relpath -> file content digest`), and
3. a **stable cross-build action identity** (the action's declared output
   paths -- content-independent, unlike the churning input-root digest).

The `tmp/digest_churn_diff.py` tool then diffs two builds' logs and reports
which actions churned and which input file(s) changed.

## What the worker logs

All lines go to the tracing target **`prepare_action_inputs_digest`** (a new
target, distinct from the existing `prepare_action_inputs_timing`). Two line
kinds, joined per-action by `work_directory`:

- **digest line** (from `prepare_action_inputs`): `input_root_digest`,
  `input_file_count`, and `tree` -- a sorted, single-line
  `relpath|content_digest[|x]` rendering of every input file. The `tree` is
  built from the REAPI `Directory` Merkle protos already in the CAS; no file
  content is read or re-hashed.
- **identity line** (from `inner_prepare_action`, after the `Command`
  decodes): `output_identity` (sorted, deduped output paths joined by `:`)
  and `operation_id`.

This is logging-only: zero behavior change to materialization, caching,
error handling, cancellation, or output upload. The diagnostic tree flatten
is wrapped so a failure is logged and the action proceeds normally.

## Procedure for the user

You must run the iOS Bazel build yourself; this branch only adds the
instrumentation. The directory cache must be enabled on the worker.

1. Build & deploy the worker from branch `ec/pr2243-digest-churn-diag`.
2. Enable the log target. With `RUST_LOG`, include the new target, e.g.:
   ```
   RUST_LOG=info,prepare_action_inputs_digest=info
   ```
   (`info` already enables it; just make sure worker stdout is captured.)
3. **Build 1** -- run the iOS Bazel build. Capture worker stdout:
   ```
   <run worker> 2>&1 | tee build_a.log
   ```
4. **Build 2** -- with NO source changes, run the iOS Bazel build again
   against the same warm worker. Capture again:
   ```
   <run worker> 2>&1 | tee build_b.log
   ```
5. Run the diff tool:
   ```
   python3 tmp/digest_churn_diff.py build_a.log build_b.log
   ```

If the worker fleet has multiple workers, concatenate each worker's log into
the per-build file, or run the build pinned to a single worker -- the tool
keys actions by stable output identity, so logs from multiple workers in one
build can simply be `cat`-ed together.

## Reading the output

- **churn summary**: count and rate of actions whose input-root digest
  differed between the two builds.
- **churned actions**: per action, the two operation IDs, the two input-root
  digests, and the specific input files whose content digest changed
  (`changed`), or were `added`/`removed`.
- **most-churned input paths**: a roll-up ranking input paths by how many
  churned actions they appear in -- the top entry is the best lead for the
  non-deterministic producer to trace.

The tool exits non-zero when any churn is detected (CI-friendly).
Options: `--max-files N` (per-action print cap, default 20),
`--show-stable` (also list non-churned actions).

## Reverting

This whole branch is diagnostic. Drop the branch / revert the two `TEMP:`
commits and delete `tmp/` to remove all instrumentation.

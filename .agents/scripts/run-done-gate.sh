#!/usr/bin/env bash
set -u

project_root="${1:-.}"
cd "$project_root" || exit 2

if [[ ! -f package.json ]]; then
  echo "BLOCKED: package.json was not found in $(pwd)"
  exit 2
fi

failures=0

has_script() {
  node -e '
    const pkg = require("./package.json");
    const name = process.argv[1];
    process.exit(pkg.scripts && pkg.scripts[name] ? 0 : 1);
  ' "$1"
}

run_required() {
  local name="$1"
  if ! has_script "$name"; then
    echo "FAIL: npm script $name is missing"
    failures=$((failures + 1))
    return
  fi

  echo "RUN: npm run $name"
  if npm run "$name"; then
    echo "PASS: npm run $name"
  else
    echo "FAIL: npm run $name"
    failures=$((failures + 1))
  fi
}

run_required lint
run_required test
run_required build

if has_script test:e2e; then
  echo "RUN: npm run test:e2e"
  if npm run test:e2e; then
    echo "PASS: npm run test:e2e"
  else
    echo "FAIL: npm run test:e2e"
    failures=$((failures + 1))
  fi
else
  echo "INFO: npm run test:e2e is not configured; manual browser checks remain required"
fi

if [[ "$failures" -gt 0 ]]; then
  echo "DONE GATE: FAIL ($failures required check(s) failed)"
  exit 1
fi

echo "DONE GATE: PASS"
echo "Status: READY_FOR_MANUAL_TEST, pending docs/review/manual checklist confirmation"

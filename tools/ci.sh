#!/usr/bin/env bash
# The CI script (campaign PA, slice E6; decisions D8b and D10).
#
# Runs what the public CI workflow runs, from the repository root, locally or
# in a disposable container:
#   1. Pueue 4: if pueue and pueued 4.x are already on PATH they are reported
#      and used; otherwise the pinned release 4.0.4 (Linux x86_64, static musl
#      binaries) is downloaded into a temporary directory and each binary is
#      verified against the sha256 pinned below (a mismatch refuses);
#   2. a virtual environment in the temporary directory with pytest and its
#      dependencies at pinned versions, installed with --require-hashes;
#   3. the standard profile, `python -m pytest -v -rs` (no path or -k filter),
#      into a log;
#   4. the skip accounting: `tools/skip_report.py --summary-line` must say
#      `untagged 0` and `consistent`;
#   5. `tools/skill_check.py`.
# It exits non-zero on any failure, and 0 only if all of the above passed.
#
# Environment (all optional):
#   ACS_TEST_PULL=1   pass-through: the container tests pull the public pinned
#                     test image (conftest.PUBLIC_IMAGE) if it is absent
#   ACS_TEST_IMAGE    pass-through: another test image
#   ACS_CI_LOG        where to write the pytest log (default: in the
#                     temporary directory, which is removed on exit)
#   ACS_CI_KEEP=1     keep the temporary directory (binaries, venv, log)
#
# It never runs the installer or touches an installation; the suite uses its
# own sandboxes (a private pueued on a short /tmp socket, temporary guard
# homes). The only network use is the pinned downloads in steps 1 and 2.
set -euo pipefail

PUEUE_VERSION="4.0.4"
PUEUE_BASE="https://github.com/Nukesor/pueue/releases/download/v${PUEUE_VERSION}"

# sha256 of the static musl release binaries (x86_64), checked against the
# copies the maintainer's Linux host runs
PUEUE_SHA256="c1b10d7e4e62211075ddd0e1dc3e8cbfc5a43d662cb3be7402a28504e23fcb51"
PUEUED_SHA256="5afeff6adbafb909e8d54e2caff158e6966c2adffa2c09e60fd631cc51b60390"

say() { printf 'ci: %s\n' "$*"; }
die() { printf 'ci: FAIL: %s\n' "$*" >&2; exit 1; }

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
[ -f tools/skip_report.py ] && [ -f tools/skill_check.py ] \
    || die "not at the repository root ($repo)"

work="$(mktemp -d "${TMPDIR:-/tmp}/acs-ci.XXXXXX")"
cleanup() {
    if [ "${ACS_CI_KEEP:-}" = 1 ]; then
        say "kept $work"
    else
        rm -rf "$work"
    fi
}
trap cleanup EXIT

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# the guard and the process-identity checks read process start times with ps
command -v ps >/dev/null 2>&1 || die "ps (the procps package) is required"
command -v git >/dev/null 2>&1 || die "git is required"

# ---- 1. Pueue ---------------------------------------------------------------
pueue_ok() {
    command -v pueue >/dev/null 2>&1 && command -v pueued >/dev/null 2>&1 \
        && pueue --version 2>/dev/null | grep -Eq '^pueue 4\.' \
        && pueued --version 2>/dev/null | grep -Eq '^pueued 4\.'
}

if pueue_ok; then
    say "pueue/pueued 4.x already on PATH: $(command -v pueue): $(pueue --version); $(command -v pueued): $(pueued --version)"
else
    [ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] \
        || die "no pueue/pueued 4.x on PATH, and the pinned download is only for Linux x86_64 (this is $(uname -s) $(uname -m)); install Pueue 4 and rerun"
    command -v curl >/dev/null 2>&1 || die "curl is required to fetch Pueue"
    bin="$work/bin"
    mkdir -p "$bin"
    for pair in "pueue:$PUEUE_SHA256" "pueued:$PUEUED_SHA256"; do
        name="${pair%%:*}"
        want="${pair##*:}"
        asset="${name}-x86_64-unknown-linux-musl"
        say "fetching $asset ($PUEUE_VERSION)"
        curl -fsSL --proto '=https' --tlsv1.2 --retry 3 \
            -o "$bin/$name.part" "$PUEUE_BASE/$asset" \
            || die "download of $PUEUE_BASE/$asset failed"
        got="$(sha256_of "$bin/$name.part")"
        if [ "$got" != "$want" ]; then
            rm -f "$bin/$name.part"
            die "$asset: sha256 mismatch (got $got, pinned $want); refusing"
        fi
        mv "$bin/$name.part" "$bin/$name"
        chmod 755 "$bin/$name"
        say "$name sha256 $got (matches the pin)"
    done
    PATH="$bin:$PATH"
    export PATH
    pueue_ok || die "the downloaded pueue/pueued do not run as 4.x on this system"
    say "using $(pueue --version) and $(pueued --version) from $bin"
fi

# ---- 2. venv with pinned pytest ---------------------------------------------
say "python: $(python3 --version 2>&1) ($(command -v python3))"
python3 - <<'PY' || die "Python 3.11 or later is required"
import sys
sys.exit(0 if sys.version_info >= (3, 11) else 1)
PY
cat > "$work/requirements.txt" <<'EOF'
pytest==9.1.1 --hash=sha256:37a86b45efb9a47a61a36449063e8e18d0cab3161329fc099eb21783169c4f0c
iniconfig==2.3.0 --hash=sha256:f631c04d2c48c52b84d0d0549c99ff3859c98df65b3101406327ecc7d53fbf12
packaging==26.3 --hash=sha256:d7193f7c8e4e93f444fde0262bf90af30e16fa0ad0ad44cb553c87339b23cd1c
pluggy==1.6.0 --hash=sha256:e920276dd6813095e9377c0bc5566d94c932c33b27a3e3945d8389c374dd4746
pygments==2.21.0 --hash=sha256:2363c69b61c4a97c838da3b130dcd6468f4848992b21a82f2a63ec34377137d9
EOF
python3 -m venv "$work/venv" || die "cannot create the virtual environment"
"$work/venv/bin/python" -m pip install -q --disable-pip-version-check \
    --require-hashes -r "$work/requirements.txt" \
    || die "cannot install the pinned pytest"
say "$("$work/venv/bin/python" -m pytest --version)"

# ---- 3. the standard profile ------------------------------------------------
log="${ACS_CI_LOG:-$work/pytest.log}"
say "standard profile: python -m pytest -v -rs (log: $log)"
set +e
"$work/venv/bin/python" -m pytest -v -rs 2>&1 | tee "$log"
rc="${PIPESTATUS[0]}"
set -e
[ "$rc" -eq 0 ] || die "pytest exited $rc"

# ---- 4. skip accounting -----------------------------------------------------
summary="$(python3 tools/skip_report.py --summary-line "$log")" \
    || die "skip report is inconsistent: $summary"
say "$summary"
case "$summary" in
    *"untagged 0;"*) ;;
    *) die "skip accounting: untagged skips present" ;;
esac
case "$summary" in
    *"; consistent") ;;
    *) die "skip accounting: not consistent" ;;
esac

# ---- 5. skill check ---------------------------------------------------------
python3 tools/skill_check.py || die "tools/skill_check.py failed"

say "OK: standard profile green, skips accounted, skill check passed"

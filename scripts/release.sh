#!/usr/bin/env bash
# Release procedure from docs/DEVELOPMENT.md, with checks and pauses.
#
# Usage: scripts/release.sh [RELEASE_VERSION] [SNAP_VERSION]
#   Defaults: RELEASE_VERSION from the pom (x.y.z-SNAPSHOT -> x.y.z),
#             SNAP_VERSION = x.y.(z+1)-SNAPSHOT
#
# Pushes to the "origin" remote; set RELEASE_REMOTE to use another (e.g. upstream).
# Stops on the first failed command. Asks before every push and before deploy.
# Only tracked files are committed (git add -u), so untracked files are
# never included.

set -euo pipefail

# gpg-agent needs the terminal to show the passphrase prompt during deploy.
GPG_TTY="$(tty)" || { echo "ERROR: release.sh must be run from a terminal." >&2; exit 1; }
export GPG_TTY

REMOTE="${RELEASE_REMOTE:-origin}"
MAIN=main

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo; echo "==> $*"; }
confirm() {
    local reply
    read -r -p "$1 [y/N] " reply
    [[ "$reply" == "y" || "$reply" == "Y" ]] || die "Stopped by user."
}
pom_version() {
    # Project version from the root pom at the given ref (default: working tree).
    local ref="${1:-}"
    if [[ -n "$ref" ]]; then
        git show "$ref:pom.xml"
    else
        cat pom.xml
    fi | sed -n '/<parent>/,/<\/parent>/d; s:.*<version>\(.*\)</version>.*:\1:p' | head -n 1
}

cd "$(git rev-parse --show-toplevel)"

# ---------------------------------------------------------------- versions
CURRENT_VERSION="$(pom_version)"
[[ "$CURRENT_VERSION" == *-SNAPSHOT ]] || die "pom version is $CURRENT_VERSION, expected a -SNAPSHOT version."

RELEASE_VERSION="${1:-${CURRENT_VERSION%-SNAPSHOT}}"
if [[ -n "${2:-}" ]]; then
    SNAP_VERSION="$2"
else
    IFS=. read -r MAJ MIN PAT <<< "$RELEASE_VERSION"
    [[ "$PAT" =~ ^[0-9]+$ ]] || die "Cannot derive SNAP_VERSION from $RELEASE_VERSION; pass it as the second argument."
    SNAP_VERSION="$MAJ.$MIN.$((PAT + 1))-SNAPSHOT"
fi

[[ "$RELEASE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "RELEASE_VERSION '$RELEASE_VERSION' is not x.y.z."
[[ "$SNAP_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT$ ]] || die "SNAP_VERSION '$SNAP_VERSION' is not x.y.z-SNAPSHOT."

TAG="v$RELEASE_VERSION"
BRANCH="release-$RELEASE_VERSION"

# --------------------------------------------------------------- preflight
info "Preflight checks"
command -v mvn >/dev/null || die "mvn not found on PATH."
# Sign once now so a gpg problem (stale keyring lock, no pinentry) stops the
# release before anything is pushed. Also caches the passphrase for deploy.
echo "release preflight" | gpg --clearsign >/dev/null || die "gpg signing failed."
git remote get-url "$REMOTE" >/dev/null 2>&1 || die "No git remote named '$REMOTE'."
REMOTE_URL="$(git remote get-url "$REMOTE")"

[[ "$(git branch --show-current)" == "$MAIN" ]] || die "Not on $MAIN."
git diff --quiet && git diff --cached --quiet || die "Tracked files have uncommitted changes."

git fetch "$REMOTE" --tags
git merge --ff-only "$REMOTE/$MAIN" >/dev/null || die "Local $MAIN cannot fast-forward to $REMOTE/$MAIN."
[[ "$(pom_version)" == "$CURRENT_VERSION" ]] || die "pom version changed after pulling $MAIN; re-run."

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && die "Tag $TAG already exists locally."
[[ -z "$(git ls-remote --tags "$REMOTE" "refs/tags/$TAG")" ]] || die "Tag $TAG already exists on $REMOTE."
git rev-parse -q --verify "refs/heads/$BRANCH" >/dev/null && die "Branch $BRANCH already exists locally."
[[ -z "$(git ls-remote --heads "$REMOTE" "$BRANCH")" ]] || die "Branch $BRANCH already exists on $REMOTE."

cat <<EOF

  Remote:           $REMOTE ($REMOTE_URL)
  Current version:  $CURRENT_VERSION
  Release version:  $RELEASE_VERSION  (tag $TAG, branch $BRANCH)
  Next snapshot:    $SNAP_VERSION
EOF
confirm "Start release?"

# ---------------------------------------------------------- release branch
info "Creating $BRANCH and setting version $RELEASE_VERSION"
git checkout -b "$BRANCH"
mvn -q versions:set -DgenerateBackupPoms=false -DnewVersion="$RELEASE_VERSION"
[[ "$(pom_version)" == "$RELEASE_VERSION" ]] || die "versions:set did not set $RELEASE_VERSION."

info "Update the version in README.md and CHANGELOG.md now (in another terminal or editor)."
read -r -p "Press Enter when done... " _
git --no-pager diff --stat
confirm "Commit these changes as '$TAG'?"
git add -u
git commit -m "$TAG"
RELEASE_SHA="$(git rev-parse HEAD)"

info "Setting version $SNAP_VERSION"
mvn -q versions:set -DgenerateBackupPoms=false -DnewVersion="$SNAP_VERSION"
[[ "$(pom_version)" == "$SNAP_VERSION" ]] || die "versions:set did not set $SNAP_VERSION."
git add -u
git commit -m "back to snapshot"

confirm "Push $BRANCH to $REMOTE ($REMOTE_URL)?"
git push -u "$REMOTE" "$BRANCH"

# ---------------------------------------------------------------- PR/merge
info "Open a PR for $BRANCH into $MAIN and merge it."
echo "    ${REMOTE_URL%.git}/compare/$MAIN...$BRANCH?expand=1"
read -r -p "Press Enter after the PR is merged... " _

git checkout "$MAIN"
git pull --ff-only "$REMOTE" "$MAIN"

# ------------------------------------------------------ find commit to tag
# A merge commit keeps RELEASE_SHA; squash or rebase merges create a new commit.
if git merge-base --is-ancestor "$RELEASE_SHA" HEAD; then
    REF="$RELEASE_SHA"
else
    REF="$(git log --format=%H -n 1 --fixed-strings --grep="$TAG" "$MAIN")"
    [[ -n "$REF" ]] || die "No commit on $MAIN matches '$TAG'. Tag it by hand."
fi
REF_VERSION="$(pom_version "$REF")"
info "Commit to tag:"
git --no-pager log -n 1 --format='    %H %s' "$REF"
echo "    pom version at that commit: $REF_VERSION"
[[ "$REF_VERSION" == "$RELEASE_VERSION" ]] || die "pom version at $REF is $REF_VERSION, expected $RELEASE_VERSION. Tag it by hand."

confirm "Tag $REF as $TAG and push the tag to $REMOTE?"
git tag "$TAG" "$REF"
git push "$REMOTE" "$TAG"

# ------------------------------------------------------------------ deploy
git checkout -q "$TAG"
[[ "$(pom_version)" == "$RELEASE_VERSION" ]] || die "Checked-out version is not $RELEASE_VERSION."
confirm "Run 'mvn clean deploy -P release' for $RELEASE_VERSION?"
mvn clean deploy -P release
git checkout -q "$MAIN"

info "Deployed $RELEASE_VERSION. Log into Sonatype and release it."

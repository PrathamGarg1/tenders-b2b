#!/usr/bin/env bash
# GeM Contractors Directory — Play Store prep & production AAB build
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MOBILE="$ROOT/apps/mobile"

echo "==> 1/4 Refresh store assets + Android native branding"
cd "$MOBILE"
npm run generate:play-assets
npx expo prebuild --platform android --no-install

echo ""
echo "==> 2/4 Typecheck"
npm run typecheck

echo ""
echo "==> 3/4 EAS production build (AAB)"
echo "    Account: jaishreeramji | Project: gem-contractor-directory"
echo "    First time: run interactively so Expo can create the upload keystore:"
echo "      cd apps/mobile && eas build -p android --profile production"
echo ""
if [[ "${1:-}" == "--build" ]]; then
  eas build -p android --profile production --non-interactive
else
  echo "    Skipping build (pass --build to run: eas build -p android --profile production --non-interactive)"
fi

echo ""
echo "==> 4/4 Done — manual Play Console steps:"
echo "  • Privacy: publish store-assets/privacy-policy.txt on Google Sites → set EXPO_PUBLIC_PRIVACY_POLICY_URL"
echo "  • Play Console: https://play.google.com/console"
echo "  • Upload AAB from: https://expo.dev/accounts/jaishreeramji/projects/gem-contractor-directory/builds"
echo "  • Listing files: apps/mobile/store-assets/"
echo "  • Full checklist: docs/GOOGLE_PLAY.md"

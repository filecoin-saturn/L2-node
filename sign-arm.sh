#!/usr/bin/env bash
set -e

codesign --force \
  --timestamp \
  --options runtime \
  --sign "Developer ID Application: Protocol Labs, Inc." dist/macos-arm64_darwin_arm64/L2-node

zip -r dist/L2-node_Darwin_arm64.zip dist/macos-arm64_darwin_arm64/L2-node

xcrun altool --notarize-app \
   --primary-bundle-id "saturn.filecoin.l2-node" \
   -u "oli@protocol.ai" \
   -p "${AC_PASSWORD}" \
   -t osx \
   -f dist/L2-node_Darwin_arm64.zip

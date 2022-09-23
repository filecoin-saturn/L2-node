#!/usr/bin/env bash
set -e

codesign --force \
  --timestamp \
  --options runtime \
  --sign "Developer ID Application: Protocol Labs, Inc." dist/macos-x86-64_darwin_amd64_v1/L2-node

zip -r dist/L2-node_Darwin_x86_64.zip dist/macos-x86-64_darwin_amd64_v1/L2-node

xcrun altool --notarize-app \
   --primary-bundle-id "saturn.filecoin.l2-node" \
   -u "oli@protocol.ai" \
   -p "${AC_PASSWORD}" \
   -t osx \
   -f dist/L2-node_Darwin_x86_64.zip

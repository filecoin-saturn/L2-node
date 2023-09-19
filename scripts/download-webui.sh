#!/usr/bin/env bash

set -e

# Check if required tools (jq and curl) are installed
for tool in curl jq; do
    if ! command -v $tool &> /dev/null; then
        echo "Error: $tool is not installed."
        exit 1
    fi
done

OUTDIR=$(dirname $0)/../resources/webui/

echo "⇣ Downloading webui dist to $OUTDIR ⇣"

mkdir -p $OUTDIR

# Downloads latest release
url=$(curl -s https://api.github.com/repos/filecoin-saturn/node-webui/releases/latest | \
    jq -r '.assets[] | select(.name|match("saturn-webui.tar.gz$")) | .browser_download_url')

if [ -z "$url" ]; then
    echo "Error: Could not find the release URL."
    exit 1
fi

curl -L $url | tar -zx -C $OUTDIR

#!/usr/bin/env bash

OUTDIR=$(dirname $0)/../resources/webui/

echo "⇣ Downloading webui dist to $OUTDIR ⇣"

mkdir -p $OUTDIR

# Downloads latest release
url=$(curl -L -s https://api.github.com/repos/filecoin-saturn/node-webui/releases/latest | \
    jq -r '.assets[] | select(.name|match("saturn-webui.tar.gz$")) | .browser_download_url')

curl -L $url | tar -zx -C $OUTDIR

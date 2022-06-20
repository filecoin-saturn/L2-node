#!/usr/bin/env bash

OUTDIR=$(dirname $0)/../resources/webui/

echo "⇣ Downloading webui dist to $OUTDIR ⇣"

mkdir -p $OUTDIR

# Downloads latest release
url=$(curl -s https://api.github.com/repos/filecoin-project/saturn-l2-webui/releases/latest | \
    jq -r '.assets[] | select(.name|match("saturn-l2-webui.tar.gz$")) | .browser_download_url')

curl -L $url | tar -zx -C $OUTDIR

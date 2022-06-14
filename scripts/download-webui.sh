#!/usr/bin/env bash

# Downloads latest release
url=$(curl -s https://api.github.com/repos/filecoin-project/saturn-l2-webui/releases/latest | \
    jq -r '.assets[] | select(.name|match("saturn-l2-webui.tar.gz$")) | .browser_download_url')

curl -L $url | tar -zx -C ../resources/webui/

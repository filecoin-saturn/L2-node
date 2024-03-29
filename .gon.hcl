source = ["./dist/macos-x86-64_darwin_amd64_v1/saturn-L2-node"]
bundle_id = "io.filecoin.saturn.l2-node"

apple_id {
  username = "oli@protocol.ai"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Protocol Labs, Inc."
}

zip {
  output_path="./dist/L2-node_Darwin_x86_64.zip"
}

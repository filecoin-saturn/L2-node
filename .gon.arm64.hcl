source = ["./dist/macos_darwin_arm64/L2-node"]
bundle_id = "saturn.filecoin.l2-node"

apple_id {
  username = "oli@protocol.ai"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Protocol Labs, Inc."
}

zip {
  output_path="./dist/L2-node_Darwin_arm64.zip"
}
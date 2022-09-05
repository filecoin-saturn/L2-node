source = [
  "./dist/saturn-L2-node-macos_darwin_amd64_v1/saturn-L2-node",
  "./dist/saturn-L2-node-macos_darwin_arm64/saturn-L2-node"
]
bundle_id = "saturn.filecoin.l2-node"

apple_id {
  username = "@env:AC_USERNAME"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Protocol Labs, Inc."
}

zip {
  output_path="./dist/saturn-L2-node.zip"
}
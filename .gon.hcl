source = ["./dist/macos_darwin_amd64/saturn-L2-node"]
bundle_id = "saturn.filecoin.l2-node"

apple_id {
  username = "@env:AC_USERNAME"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Protocol Labs, Inc."
}

zip {
  output_path="./dist/saturn-L2-node_macos.zip"
}

dmg {
  output_path="./dist/saturn-L2-node_macos.dmg"
  volume_name="saturn-L2-node"
}
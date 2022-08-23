# gon.hcl
#
# The path follows a pattern
# ./dist/BUILD-ID_TARGET/BINARY-NAME
source = ["./dist/saturn-L2-node-macos_darwin_amd64/saturn-L2-node"]
bundle_id = "filecoin.saturn.l2-node"

apple_id {
  username = "@env:AC_USERNAME"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: @env:AC_FULLNAME"
}

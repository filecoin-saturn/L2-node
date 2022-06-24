package resources

import "embed"

// WebUI folder is empty during local development, embed resources.go
// so go doesn't complain about "no embeddable files"
//
//go:embed webui resources.go
var WebUI embed.FS

package resources

import (
	"embed"
	_ "embed"
)

// webui folder is empty during local development, embed resources.go
// so go doesn't complain about "no embeddable files"
//
//go:embed webui resources.go
var WebUI embed.FS

// Package openapi 通过 go:embed 内嵌 OpenAPI 3 规范与 Swagger UI 静态资源,
// 离线提供可交互的 API 文档, 无需外部 CDN。
package openapi

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed openapi.json assets/swagger-ui-bundle.js assets/swagger-ui.css
var content embed.FS

// specHTML Swagger UI 页面(引用同路径下本地静态资源与 /openapi.json)
const specHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>区域再生资源回收服务 API</title>
  <link rel="stylesheet" href="./swagger-ui.css"/>
  <style>body{margin:0}</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="./swagger-ui-bundle.js"></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({
        url: '../openapi.json',
        dom_id: '#swagger-ui',
        deepLinking: true
      });
    };
  </script>
</body>
</html>`

// Register 挂载公开文档路由(无需鉴权):
//
//	GET /openapi.json
//	GET /docs/            (Swagger UI, 末尾斜杠必填)
//	GET /docs/swagger-ui-bundle.js / swagger-ui.css
func Register(mux interface {
	Get(pattern string, h http.HandlerFunc)
}) {
	mux.Get("/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		b, err := content.ReadFile("openapi.json")
		if err != nil {
			http.Error(w, "spec not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(b)
	})
	mux.Get("/docs/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(specHTML))
	})
	asset := func(name, contentType string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			b, err := fs.ReadFile(content, "assets/"+name)
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", contentType)
			_, _ = w.Write(b)
		}
	}
	mux.Get("/docs/swagger-ui-bundle.js", asset("swagger-ui-bundle.js", "application/javascript; charset=utf-8"))
	mux.Get("/docs/swagger-ui.css", asset("swagger-ui.css", "text/css; charset=utf-8"))
}

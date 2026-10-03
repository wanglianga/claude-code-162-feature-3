package httpapi

import (
	_ "embed"
	"net/http"
)

//go:embed openapi/openapi.yaml
var openapiYAML []byte

// openapiSpec 以原始 YAML 返回 OpenAPI 3.x 规范
func (s *Server) openapiSpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openapiYAML)
}

// swaggerUI 可直接在浏览器使用的 Swagger UI(资产走 CDN; 无外网时可直接下载 /openapi.yaml)
func (s *Server) swaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>区域再生资源回收服务 - API 文档</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
  <style>body{margin:0}.topbar{padding:10px 16px;font:14px sans-serif;background:#fafafa;border-bottom:1px solid #eee}
  .topbar a{margin-left:12px}</style>
</head>
<body>
  <div class="topbar">
    <strong>区域再生资源回收服务 API（OpenAPI 3）</strong>
    <a href="/openapi.yaml">下载 openapi.yaml</a>
    <span>｜ 点 <b>Authorize</b> 输入测试 Token，如 <code>token-resident1</code></span>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script>
    window.addEventListener('load', function () {
      window.ui = SwaggerUIBundle({
        url: '/openapi.yaml',
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis],
        layout: 'BaseLayout',
        persistAuthorization: true
      });
    });
  </script>
</body>
</html>`))
}

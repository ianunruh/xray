package web

import (
	"io/fs"
	"net/http"
)

func Handler() http.Handler {
	distSub, _ := fs.Sub(distFS, "dist")
	fileServer := http.FileServer(http.FS(distSub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			fileServer.ServeHTTP(w, r)
			return
		}
		// Try to serve the exact file (JS, CSS, images, etc.)
		if f, err := distSub.Open(path[1:]); err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		// Fall back to index.html for SPA routing
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

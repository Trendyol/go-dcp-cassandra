package rest

import (
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type server struct {
}

func NewServer() Server {
	return &server{}
}

type Server interface {
	SetupRouter() *gin.Engine
}

func (server server) SetupRouter() *gin.Engine {
	router := gin.New()
	router.Use(gzip.Gzip(gzip.BestCompression))
	router.Use(gin.Recovery())

	pprof.Register(router)
	router.GET("/_monitoring/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "UP"})
	})
	router.GET("/metrics", prometheusHandler())
	return router
}

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{DisableCompression: true})

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

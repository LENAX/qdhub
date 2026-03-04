package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

// Recovery returns a middleware that recovers from panics.
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 记录请求方法和路径，便于将 panic 与具体接口对应
				method := ""
				path := ""
				query := ""
				if c.Request != nil {
					method = c.Request.Method
					if c.Request.URL != nil {
						path = c.Request.URL.Path
						query = c.Request.URL.RawQuery
					}
				}
				// 打印完整调用栈，便于精确定位 panic 源头
				logrus.Errorf("[PANIC] %v | method=%s path=%s?%s\n%s", err, method, path, query, debug.Stack())
				c.JSON(http.StatusInternalServerError, Response{
					Code:    500,
					Message: "internal server error",
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}

// Logger returns a middleware that logs HTTP requests.
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()

		if query != "" {
			path = path + "?" + query
		}

		logrus.Infof("[HTTP] %d | %13v | %15s | %-7s %s",
			statusCode,
			latency,
			c.ClientIP(),
			c.Request.Method,
			path,
		)
	}
}

// CORS returns a middleware that handles CORS.
func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")
		c.Header("Access-Control-Max-Age", "86400")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// ts_proxy is a standalone Tushare realtime forwarding server for mainland deployment.
// It subscribes to Tushare WS, normalizes ticks, and streams them to Hong Kong clients over WebSocket with scheme B (RSA exchange AES + AES encryption).
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"qdhub/ts_proxy/server"
	"qdhub/ts_proxy/tushare"

	"github.com/sirupsen/logrus"
)

func main() {
	token := flag.String("token", os.Getenv("TUSHARE_TOKEN"), "Tushare token")
	topic := flag.String("topic", defaultEnv("TUSHARE_TOPIC", "HQ_STK_TICK"), "Tushare topic")
	codesStr := flag.String("codes", os.Getenv("TUSHARE_CODES"), "Tushare codes comma-separated (default: 3*.SZ,0*.SZ,6*.SH)")
	listen := flag.String("listen", defaultEnv("LISTEN_ADDR", ":8888"), "Listen address for WebSocket")
	rsaKey := flag.String("rsa-key", os.Getenv("RSA_PRIVATE_KEY_PATH"), "Path to RSA private key for scheme B")
	reconnectMax := flag.Int("reconnect-max", envInt("TUSHARE_RECONNECT_MAX", 30), "Max reconnect attempts to Tushare (0 = unlimited)")
	flag.Parse()

	if strings.TrimSpace(*token) == "" {
		log.Fatal("TUSHARE_TOKEN or -token is required")
	}
	if strings.TrimSpace(*rsaKey) == "" {
		log.Fatal("RSA_PRIVATE_KEY_PATH or -rsa-key is required")
	}

	codes := tushare.DefaultCodes
	if *codesStr != "" {
		codes = strings.Split(*codesStr, ",")
		for i := range codes {
			codes[i] = strings.TrimSpace(codes[i])
		}
	}

	broadcast, err := server.NewBroadcast(*rsaKey)
	if err != nil {
		log.Fatalf("NewBroadcast: %v", err)
	}

	http.HandleFunc("/realtime", broadcast.ServeWS)
	srv := &http.Server{Addr: *listen}

	go func() {
		logrus.Infof("[ts_proxy] listening on %s", *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("[ts_proxy] http: %v", err)
		}
	}()

	client := &tushare.Client{
		Token:         *token,
		Topic:         *topic,
		Codes:         codes,
		ReconnectMax:  *reconnectMax,
		OnTick:        broadcast.PushTick,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := client.Run(ctx); err != nil && ctx.Err() == nil {
			logrus.Errorf("[ts_proxy] tushare client: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logrus.Info("[ts_proxy] shutting down")
	cancel()
	_ = srv.Shutdown(context.Background())
}

func defaultEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, _ := strconv.Atoi(v)
		return n
	}
	return def
}

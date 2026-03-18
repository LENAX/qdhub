// ts_proxy_diagnose is a standalone diagnostic tool to test connectivity to the ts_proxy forwarding service.
// Usage: go run ./ts_proxy_diagnose -addr ws://<host>:8888/realtime [-rsa-pub /path/to/server_public.pem]
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"qdhub/ts_proxy/crypto"

	"github.com/gorilla/websocket"
)

const listenDur = 2 * time.Second

func main() {
	addr := flag.String("addr", "", "WebSocket address (e.g. ws://host:8888/realtime)")
	rsaPubPath := flag.String("rsa-pub", os.Getenv("RSA_PUBLIC_KEY_PATH"), "Optional: server RSA public key path for scheme B key exchange")
	flag.Parse()

	if *addr == "" {
		fmt.Fprintln(os.Stderr, "usage: ts_proxy_diagnose -addr ws://<host>:port/path [-rsa-pub /path/to/server_public.pem]")
		os.Exit(1)
	}

	start := time.Now()
	conn, _, err := websocket.DefaultDialer.Dial(*addr, nil)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Printf("CONNECT_FAIL\t%v\t(elapsed %v)\n", err, elapsed)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("CONNECT_OK\t(elapsed %v)\n", elapsed)

	if *rsaPubPath == "" {
		fmt.Println("SCHEME_B_SKIP\t(no -rsa-pub, only connectivity checked)")
		return
	}

	pub, err := crypto.LoadRSAPublicKeyFromFile(*rsaPubPath)
	if err != nil {
		fmt.Printf("RSA_PUB_LOAD_FAIL\t%v\n", err)
		os.Exit(1)
	}
	aesKey, err := crypto.GenerateKey(32)
	if err != nil {
		fmt.Printf("AES_KEY_GEN_FAIL\t%v\n", err)
		os.Exit(1)
	}
	encKey, err := crypto.EncryptAESKeyWithRSA(pub, aesKey)
	if err != nil {
		fmt.Printf("RSA_ENCRYPT_FAIL\t%v\n", err)
		os.Exit(1)
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.BinaryMessage, encKey); err != nil {
		fmt.Printf("SEND_KEY_FAIL\t%v\n", err)
		os.Exit(1)
	}
	fmt.Println("SCHEME_B_KEY_SENT")

	cipher, err := crypto.NewSessionCipher(aesKey)
	if err != nil {
		fmt.Printf("SESSION_CIPHER_FAIL\t%v\n", err)
		os.Exit(1)
	}
	deadline := time.Now().Add(listenDur)
	conn.SetReadDeadline(deadline)
	frameNum := 0
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		plain, err := cipher.Decrypt(msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "DECRYPT_FAIL\t%v\n", err)
			continue
		}
		frameNum++
		fmt.Printf("--- frame %d (%dB) ---\n%s\n", frameNum, len(plain), string(plain))
	}
	if frameNum == 0 {
		fmt.Printf("READ_FRAME_FAIL\tno frame received within %v\n", listenDur)
		os.Exit(1)
	}
	fmt.Printf("DIAGNOSE_OK\t%d frame(s) in %v\n", frameNum, listenDur)
}

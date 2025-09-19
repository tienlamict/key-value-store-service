package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	Version     = "KV/1.0"
	DefaultAddr = "127.0.0.1:5050"
)

func main() {
	addr := DefaultAddr
	if len(os.Args) > 1 && os.Args[1] != "" {
		addr = os.Args[1]
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("connect error:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("[KVSS Client] connected %s\n", addr)
	fmt.Println(`Type commands without version.....`)

	go func() {
		// reader for server responses
		rc := bufio.NewReader(conn)
		for {
			line, err := rc.ReadString('\n')
			if err != nil {
				fmt.Println("[server closed]")
				os.Exit(0)
			}
			fmt.Print("[resp] ", line)
		}
	}()

	// stdin loop
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			return
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// prepend version
		msg := Version + " " + line + "\n"
		if _, err := conn.Write([]byte(msg)); err != nil {
			fmt.Println("write error:", err)
			return
		}
	}
}

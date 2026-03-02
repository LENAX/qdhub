//go:build ignore
// +build ignore

// gen_bcrypt 生成 bcrypt 哈希，用于 006_seed_default_admin 迁移中的默认 admin 密码。
// 默认密码 admin123 需与 server_e2e_test.go 中 e2eAdminPassword 及迁移注释一致。

package main

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

func main() {
	const defaultAdminPassword = "admin123" // 与 e2e 常量 e2eAdminPassword 及 006 迁移一致
	h, _ := bcrypt.GenerateFromPassword([]byte(defaultAdminPassword), bcrypt.DefaultCost)
	fmt.Println(string(h))

	const guestPassword = "guest123" // 与 008_seed_guest_user 迁移一致，仅查看数据
	h2, _ := bcrypt.GenerateFromPassword([]byte(guestPassword), bcrypt.DefaultCost)
	fmt.Println("guest:", string(h2))
}

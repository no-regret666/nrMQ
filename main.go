package main

import (
	"fmt"
	"reflect"
)

func main() {
	// 示例值
	var x int = 42
	var y string = "Hello, Go!"
	var z []string = []string{"a", "b", "c"}

	// 获取类型信息
	fmt.Println("Type of x:", reflect.TypeOf(x)) // int
	fmt.Println("Type of y:", reflect.TypeOf(y)) // string
	fmt.Println("Type of z:", reflect.TypeOf(z)) // []string

	// 使用 Type 的方法
	t := reflect.TypeOf(z)
	fmt.Println("Type name:", t.Name()) // 输出: ""
	fmt.Println("Kind:", t.Kind())      // 输出: slice
}

package Util

import (
	"math/rand"
	"time"
)

// 生成指定范围内的随机整数 [min, max]
func RandomInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min+1) + min
}

// 生成指定范围内的随机浮点数 [min, max]
func RandomFloat(min, max float64) float64 {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Float64()*(max-min)
}

// 生成指定范围内的随机不重复的整数数组
func RandomUniqueInts(min, max int, count int) []int {
	rand.Seed(time.Now().UnixNano())
	nums := make([]int, count)
	for i := 0; i < count; i++ {
		nums[i] = rand.Intn(max-min+1) + min
	}
	return nums
}

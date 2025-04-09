package main

import "strconv"

func CheckNums(PartToCon map[string][]string, min, max int, nums int) (ret string, b bool) {
	size := 0
	for k, v := range PartToCon {
		if len(v) < min {
			ret += "the consumer" + k + " responsible " + strconv.Itoa(len(v)) + " part < min(" + strconv.Itoa(min) + ")\n"
		} else if len(v) > max {
			ret += "the consumer" + k + " responsible " + strconv.Itoa(len(v)) + " part > max(" + strconv.Itoa(max) + ")\n"
		}
		size++
	}
	if size != nums {
		ret += "the consumers responsible parts quantity(" + strconv.Itoa(size) + ") != nums(" + strconv.Itoa(nums) + ")\n"
	}

	if ret != "" {
		return ret, false
	}
	return ret, true
}

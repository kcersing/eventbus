package eventbus

import (
	"path"
	"strings"
)

// isWildcardPattern 判断 pattern 是否包含有效的通配符元字符。
//
// 无效 pattern（例如未闭合的 '['）返回 false，订阅会按精确主题处理，
// 与修复前的行为保持兼容。
func isWildcardPattern(pattern string) bool {
	if !strings.ContainsAny(pattern, "*?[\\") {
		return false
	}
	for _, seg := range strings.Split(pattern, ".") {
		if _, err := path.Match(seg, ""); err != nil {
			return false
		}
	}
	return true
}

// matchTopic 按 . 分段匹配；* / ? / [] 只在单个段内生效。
func matchTopic(pattern, topic string) bool {
	return matchSegments(strings.Split(pattern, "."), strings.Split(topic, "."))
}

func matchSegments(pattern, topic []string) bool {
	if len(pattern) != len(topic) {
		return false
	}
	for i := range pattern {
		ok, err := path.Match(pattern[i], topic[i])
		if err != nil || !ok {
			return false
		}
	}
	return true
}

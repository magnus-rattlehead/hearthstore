package storage

// encodeIndexComponent preserves byte order while reserving a zero terminator.
func encodeIndexComponent(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+2)
	for _, value := range raw {
		out = append(out, value)
		if value == 0 {
			out = append(out, 255)
		}
	}
	return out
}

func appendIndexComponent(out, raw []byte) []byte {
	out = append(out, encodeIndexComponent(raw)...)
	return append(out, 0, 0)
}

// takeIndexComponent returns the escaped component and the next component/tail.
func takeIndexComponent(data []byte) ([]byte, []byte, bool) {
	for i := 0; i < len(data); i++ {
		if data[i] != 0 {
			continue
		}
		if i+1 >= len(data) {
			return nil, nil, false
		}
		switch data[i+1] {
		case 0:
			return data[:i], data[i+2:], true
		case 255:
			i++
		default:
			return nil, nil, false
		}
	}
	return nil, nil, false
}

package litetable

// Decode decodes a buffer into a litetable operation message type and returns the payload
func Decode(buf []byte) (Operation, []byte) {
	if len(buf) < 5 { // Minimum length for protocols
		return OperationUnknown, nil
	}

	// Early return based on first byte
	switch buf[0] {
	case
		'R': // READ
		if len(buf) >= 5 && buf[1] == 'E' && buf[2] == 'A' && buf[3] == 'D' && buf[4] == ' ' {
			return OperationRead, buf[5:]
		}
	case 'W': // WRITE
		if len(buf) >= 6 && buf[1] == 'R' && buf[2] == 'I' && buf[3] == 'T' && buf[4] == 'E' && buf[5] == ' ' {
			return OperationWrite, buf[6:]
		}
	case 'D': // DELETE
		if len(buf) >= 7 && buf[1] == 'E' && buf[2] == 'L' && buf[3] == 'E' && buf[4] == 'T' && buf[5] == 'E' && buf[6] == ' ' {
			return OperationDelete, buf[7:]
		}
	case 'C': // CREATE
		if len(buf) >= 7 && buf[1] == 'R' && buf[2] == 'E' && buf[3] == 'A' && buf[4] == 'T' && buf[5] == 'E' && buf[6] == ' ' {
			return OperationCreate, buf[7:]
		}
	}

	return OperationUnknown, nil
}

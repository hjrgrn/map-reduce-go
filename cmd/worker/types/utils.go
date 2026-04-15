package types

func HashFunction(word string, bucket int) int {
	total := 0
	for i, c := range word {
		total += int(c) + i
	}
	return total % bucket
}

package probabilistic

import (
	"crypto/md5"
	"fmt"
	"os"
	"strings"
	"unicode"
)

func ReadFile(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func GetHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res = fmt.Sprintf("%s%b", res, b)
	}
	return res
}

func stopWords() []string {
	return []string{"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
		"are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but",
		"by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from",
		"further", "had", "has", "have", "having", "he", "her", "here", "hers", "him", "his", "how",
		"i", "if", "in", "into", "is", "it", "its", "it's", "just", "me", "more", "most", "my", "myself",
		"no", "nor", "not", "now", "of", "off", "on", "once", "only", "or", "other", "our", "ours",
		"ourselves", "out", "over", "own", "same", "she", "should", "so", "some", "such", "than", "that",
		"the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those",
		"through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what", "when",
		"where", "which", "while", "who", "whom", "why", "with", "would", "you", "your", "yours",
		"yourself", "yourselves"}
}

func CleanText(text string) []string {
	text = strings.ToLower(text)
	var cleanedText strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsSpace(r) {
			cleanedText.WriteRune(r)
		}
	}
	words := strings.Fields(cleanedText.String())
	return words
}

func RemoveStopWords(words []string) []string {
	stopWordsSet := make(map[string]struct{})
	for _, sw := range stopWords() {
		stopWordsSet[sw] = struct{}{}
	}

	var filteredWords []string
	for _, word := range words {
		if _, isStopWord := stopWordsSet[word]; !isStopWord {
			filteredWords = append(filteredWords, word)
		}
	}
	return filteredWords
}

func CountWords(words []string) map[string]int {
	wordCount := make(map[string]int)
	for _, word := range words {
		wordCount[word]++
	}
	return wordCount
}

func GetWordWeights(text string) map[string]int {
	words := CleanText(text)
	filteredWords := RemoveStopWords(words)
	wordWeights := CountWords(filteredWords)
	return wordWeights
}

func ConvertHashToVector(word string, weight int) []int {
	binaryHash := GetHashAsString([]byte(word))

	for len(binaryHash) < 128 {
		binaryHash = "0" + binaryHash
	}

	vector := make([]int, 128)
	for i, bit := range binaryHash {
		if bit == '0' {
			vector[i] = -weight
		} else {
			vector[i] = weight
		}
	}
	return vector
}

func ComputeVectorSum(wordWeights map[string]int) []int {
	vectorSum := make([]int, 128)
	for word, weight := range wordWeights {
		vector := ConvertHashToVector(word, weight)
		for i := 0; i < 128; i++ {
			vectorSum[i] += vector[i]
		}
	}
	return vectorSum
}

func ComputeSimhash(wordWeights map[string]int) uint64 {
	vectorSum := ComputeVectorSum(wordWeights)
	var simhash uint64

	for i, sum := range vectorSum {
		if sum > 0 {
			simhash |= 1 << (127 - i)
		}
	}
	return simhash
}

func HammingDistance(hash1, hash2 uint64) int {
	xor := hash1 ^ hash2
	dist := 0
	for xor > 0 {
		dist++
		xor &= xor - 1
	}
	return dist
}

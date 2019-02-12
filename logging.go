package stream_common

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
)

// PrintError function
func PrintError(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
}

func PrintError2(msg *sarama.ProducerError) {
	fmt.Printf("Failed to produce message: %s", msg.Err)
}
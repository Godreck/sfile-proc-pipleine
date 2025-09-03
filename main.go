package main

import (
	"fmt"
	"log"

	mpp "github.com/your-username/sfile-proc-pipleine/processing_pipeline"
)

func main() {
	pipeline := mpp.NewPipeline(4)

	pipeline.AddProcessingStage(func(data []byte) []byte {
		// Transform data here
		return data
	})

	files := []string{"file1.txt", "file2.txt", "file3.txt"}

	for _, file := range files {
		pipeline.Input <- file
	}
	close(pipeline.Input)

	pipeline.StartWorkers()

	if err := pipeline.HandleErrors(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Processing completed successfully")
}

package processing_pipeline

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

// Pipline struct represent pipline, it stocks fields input (chan string), output (chan Result), errChan (chan error) and worckerNum (int).
type Pipeline struct {
	Input      chan string
	output     chan Result
	errChan    chan error
	worckerNum int
}

// Result struct represent pipline output structure, it stocks filename (string), content ([]byte).
type Result struct {
	filename string
	content  []byte
}

func NewPipeline(worckerNum int) *Pipeline {
	return &Pipeline{
		Input:      make(chan string, worckerNum),
		output:     make(chan Result, worckerNum),
		errChan:    make(chan error, worckerNum),
		worckerNum: worckerNum,
	}
}

func (p *Pipeline) AddProcessingStage(processor func([]byte) []byte) { // что это за записть?
	input := p.output
	output := make(chan Result, p.worckerNum)

	go func() {
		defer close(output)
		for result := range input {
			processed := processor(result.content)
			output <- Result{result.filename, processed}
		}
	}()
	p.output = output
}

func (p *Pipeline) HandleErrors() error {
	var errList []error
	for err := range p.errChan {
		errList = append(errList, err)
	}
	if len(errList) > 0 {
		return fmt.Errorf("multiple errors occured: %v", errList)
	}
	return nil
}

func processFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := make([]byte, 0, 1024)

	for {
		chunk, isPrefix, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		buffer = append(buffer, chunk...)
		if !isPrefix {
			buffer = append(buffer, '\n')
		}
	}
	return buffer, nil
}

func processInChunks(filename string, chunkSize int) chan []byte {
	chunks := make(chan []byte)

	go func() {
		defer close(chunks)

		file, err := os.Open(filename)
		if err != nil {
			return
		}
		defer file.Close()

		buffer := make([]byte, chunkSize)
		for {
			n, err := file.Read(buffer)
			if n > 0 {
				chunks <- buffer[:n]
			}
			if err == io.EOF {
				break
			}
		}
	}()

	return chunks
}

func (p *Pipeline) StartWorkers() {
	var wg sync.WaitGroup

	for i := 0; i < p.worckerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for filename := range p.Input {
				content, err := processFile(filename)
				if err != nil {
					p.errChan <- err
					continue
				}
				p.output <- Result{filename, content}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(p.output)
		close(p.errChan)
	}()
}

func (p *Pipeline) ProcessLargeFiles(filename string) error {
	const maxCunkSize = 1024 * 1024
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, maxCunkSize)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		chunk := make([]byte, n)
		copy(chunk, buffer[:n])

		select {
		case p.Input <- string(chunk):
		case err := <-p.errChan:
			return err
		}
	}

	return nil
}

type Processor interface {
	Process([]byte) ([]byte, error)
}

type TextProcessor struct {
	transformFunc func(string) string
}

func (tp *TextProcessor) Process(input []byte) ([]byte, error) {
	text := string(input)
	processed := tp.transformFunc(text)
	return []byte(processed), nil
}

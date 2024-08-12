package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var check = false
var filePath = ""
var maxFileSize = 20
var maxPool = 5
var splitElmentReturn byte = '\n'
var splitElmentSpace byte = ' '
var splitElment = "return"
var output = ""
var outputName = "part{%d}.{%s}"
var splitElmentMap = map[string]byte{
	"space": splitElmentSpace,
	"return": splitElmentReturn,
}
var rootCmd = &cobra.Command{
	Use:   "splitFast",
	Short: "快速分割文件",
	Long:  `可以设定分割大小，精确分割文件，也可以设置分割符，按照分割符尽量接近分割大小。异步分割，适合大文件分割`,
	Run: func(cmd *cobra.Command, args []string) {
		if filePath == "" {
			fmt.Println("filePath is required")
			return
		}
		if splitElment != "none" && splitElment != "return" && splitElment != "space" {
			fmt.Println("splitElment must be one of 'none', 'return', 'space'")
			return
		}
		maxFileSize = maxFileSize * 1024 * 1024 * 1024
		print("start at ", time.Now().Format("2006-01-02 15:04:05"))
		if check {
			err := vaildPartFile(filePath)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("File check successfully.")
			}
			return
		} else {
			err := scanFile(filePath)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("File split successfully.")
			}
		}
		print("end at ", time.Now().Format("2006-01-02 15:04:05"))
	},
}

func readPartOfFile(channel chan struct{}, filePath string, wg *sync.WaitGroup, start, end, partIndex int64) error {
	channel <- struct{}{}
	defer func() {
		<-channel // 方法运行结束释放信号量
	}()
	defer wg.Done()
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	dirName := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)
	if output != "" {
		dirName = output
	}
	partFileName := filepath.Join(dirName, fmt.Sprintf("part%d.%s", partIndex, baseName))
	partFile, err := os.Create(partFileName)
	if err != nil {
		return fmt.Errorf("error creating new part file: %v", err)
	}
	defer partFile.Close()
	_, err = file.Seek(start, io.SeekStart)
	if err != nil {
		fmt.Printf("Error seeking to start position %d: %v\n", start, err)
		return err
	}
	buffer := make([]byte, 67108864)
	current := start
	for {
		if current + 67108864 > end {
            buffer = make([]byte, end - current)
        }
        _, err := file.Read(buffer)
        // 控制条件,根据实际调整
        if err != nil && err != io.EOF {
			fmt.Printf("Error reading file: %v\n", err)
            return err
        }
		_, err = partFile.Write(buffer)
		if err != nil {
			fmt.Printf("Error writing to part file: %v\n", err)
			return err
		}
        if current + 67108864 > end {
            break
        }
		current += 67108864
    }
	return nil
	
}

func scanFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Error stating file '%s': %v\n", filePath, err)
		return err
	}
	splitchunk, err := findBoundary(file, int64(maxFileSize), fileInfo.Size())
	// 创建一个bufio.Scanner来逐行读取文件
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	var wg sync.WaitGroup
	channel := make(chan struct{}, maxPool)
	for index, start := range splitchunk {
		wg.Add(1)
		var end int64
		if index + 1 > len(splitchunk) - 1 {
			end = fileInfo.Size()
		} else {
			end = splitchunk[index+1]
		}
		go readPartOfFile(channel, filePath, &wg, start, end, int64(index))
	}
	wg.Wait()
	return nil
}

func findBoundary(file *os.File, chunkSize int64, filesize int64) ([]int64, error) {
	splitSize := make([]int64, 0)
	start := int64(0)
	for {
		splitSize = append(splitSize, start)
		start += chunkSize
		if start > filesize {
			break
		}
		if splitElment == "none" {
			continue
		}
		_, err := file.Seek(start, io.SeekStart)
		if err != nil {
			return splitSize, err
		}
		buf := make([]byte, 1024000)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			return splitSize, err
		}
		for i := range buf[:n] {
			if buf[i] == splitElmentMap[splitElment] {
				// 返回换行符的字节偏移量
				start = start + int64(i) + 1
				break
			}
		}
	}
	return splitSize, nil
}

func vaildPartFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dirName := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)
	partIndex := 0
	partFileName := filepath.Join(dirName, fmt.Sprintf(outputName, partIndex, baseName))
	index := 0
	partLineIndex := 0
	partfile, err := os.Open(partFileName)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	lineReader := bufio.NewReader(partfile)
	defer file.Close()
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			return fmt.Errorf("error line is empty")
		}
		partline, _, err := lineReader.ReadLine()
		partLineIndex++
		if err == io.EOF {
			partfile.Close()
			partIndex++
			partLineIndex = 0
			partFileName = filepath.Join(dirName, fmt.Sprintf("part%d.%s", partIndex, baseName))
			partfile, err = os.Open(partFileName)
			if err != nil {
				return fmt.Errorf("error opening file: %v", err)
			}
			lineReader = bufio.NewReader(partfile)
			partline, _, _ = lineReader.ReadLine()
		}
		if line != string(partline){
			fmt.Println("error index:", index, "partLineIndex:", partLineIndex, "line:", line, "partline:", string(partline))
			fmt.Println("error index:", index, "partFileName:", partFileName)
			return fmt.Errorf("error index: %d, line: %s, partline: %s", index, line, string(partline))
		}
		index++
	}
	if partfile != nil {
		partfile.Close()
	}
	return nil
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&filePath, "filePath", "f", "", "需要分割的对象文件路径, 必填")
	rootCmd.MarkFlagRequired("filePath")
	rootCmd.PersistentFlags().BoolVarP(&check, "check", "c", false, "是否检查模式, 检测分割后的文件与原文件是否一致, 默认为false, 即分割模式")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "", "分割后的文件目录，默认与分割对象文件相同")
	rootCmd.PersistentFlags().IntVarP(&maxFileSize, "bytes=SIZE", "b", maxFileSize, "分割后的子文件大小, 单位为GB, 注意: 如果splitElment为none, 会精确分割的子文件大小; 如果有分割符(return, space), 文件会以分割符分割，文件大小尽量为接近此大小")
	rootCmd.PersistentFlags().IntVarP(&maxPool, "pool", "p", maxPool, "最大并发数")
	rootCmd.PersistentFlags().StringVarP(&outputName, "splited filename", "s", outputName, "分割后的子文件名, 其中{%d}为子文件序号，{%s}为分割对象文件名")
	rootCmd.PersistentFlags().StringVarP(&splitElment, "splitElment", "e", splitElment, "分割时会根据splitElment来分割，可选(none, return, space)，当为none时，以精确的bytesSize分割，当为return时，以近似bytesSize并且寻找最近换行符分割，当为space时以近似bytesSize并且寻找最近空格分割")
}

func main() {
	rootCmd.Execute()
}
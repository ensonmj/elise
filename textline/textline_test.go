package textline

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
)

type LineWorker struct{}

func (w *LineWorker) Process(line []byte) (interface{}, error) {
	return line, nil
}

type FileWorker struct {
	buf []string
}

func (w *FileWorker) PrepareOnce() error {
	return nil
}

func (w *FileWorker) BeforeWrite(fn string) error {
	return nil
}

func (w *FileWorker) PreWrite(row int) error {
	return nil
}

func (w *FileWorker) Write(data interface{}) error {
	w.buf = append(w.buf, string(data.([]byte)))
	return nil
}

func (w *FileWorker) PostWrite(row int) error {
	return nil
}

func (w *FileWorker) AfterWrite() error {
	return nil
}

func randStr(maxLen int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	length := rand.Int()%maxLen + 1 // make sure length > 0
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = alphanum[rand.Int63()%int64(len(alphanum))]
	}
	return string(buf)
}

func randStrSlice(num, maxLen int) []string {
	rand.Seed(9893489983248324)
	var strSlice []string
	for i := 0; i < num; i++ {
		strSlice = append(strSlice, randStr(maxLen))
	}
	return strSlice
}

func TestSequence(t *testing.T) {
	log.SetLevel(log.Level(0))

	file, _ := ioutil.TempFile(os.TempDir(), "textline")
	defer os.Remove(file.Name())

	inNum := 1000
	input := randStrSlice(inNum, 20)
	for _, str := range input {
		file.WriteString(str + "\n")
	}

	fw := &FileWorker{}
	tlm := New(file.Name(), 10, &LineWorker{}, fw)
	tlm.FeedLine()
	tlm.Wait()

	outNum := len(fw.buf)
	if outNum != inNum {
		t.Fatalf("Expected %v, but got %v", inNum, outNum)
	} else {
		t.Logf("Expected %v", inNum)
	}
	for i, str := range fw.buf {
		in := input[i]
		if str != in {
			t.Fatal("Expected %v, but got %v", in, str)
		} else {
			t.Logf("Expected %v", str)
		}
	}
}

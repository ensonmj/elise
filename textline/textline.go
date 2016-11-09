package textline

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TLManager struct {
	LineProcessor
	FileStorage
	inPath       string
	numProc      int
	feedChan     chan LineInfo
	ctx          context.Context
	cancel       context.CancelFunc
	started      time.Time
	procEG       errgroup.Group
	fileEG       errgroup.Group
	fileInfoList []*FileInfo
}

type LineProcessor interface {
	Process(line []byte) (interface{}, error)
}

type FileInfo struct {
	FileStorage
	FilePath     string
	procLineCnt  int32
	writeLineCnt int
	writerChan   chan LineInfo
	started      time.Time
}

func (fi *FileInfo) writeLine(data interface{}) error {
	if data == nil {
		// don't write Process err line
		return nil
	}

	if err := fi.PreWrite(fi.writeLineCnt); err != nil {
		log.WithError(err).Warn("Failed to prewrite")
		return err
	}
	if err := fi.Write(data); err != nil {
		log.WithError(err).Warn("Failed to write file")
		return err
	}
	if err := fi.PostWrite(fi.writeLineCnt); err != nil {
		log.WithError(err).Warn("Failed to postwrite")
		return err
	}
	fi.writeLineCnt++

	return nil
}

func (fi *FileInfo) drainChan() {
	go func() {
		for range fi.writerChan {
		}
	}()
}

type FileStorage interface {
	PrepareOnce() error
	BeforeWrite(fn string) error
	PreWrite(row int) error
	Write(data interface{}) error
	PostWrite(row int) error
	AfterWrite() error
}

type LineInfo struct {
	*FileInfo
	Index int
	Bytes []byte
	Data  interface{}
}

type LineInfoSlice []LineInfo

func (lis LineInfoSlice) Len() int {
	return len(lis)
}

func (lis LineInfoSlice) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}

// ascending order
func (lis LineInfoSlice) Less(i, j int) bool {
	return lis[i].Index < lis[j].Index
}

func New(path string, num int, lineProc LineProcessor, fileSave FileStorage) *TLManager {
	m := &TLManager{
		LineProcessor: lineProc,
		FileStorage:   fileSave,
		inPath:        path,
		numProc:       num,
		feedChan:      make(chan LineInfo, num),
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.registerLineProc()
	return m
}

func (m *TLManager) registerLineProc() {
	for i := 0; i < m.numProc; i++ {
		index := i
		m.procEG.Go(func() error {
			for {
				select {
				case lineInfo, ok := <-m.feedChan:
					if !ok {
						log.WithField("index", index).Debug("Worker exit")
						return nil
					}

					// ignore error here, just for keep input sequence
					data, err := m.Process(lineInfo.Bytes)
					if err != nil {
						log.WithFields(log.Fields{
							"index":    index,
							"filePath": lineInfo.FilePath,
							"row":      lineInfo.Index,
							"err":      err,
						}).Warn("Failed to process one line")
					}
					atomic.AddInt32(&lineInfo.procLineCnt, 1)
					// data is nil is error happend
					lineInfo.Data = data
					lineInfo.writerChan <- lineInfo
				}
			}

			return nil
		})
	}
}

func (m *TLManager) FeedLine() error {
	defer close(m.feedChan)
	m.started = time.Now()

	err := m.PrepareOnce()
	if err != nil {
		log.WithError(err).Warn("Failed to prepare once")
		return err
	}

	if m.inPath == "-" {
		return m.readFile(m.inPath)
	}

	err = filepath.Walk(m.inPath, func(path string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}
		log.WithFields(log.Fields{
			"path":     path,
			"fileName": fi.Name(),
		}).Debug("Get file path for read")

		return m.readFile(path)
	})

	if err != nil {
		log.WithFields(log.Fields{
			"inPath": m.inPath,
			"err":    err,
		}).Warn("Failed to walk dir")
		return err
	}

	log.WithFields(log.Fields{
		"inPath":  m.inPath,
		"elapsed": time.Since(m.started),
	}).Info("Finished to feed all text line")
	return nil
}

func (m *TLManager) readFile(path string) error {
	var f *os.File
	var err error
	if path == "-" {
		f = os.Stdin
	} else {
		if f, err = os.Open(path); err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Warn("Failed to open file for read")
			return err
		}
		defer f.Close()
	}

	fi := &FileInfo{
		FileStorage: m.FileStorage,
		FilePath:    path,
		writerChan:  make(chan LineInfo, m.numProc),
		started:     time.Now(),
	}
	m.registerPostProc(fi)
	m.fileInfoList = append(m.fileInfoList, fi)

	var lineCount int
	sc := bufio.NewScanner(f)
	sc.Buffer([]byte{}, 2*1024*1024) // default 64k, change to 2M
SCANLOOP:
	for sc.Scan() {
		select {
		case <-m.ctx.Done():
			log.WithFields(log.Fields{
				"inPath": fi.FilePath,
				"err":    m.ctx.Err(),
			}).Warn("Partial finished to process one file")
			break SCANLOOP
		default:
			lineInfo := LineInfo{
				FileInfo: fi,
				Index:    lineCount,
				Bytes:    make([]byte, len(sc.Bytes())),
			}
			copy(lineInfo.Bytes, sc.Bytes())
			m.feedChan <- lineInfo
			lineCount++
		}
	}

	if err := sc.Err(); err != nil {
		log.WithFields(log.Fields{
			"file":        fi.FilePath,
			"readLineCnt": lineCount,
			"elapsed":     time.Since(fi.started),
			"err":         err,
		}).Warn("Failed to read line from file")
		return err
	}

	log.WithFields(log.Fields{
		"file":        fi.FilePath,
		"readLineCnt": lineCount,
		"elapsed":     time.Since(fi.started),
	}).Info("Finished to read one file")
	return nil
}

func (m *TLManager) registerPostProc(fi *FileInfo) {
	m.fileEG.Go(func() error {
		if err := fi.BeforeWrite(fi.FilePath); err != nil {
			m.cancel()
			fi.drainChan()
			log.WithError(err).Warn("Failed to beforewrite")
			return err
		}

		var cache LineInfoSlice
		var currIndex int
		for line := range fi.writerChan {
			// keep output follow input sequence
			if line.Index != currIndex {
				cache = append(cache, line)
				sort.Sort(cache)
				continue
			}
			currIndex++

			if err := fi.writeLine(line.Data); err != nil {
				log.WithFields(log.Fields{
					"lineIndex": line.Index,
					"err":       err,
				}).Warn("Failed to writeLine")
				m.cancel()
				fi.drainChan()
				return err
			}

			// read from cache
			cacheIndex := 0
			for _, line := range cache {
				if line.Index != currIndex {
					break
				}
				cacheIndex++
				currIndex++

				if err := fi.writeLine(line.Data); err != nil {
					log.WithFields(log.Fields{
						"lineIndex": line.Index,
						"err":       err,
					}).Warn("Failed to writeLine")
					m.cancel()
					fi.drainChan()
					return err
				}
			}
			if cacheIndex > 0 {
				cache = cache[:copy(cache, cache[cacheIndex:])]
			}
		}

		if err := fi.AfterWrite(); err != nil {
			log.WithError(err).Warn("Failed to afterwrite")
			m.cancel()
			return err
		}

		log.WithFields(log.Fields{
			"file":         fi.FilePath,
			"procLineCnt":  atomic.LoadInt32(&fi.procLineCnt),
			"writeLineCnt": fi.writeLineCnt,
			"elapsed":      time.Since(fi.started),
		}).Info("Finished to save result for one file")
		return nil
	})
}

func (m *TLManager) Wait() {
	m.procEG.Wait()
	log.WithFields(log.Fields{
		"inPath":  m.inPath,
		"elapsed": time.Since(m.started),
	}).Debug("Finished to process all text line")

	for _, fi := range m.fileInfoList {
		close(fi.writerChan)
	}
	m.fileEG.Wait()
	log.WithFields(log.Fields{
		"inPath":  m.inPath,
		"elapsed": time.Since(m.started),
	}).Debug("Finished to write all result")

	log.WithFields(log.Fields{
		"inPath":  m.inPath,
		"elapsed": time.Since(m.started),
	}).Info("Finished all work")
}

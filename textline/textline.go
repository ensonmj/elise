package textline

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
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
	FilePath   string
	LineCnt    uint64
	writerChan chan LineInfo
	started    time.Time
}

type FileStorage interface {
	PrepareOnce() error
	BeforeWrite(fn string) error
	PreWrite(num int) error
	Write(data interface{}) error
	PostWrite(num int) error
	AfterWrite() error
}

type LineInfo struct {
	*FileInfo
	Index uint64
	Bytes []byte
	Data  interface{}
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

					data, err := m.Process(lineInfo.Bytes)
					if err != nil {
						log.WithFields(log.Fields{
							"index":    index,
							"filePath": lineInfo.FilePath,
							"row":      lineInfo.Index,
							"err":      err,
						}).Warn("Failed to process one line")
						continue
					}

					atomic.AddUint64(&lineInfo.LineCnt, 1)
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

	var lineCount uint64
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
		// don't return immediately if error occur
		// we must drain 'writerChan' before exit
		var err error
		if err = fi.BeforeWrite(fi.FilePath); err != nil {
			log.WithError(err).Warn("Failed to beforewrite")
			m.cancel()
		}

		lineCount := 0
		for line := range fi.writerChan {
			if err != nil {
				// drain 'writerChan'
				continue
			}

			if err = fi.PreWrite(lineCount); err != nil {
				log.WithError(err).Warn("Failed to prewrite")
				m.cancel()
				continue
			}
			if err = fi.Write(line.Data); err != nil {
				log.WithError(err).Warn("Failed to write file")
				m.cancel()
				continue
			}
			if err = fi.PostWrite(lineCount); err != nil {
				log.WithError(err).Warn("Failed to postwrite")
				m.cancel()
				continue
			}
			lineCount++
		}

		if err = fi.AfterWrite(); err != nil {
			log.WithError(err).Warn("Failed to afterwrite")
			m.cancel()
		}

		log.WithFields(log.Fields{
			"file":         fi.FilePath,
			"writeLineCnt": atomic.LoadUint64(&fi.LineCnt),
			"elapsed":      time.Since(fi.started),
		}).Info("Finished to save result for one file")
		return err
	})
}

func (m *TLManager) Wait() {
	m.procEG.Wait()

	for _, fi := range m.fileInfoList {
		close(fi.writerChan)
	}
	m.fileEG.Wait()

	log.WithFields(log.Fields{
		"inPath":  m.inPath,
		"elapsed": time.Since(m.started),
	}).Info("Finished all work")
}

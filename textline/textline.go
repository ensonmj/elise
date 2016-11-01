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
	LineProc
	FileSave
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

type LineProc interface {
	Process(line []byte) (interface{}, error)
}

type FileInfo struct {
	FileSave
	FileName   string
	LineCnt    uint64
	writerChan chan LineInfo
	started    time.Time
}

type FileSave interface {
	PrepareOnce() error

	BeforeWrite(fn string) error
	Prepare(num int) error
	Write(data interface{}) error
	AfterWrite(fn string) error
}

type LineInfo struct {
	*FileInfo
	Index uint64
	Bytes []byte
	Data  interface{}
}

func newFileInfo(fn string, num int, fileSave FileSave) *FileInfo {
	return &FileInfo{
		FileSave:   fileSave,
		FileName:   fn,
		writerChan: make(chan LineInfo, num),
	}
}

func New(path string, num int, lineProc LineProc, fileSave FileSave) *TLManager {
	m := &TLManager{
		LineProc: lineProc,
		FileSave: fileSave,
		inPath:   path,
		numProc:  num,
		feedChan: make(chan LineInfo, num),
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
							"index": index,
							"err":   err,
						}).Debug("Failed to process one line")
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
		fi := newFileInfo("-", m.numProc, m.FileSave)
		return m.readFile(os.Stdin, fi)
	}

	err = filepath.Walk(m.inPath, func(path string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}
		log.WithFields(log.Fields{
			"path":     path,
			"fileName": fi.Name(),
		}).Debug("Get file path for read")

		f, err := os.Open(path)
		if err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Warn("Failed to open file for read")
			return err
		}
		defer f.Close()

		nfi := newFileInfo(path, m.numProc, m.FileSave)
		return m.readFile(f, nfi)
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

func (m *TLManager) readFile(f *os.File, fi *FileInfo) error {
	m.registerPostProc(fi)
	fi.started = time.Now()
	m.fileInfoList = append(m.fileInfoList, fi)

	var lineCount uint64
	sc := bufio.NewScanner(f)
	sc.Buffer([]byte{}, 2*1024*1024) // default 64k, change to 2M
SCANLOOP:
	for sc.Scan() {
		select {
		case <-m.ctx.Done():
			log.WithFields(log.Fields{
				"inPath": fi.FileName,
				"err":    m.ctx.Err(),
			}).Warn("Partial finished to process file")
			break SCANLOOP
		default:
			lineInfo := LineInfo{FileInfo: fi, Index: lineCount}
			copy(lineInfo.Bytes, sc.Bytes())
			m.feedChan <- lineInfo
			lineCount++
		}
	}

	if err := sc.Err(); err != nil {
		log.WithFields(log.Fields{
			"file":        fi.FileName,
			"readLineCnt": lineCount,
			"elapsed":     time.Since(fi.started),
			"err":         err,
		}).Warn("Failed to read line from file")
		return err
	}
	log.WithFields(log.Fields{
		"file":        fi.FileName,
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
		if err = fi.BeforeWrite(fi.FileName); err != nil {
			log.WithError(err).Warn("Failed to prepare file")
			m.cancel()
		}

		lineCount := 0
		for line := range fi.writerChan {
			if err != nil {
				// drain 'writerChan'
				continue
			}

			if err = fi.Prepare(lineCount); err != nil {
				log.WithError(err).Warn("Failed to prepare file")
				m.cancel()
				continue
			}
			if err = fi.Write(line.Data); err != nil {
				log.WithError(err).Warn("Failed to write file")
				m.cancel()
				continue
			}
			lineCount++
		}

		if err = fi.AfterWrite(fi.FileName); err != nil {
			log.WithError(err).Warn("Failed to prepare file")
			m.cancel()
		}

		log.WithFields(log.Fields{
			"file":         fi.FileName,
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

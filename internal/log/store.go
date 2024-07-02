package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian // バイナリのエンコーディング形式
)

const (
	lenWidth = 8 // Appendするbyteのサイズは、必ず8バイト=2^64で統一する
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer // バッファを利用したI/Oを行ってくれる構造体。効率的な書き込みが可能
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// 引数のbyteのサイズ→引数のbyteの順でファイルに書き込む
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// 引数pのサイズを8バイトで表して書き込む。binary.Writeは、数値等を一発でバイナリに変換して書き込めるので便利
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// 引数pの書き込み。wは書き込んだバイトのサイズ
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// pのサイズだけでなく、pの長さのサイズも忘れずに足す
	w += lenWidth

	// これまでに書き込んだ合計値
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// まだ書き込まれていない、バッファにあるログを書き込む
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// まずはエントリを読み込む
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// エントリで受け取ったサイズ分のバイトをログから読み込む
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// 引数pにログのバイトを格納
	return s.File.ReadAt(p, off)
}

// 書き込み先のログファイルを閉じる
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

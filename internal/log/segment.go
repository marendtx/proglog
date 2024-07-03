package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "proglog/api/v1"

	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		// もし何もindexに書き込まれていないのであれば、次に書き込まれるべきオフセットはbaseOffset
		s.nextOffset = baseOffset
	} else {
		// もし何か書き込まれているのであれば、次に書き込まれるべきオフセットは、取得できた末尾のオフセットに、baseOffsetと1を加算した値
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// curは書き込むオフセット
	cur := s.nextOffset

	// record構造体をbyteにエンコード
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// segmentでの書き込み方について。
	// まず、連番の値（オフセットと呼ぶことにする）を用意する。
	// 次に、storeに値を書き込む。（storeに値を書き込んだら、書き込んだ位置を返す）
	// この位置と、オフセットの組み合わせを、indexに書き込む。
	// storeにもindexにも書き込んだらオフセットを加算する。
	// これが一連の書き込みの流れ。

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// インデックスのオフセットは、baseOffsetからの相対
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	// 次に書き込まれるべきオフセットを加算。ここの処理で書き込んだので。
	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	// 相対位置のオフセットにより、indexからポジションを取得
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// 取得したポジションで、storeから値を取得
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// プロトコルバッファのRecordオブジェクトに格納
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// indexは、4バイト分のオフセットと、8バイト分のポジション、
// 計12バイトが並ぶことにする
// オフセット * entWidthで、実際のポジションが書かれたバイトにたどり着ける

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // indexのサイズをどんどん記録していく
}

func newIndex(f *os.File, c Config) (*index, error) {
	// 引数で受け取ったファイルから、index構造体を生成
	idx := &index{
		file: f,
	}

	// ファイルの情報を取得し、index構造体のサイズに入れておく
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// ファイルの元のサイズ記録。おそらく0だが。。
	idx.size = uint64(fi.Size())

	// ファイルのサイズを、メモリマップするために(おそらく1024byteに)変換する
	// つまり、メモリの1024byte分をindexとして使う
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// gommapによるメモリマップ作成。
	// メモリマップとは、ファイルの内容を直接メモリにマッピングすることで、高速なI/Oを実現する技術
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),                      // メモリにマップするファイル
		gommap.PROT_READ|gommap.PROT_WRITE, // メモリマップされた領域を、読み書き可能モードにする。
		gommap.MAP_SHARED,                  // メモリマップされた領域を、他のプロセスと共有できるようにする
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// メモリマップされた内容をファイルディスクリプタを介してファイルに書き込む
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// メモリマップされた領域の解放
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	// ファイルをディスクに書き込む。エディタでの保存のイメージ
	if err := i.file.Sync(); err != nil {
		return err
	}

	// 記録しておいた元のファイルサイズに戻す
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// inは、indexのエントリで読み取りたい箇所。-1なら最後。そうでないなら何個目のエントリか。
	// 読み取りたい箇所を、変数outに一時格納
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// outにエントリのサイズの12をかけ、何個目のエントリか？を、バイト数に変換
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// 読み取りたかった箇所のオフセットをoutに格納
	out = enc.Uint32(i.mmap[pos : pos+offWidth])

	// 読み取りたかった箇所のポジションをposに格納
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// エントリを書き込めるかどうか
	if i.isMaxed() {
		return io.EOF
	}

	// オフセット分をバイナリにして書き込む。
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// 実際のポジションをバイナリにして書き込む
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// 書き込んだエントリ分のサイズを更新
	i.size += uint64(entWidth)

	return nil
}

func (i *index) isMaxed() bool {
	// エントリを書き込もうとした際、確保済みのメモリマップのサイズを超過しているかどうか。
	// つまり、indexファイルには、メモリマップ以上のバイトを書き込めないようにする
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}

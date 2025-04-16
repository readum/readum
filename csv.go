package repository

import (
	"iter"
)

// CSVReaderFactoryは、ファイルパスを受け取りCSVReaderインターフェースを返すファクトリ関数型。
// main.goなどで依存性注入(DI)することで、usecase層で動的にCSVファイルを開くことができる。
// これにより、CSVファイルのパスが実行時に決まる場合でも柔軟にCSVReaderを生成できる。
type CSVReaderFactory func(path string) (CSVReader, error)

// CSVReaderインターフェース
type CSVReader interface {
	Read() iter.Seq2[[]string, error]
	Close() error
}

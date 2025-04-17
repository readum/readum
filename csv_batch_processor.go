package usecase

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go-server/domain/logic"
	"go-server/domain/model"
	"go-server/domain/proxy"
	"go-server/domain/repository"
	"go-server/pkg/errors"
	"go-server/pkg/log"
)

// S3バケット名とCSVファイルキーは定数として定義
const (
	csvBucket = "your-bucket-name"
	csvKey    = "your-csv-file.csv"
)

// CSVBatchAttrsはバッチ処理の共通属性
// ReceiptNoなど、入力・出力で共通するフィールドをまとめる
type CSVBatchAttrs struct {
	RowNumber    int  // 現在の処理行番号（1始まり）
	Completed    bool // 処理完了フラグ
	SuccessCount int  // 成功件数
	FailedCount  int  // 失敗件数
}

// CSVBatchInputはバッチ処理の入力DTO
// ReceiptNoなどバッチ識別子はInput/Resultで個別に持つ
type CSVBatchInput struct {
	ReceiptNo string
	CSVBatchAttrs
	// ...入力専用フィールド...
}

// CSVBatchResultはバッチ処理の結果DTO
type CSVBatchResult struct {
	ReceiptNo string
	CSVBatchAttrs
	// ...出力専用フィールド...
}

// CSVBatchProcessorはCSV一括処理のユースケースを担う
type CSVBatchProcessor struct {
	storage          proxy.StorageProxy
	csvReaderFactory repository.CSVReaderFactory
	recordRepo       repository.CSVRecordRepository
	resultRepo       repository.ProcessResultRepository
	detailRepo       repository.ProcessResultDetailRepository
}

// NewCSVBatchProcessorはCSVBatchProcessorを生成する
func NewCSVBatchProcessor(
	storage proxy.StorageProxy,
	csvReaderFactory repository.CSVReaderFactory,
	recordRepo repository.CSVRecordRepository,
	resultRepo repository.ProcessResultRepository,
	detailRepo repository.ProcessResultDetailRepository,
) *CSVBatchProcessor {
	return &CSVBatchProcessor{
		storage:          storage,
		csvReaderFactory: csvReaderFactory,
		recordRepo:       recordRepo,
		resultRepo:       resultRepo,
		detailRepo:       detailRepo,
	}
}

// ProcessCSVBatchはS3からCSVをダウンロードし、バリデーション・DynamoDB登録・結果集計を行う
func (p *CSVBatchProcessor) ProcessCSVBatch(
	ctx context.Context,
	input CSVBatchInput,
) (CSVBatchResult, error) {
	// 入力から結果DTOを初期化
	result := CSVBatchResult{
		CSVBatchAttrs: input.CSVBatchAttrs,
		ReceiptNo:     input.ReceiptNo,
	}
	// Completedがtrueなら即return
	if input.Completed {
		result.Completed = true
		return result, nil
	}
	// 一時ファイルパスを生成
	tempPath := filepath.Join(os.TempDir(), csvKey)
	// S3からCSVファイルをダウンロード
	if err := p.storage.DownloadFile(ctx, tempPath, csvBucket, csvKey); err != nil {
		return result, errors.Wrapf(err, errors.Storage, "CSVダウンロード失敗: %s/%s", csvBucket, csvKey)
	}
	defer os.Remove(tempPath)

	// CSVReaderを生成
	csvReader, err := p.csvReaderFactory(tempPath)
	if err != nil {
		return result, errors.Wrap(err, errors.ServerProcFail, "CSVReader生成失敗")
	}
	defer csvReader.Close()

	var (
		records            []*model.CSVRecord
		details            []*model.ProcessResultDetail
		firstInsurerNumber string
	)
	const maxTransactItems = 100

	// RowNumberは1始まりなので、RowNumber-1件スキップ
	iter := csvReader.SkipN(input.RowNumber - 1)

	// CSVを1行ずつストリームで処理
	for row, err := range iter {
		// コンテキストキャンセル・タイムアウト時は処理中断
		if ctx.Err() != nil {
			break
		}
		p.processCSVRow(row, err, result.RowNumber, &result, &records, &details, &firstInsurerNumber)
		result.RowNumber++

		// バッチサイズ判定（detailsの件数のみで判定）
		if len(details) < maxTransactItems {
			continue
		}
		if len(records) > 0 {
			if err := p.recordRepo.TransactPut(ctx, records, nil, nil); err != nil {
				// DynamoDB登録失敗時、正常扱いだったdetailsをエラーに更新
				details = model.ProcessResultDetails(details).UpdateOnRecordError(records)
				log.Error(ctx, err, "CSVRecord一括登録失敗")
				// エラーでも処理継続
			}
			records = nil
		}
		if len(details) > 0 {
			if err := p.detailRepo.TransactPut(ctx, details); err != nil {
				log.Error(ctx, err, "ProcessResultDetail一括登録失敗")
				// エラーでも処理継続
			}
			details = nil
		}
	}

	// 最後のバッチ（ProcessResult付き）を登録
	if len(records) > 0 || len(details) > 0 {
		resultStatus := "成功"
		var errMsg string
		if result.FailedCount > 0 {
			resultStatus = "エラー"
			errMsg = fmt.Sprintf("エラー件数: %d", result.FailedCount)
		}
		// ファイル全体の処理結果（サマリ）を生成
		resultModel := model.NewProcessResult(
			firstInsurerNumber,
			result.ReceiptNo,
			csvKey,
			resultStatus,
			errMsg,
		)
		if len(records) > 0 {
			if err := p.recordRepo.TransactPut(ctx, records, nil, nil); err != nil {
				return result, errors.Wrap(err, errors.Database, "CSVRecord一括登録失敗")
			}
		}
		if len(details) > 0 {
			if err := p.detailRepo.TransactPut(ctx, details); err != nil {
				return result, errors.Wrap(err, errors.Database, "ProcessResultDetail一括登録失敗")
			}
		}
		if err := p.resultRepo.Put(ctx, resultModel); err != nil {
			return result, errors.Wrap(err, errors.Database, "ProcessResult登録失敗")
		}
	}
	// 全件処理し終えたらCompletedをtrueに
	result.Completed = true
	return result, nil
}

// 1行分の処理をプライベートメソッドに切り出し
func (p *CSVBatchProcessor) processCSVRow(
	row []string,
	err error,
	rowNumber int,
	result *CSVBatchResult,
	records *[]*model.CSVRecord,
	details *[]*model.ProcessResultDetail,
	firstInsurerNumber *string,
) {
	var descs []*errors.ErrorDescription
	status := "成功"

	// 読み込みエラー時はバリデーションせずエラー明細のみ生成
	if err != nil {
		result.FailedCount++
		descs = []*errors.ErrorDescription{{Message: fmt.Sprintf("CSV読み込み失敗: %v", err)}}
	}
	// バリデーション（descが空の場合のみ）
	if len(descs) == 0 {
		descs = logic.NewCSVValidator(rowNumber, row).Validate()
	}
	// バリデーションエラー時の処理
	if len(descs) > 0 {
		status = "エラー"
		result.FailedCount++
	}
	// バリデーションOK時の処理
	if len(descs) == 0 {
		result.SuccessCount++
		rec, _ := model.NewCSVRecord(row, fmt.Sprintf("%d", rowNumber))
		*records = append(*records, rec)
		if *firstInsurerNumber == "" {
			*firstInsurerNumber = rec.InsurerNumber
		}
	}
	*details = append(*details, model.NewProcessResultDetail(
		result.ReceiptNo,
		fmt.Sprintf("%d", rowNumber),
		status,
		descs,
	))
}

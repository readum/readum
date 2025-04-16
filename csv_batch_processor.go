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
)

// S3バケット名とCSVファイルキーは定数として定義
const (
	csvBucket = "your-bucket-name"
	csvKey    = "your-csv-file.csv"
)

// CSVBatchDTOはバッチ処理の結果をまとめて返すDTO
type CSVBatchDTO struct {
	ReceiptNo string
	Success   int
	Failed    int
	RowNum    int
}

// CSVBatchProcessorはCSV一括処理のユースケースを担う
type CSVBatchProcessor struct {
	storage          proxy.StorageProxy
	csvReaderFactory repository.CSVReaderFactory
	recordRepo       repository.CSVRecordRepository
}

// NewCSVBatchProcessorはCSVBatchProcessorを生成する
func NewCSVBatchProcessor(
	storage proxy.StorageProxy,
	csvReaderFactory repository.CSVReaderFactory,
	recordRepo repository.CSVRecordRepository,
) *CSVBatchProcessor {
	return &CSVBatchProcessor{
		storage:          storage,
		csvReaderFactory: csvReaderFactory,
		recordRepo:       recordRepo,
	}
}

// ProcessCSVBatchはS3からCSVをダウンロードし、バリデーション・DynamoDB登録・結果集計を行う
func (p *CSVBatchProcessor) ProcessCSVBatch(
	ctx context.Context,
	dto CSVBatchDTO,
) (CSVBatchDTO, error) {
	// 一時ファイルパスを生成
	tempPath := filepath.Join(os.TempDir(), csvKey)
	// S3からCSVファイルをダウンロード
	if err := p.storage.DownloadFile(ctx, tempPath, csvBucket, csvKey); err != nil {
		return dto, errors.Wrapf(err, errors.Storage, "CSVダウンロード失敗: %s/%s", csvBucket, csvKey)
	}
	defer os.Remove(tempPath)

	// CSVReaderを生成
	csvReader, err := p.csvReaderFactory(tempPath)
	if err != nil {
		return dto, errors.Wrap(err, errors.ServerProcFail, "CSVReader生成失敗")
	}
	defer csvReader.Close()

	var (
		records            []*model.CSVRecord
		details            []*model.ProcessResultDetail
		firstInsurerNumber string
	)
	const maxTransactItems = 100

	// CSVを1行ずつストリームで処理
	for row, err := range csvReader.Read() {
		// コンテキストキャンセル・タイムアウト時は処理中断
		if ctx.Err() != nil {
			break
		}
		var descs []*errors.ErrorDescription
		status := "成功"

		// 読み込みエラー時はバリデーションせずエラー明細のみ生成
		if err != nil {
			dto.Failed++
			descs = []*errors.ErrorDescription{{Message: fmt.Sprintf("CSV読み込み失敗: %v", err)}}
		}
		// バリデーション（descが空の場合のみ）
		if len(descs) == 0 {
			descs = logic.NewCSVValidator(dto.RowNum, row).Validate()
		}
		// バリデーションエラー時の処理
		if len(descs) > 0 {
			status = "エラー"
			dto.Failed++
		}
		// バリデーションOK時の処理
		if len(descs) == 0 {
			dto.Success++
			rec, _ := model.NewCSVRecord(row) // バリデーション済みなのでエラー無視
			records = append(records, rec)
			// 最初の正常データのInsurerNumberを記録
			if firstInsurerNumber == "" {
				firstInsurerNumber = rec.InsurerNumber
			}
		}
		// 行ごとの処理結果明細を追加
		details = append(details, model.NewProcessResultDetail(
			dto.ReceiptNo,
			fmt.Sprintf("%d", dto.RowNum),
			status,
			descs,
		))
		dto.RowNum++

		// バッチサイズ判定（次のバッチでresultを含めて100件を超えそうなら登録）
		if len(records)+len(details)+1 >= maxTransactItems {
			if err := p.recordRepo.TransactPut(ctx, records, nil, details); err != nil {
				return dto, errors.Wrap(err, errors.Database, "DynamoDB登録失敗")
			}
			records, details = nil, nil
		}
	}

	// 最後のバッチ（ProcessResult付き）を登録
	if len(records) > 0 || len(details) > 0 {
		resultStatus := "成功"
		var errMsg string
		if dto.Failed > 0 {
			resultStatus = "エラー"
			errMsg = fmt.Sprintf("エラー件数: %d", dto.Failed)
		}
		// ファイル全体の処理結果（サマリ）を生成
		result := model.NewProcessResult(
			firstInsurerNumber,
			dto.ReceiptNo,
			csvKey,
			resultStatus,
			errMsg,
		)
		// 最終バッチをDynamoDBに登録
		if err := p.recordRepo.TransactPut(ctx, records, result, details); err != nil {
			return dto, errors.Wrap(err, errors.Database, "DynamoDB登録失敗")
		}
	}
	return dto, nil
}

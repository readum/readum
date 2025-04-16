package dynamodb

import (
	"context"
	stdErr "errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"go-server/domain/model"
	"go-server/pkg/errors"
)

// csvRecordRepository CSVRecord, ProcessResult, ProcessResultDetailをDynamoDBに一括登録するリポジトリ
type csvRecordRepository struct {
	db                 *dynamodb.Client
	csvRecordTable     string
	processResultTable string
	processDetailTable string
}

// NewCSVRecordRepository は、DynamoDBクライアントとテーブル名を内部で設定してcsvRecordRepositoryを生成する
func NewCSVRecordRepository() *csvRecordRepository {
	return &csvRecordRepository{
		db:                 db, // 同パッケージ内のグローバル変数dbを利用
		csvRecordTable:     "CSVRecord",
		processResultTable: "ProcessResult",
		processDetailTable: "ProcessResultDetail",
	}
}

// TransactPut CSVRecord, ProcessResult, ProcessResultDetailをDynamoDBのTransactWriteItemsで一括登録する
// records, result, detailsの合計が100件を超える場合はエラーを返す
func (r *csvRecordRepository) TransactPut(
	ctx context.Context,
	records []*model.CSVRecord,
	result *model.ProcessResult,
	details []*model.ProcessResultDetail,
) error {
	// すべて空（またはnil）の場合はエラー
	if len(records) == 0 && result == nil && len(details) == 0 {
		return errors.Errorf(errors.InvalidParams, "TransactPut: all parameters are empty")
	}
	// 合計件数を計算
	total := len(records)
	if result != nil {
		total++
	}
	total += len(details)
	// DynamoDBのTransactWriteItemsは最大100件まで
	if total > 100 {
		return errors.Errorf(errors.InvalidParams, "TransactWriteItems: total items(%d) exceeds 100", total)
	}
	// 4MB制限についてはAWS公式参照。ここでは件数のみチェック。

	var transactItems []types.TransactWriteItem

	// CSVRecordをTransactWriteItemsに追加
	for _, record := range records {
		recordItem, err := attributevalue.MarshalMap(record)
		if err != nil {
			return errors.Wrap(err, errors.Database, "failed to marshal CSVRecord")
		}
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           &r.csvRecordTable,
				Item:                recordItem,
				ConditionExpression: aws.String("attribute_not_exists(InsurerNumber) AND attribute_not_exists(CareInsuranceNumber)"),
			},
		})
	}

	// ProcessResultをTransactWriteItemsに追加（resultがnilでなければ）
	if result != nil {
		resultItem, err := attributevalue.MarshalMap(result)
		if err != nil {
			return errors.Wrap(err, errors.Database, "failed to marshal ProcessResult")
		}
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           &r.processResultTable,
				Item:                resultItem,
				ConditionExpression: aws.String("attribute_not_exists(InsurerNumber) AND attribute_not_exists(ReceiptNo)"),
			},
		})
	}

	// ProcessResultDetailをTransactWriteItemsに追加
	for _, d := range details {
		detailItem, err := attributevalue.MarshalMap(d)
		if err != nil {
			return errors.Wrapf(err, errors.Database, "failed to marshal ProcessResultDetail: %+v", d)
		}
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           &r.processDetailTable,
				Item:                detailItem,
				ConditionExpression: aws.String("attribute_not_exists(ReceiptNo) AND attribute_not_exists(DetailNo)"),
			},
		})
	}

	// DynamoDBに一括登録を実行
	_, err := r.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		var ccfe *types.TransactionCanceledException
		if stdErr.As(err, &ccfe) {
			// どのテーブルで重複したかは、エラーメッセージや内容から推測するしかない
			return errors.Wrapf(
				err,
				errors.AlreadyExists,
				"DynamoDBトランザクションでPK/SK重複: テーブル=%s, メッセージ=%s",
				"CSVRecord/ProcessResult/ProcessResultDetail", // 実際はどのテーブルか特定できればベスト
				ccfe.ErrorMessage(),
			)
		}
		return errors.Wrap(err, errors.Database, "failed to TransactWriteItems")
	}
	return nil
}

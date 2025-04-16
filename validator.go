package logic

import (
	"go-server/pkg/errors"
	"go-server/pkg/validator"
)

// CSVValidatorはCSV1行分のバリデーションを行う
type CSVValidator struct {
	RowNum int
	Row    []string
}

// NewCSVValidatorはCSVValidatorを生成する
func NewCSVValidator(rowNum int, row []string) *CSVValidator {
	return &CSVValidator{RowNum: rowNum, Row: row}
}

// Validateは1行分のバリデーションを実施し、エラー詳細スライスを返す
func (v *CSVValidator) Validate() []*errors.ErrorDescription {
	return validator.New().
		RequireStr("field1", v.Row[0]).
		MaxLenStr("field1", v.Row[0], 6).
		RequireStr("field2", v.Row[1]).
		MaxLenStr("field2", v.Row[1], 10).
		RequireStr("field3", v.Row[2]).
		MaxLenStr("field3", v.Row[2], 64).
		RequireStr("field4", v.Row[3]).
		MaxLenStr("field4", v.Row[3], 128).
		RequireStr("field5", v.Row[4]).
		ISODate("field5", v.Row[4], false).
		RequireStr("field6", v.Row[5]).
		ISODate("field6", v.Row[5], false).
		RequireStr("field7", v.Row[6]).
		ISODate("field7", v.Row[6], false).
		Descriptions()
}

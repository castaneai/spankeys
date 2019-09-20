package spankeys

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/civil"

	"cloud.google.com/go/spanner"
	proto3 "github.com/golang/protobuf/ptypes/struct"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func DecodeToInterface(gcv *spanner.GenericColumnValue, ptr interface{}) error {
	_, isNull := gcv.Value.Kind.(*proto3.Value_NullValue)

	switch gcv.Type.Code {
	case sppb.TypeCode_BOOL:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullBool{}))
			return nil
		}
		var v bool
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_INT64:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullInt64{}))
			return nil
		}
		var v int64
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_FLOAT64:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullFloat64{}))
			return nil
		}
		var v float64
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_STRING:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullString{}))
			return nil
		}
		var v string
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_DATE:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullDate{}))
			return nil
		}
		var v civil.Date
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_TIMESTAMP:
		if isNull {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(spanner.NullTime{}))
			return nil
		}
		var v time.Time
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_BYTES:
		var v []byte
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	}
	return fmt.Errorf("failed to decode GenericColumnValue(typeCode: %s)", gcv.Type.Code)
}

package spankeys

import (
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/civil"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func DecodeToInterface(gcv *spanner.GenericColumnValue, ptr interface{}) error {
	switch gcv.Type.Code {
	case sppb.TypeCode_BOOL:
		var v bool
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_INT64:
		var v int64
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_FLOAT64:
		var v float64
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_STRING:
		var v string
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_DATE:
		var v civil.Date
		if err := gcv.Decode(&v); err != nil {
			return err
		}
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_TIMESTAMP:
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

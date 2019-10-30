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
	case sppb.TypeCode_ARRAY:
		if isNull {
			return handleNullArray(gcv.Type.ArrayElementType.Code, ptr)
		}
		lv, err := getListValue(gcv.Value)
		if err != nil {
			return err
		}
		cnull := containsNull(lv)

		switch gcv.Type.ArrayElementType.Code {
		case sppb.TypeCode_BOOL:
			if cnull {
				v := make([]spanner.NullBool, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]bool, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_INT64:
			if cnull {
				v := make([]spanner.NullInt64, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]int64, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_FLOAT64:
			if cnull {
				v := make([]spanner.NullFloat64, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]float64, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_STRING:
			if cnull {
				v := make([]spanner.NullString, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]string, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_DATE:
			if cnull {
				v := make([]spanner.NullDate, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]civil.Date, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_TIMESTAMP:
			if cnull {
				v := make([]spanner.NullTime, len(lv.Values))
				if err := gcv.Decode(&v); err != nil {
					return err
				}
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
				return nil
			}
			v := make([]time.Time, len(lv.Values))
			if err := gcv.Decode(&v); err != nil {
				return err
			}
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
			return nil
		case sppb.TypeCode_ARRAY:
			return fmt.Errorf("nested ARRAY type is not supported")
		case sppb.TypeCode_STRUCT:
			return fmt.Errorf("STRUCT type in ARRAY is not supported")
		default:
			return fmt.Errorf("failed to decode GenericColumnValue(typeCode: %s, elementType: %s)", gcv.Type.Code, gcv.Type.ArrayElementType.Code)
		}
	case sppb.TypeCode_STRUCT:
		return fmt.Errorf("STRUCT type is not supported")
	}
	return fmt.Errorf("failed to decode GenericColumnValue(typeCode: %s)", gcv.Type.Code)
}

func handleNullArray(elemCode sppb.TypeCode, ptr interface{}) error {
	switch elemCode {
	case sppb.TypeCode_BOOL:
		var v []bool
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_INT64:
		var v []int64
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_FLOAT64:
		var v []float64
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_TIMESTAMP:
		var v []time.Time
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_DATE:
		var v []civil.Date
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_STRING:
		var v []string
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_BYTES:
		var v [][]byte
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(v))
		return nil
	case sppb.TypeCode_ARRAY:
		return fmt.Errorf("nested ARRAY type is not supported")
	case sppb.TypeCode_STRUCT:
		return fmt.Errorf("STRUCT type in ARRAY is not supported")
	}
	return fmt.Errorf("failed to decode NULL array element (unknown typecode: %d)", elemCode)
}

func getListValue(v *proto3.Value) (*proto3.ListValue, error) {
	if x, ok := v.GetKind().(*proto3.Value_ListValue); ok && x != nil {
		return x.ListValue, nil
	}
	return nil, fmt.Errorf("cannot convert to ListValue")
}

func containsNull(list *proto3.ListValue) bool {
	for _, v := range list.Values {
		_, isNull := v.Kind.(*proto3.Value_NullValue)
		if isNull {
			return true
		}
	}
	return false
}

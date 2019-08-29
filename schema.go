package spankeys

import (
	"context"

	"cloud.google.com/go/spanner"
)

type Column struct {
	Name string
}

type IndexColumn struct {
	*Column
	OrdinalPosition int64
	IsNullable      bool
}

type IndexType string
type IndexState string

const (
	IndexType_Index                    IndexType  = "INDEX"
	IndexType_PrimaryKey               IndexType  = "PRIMARY_KEY"
	IndexState_Prepare                 IndexState = "PREPARE"
	IndexState_Unknown                 IndexState = ""
	IndexState_WriteOnly               IndexState = "WRITE_ONLY"
	IndexState_WriteOnlyCleanup        IndexState = "WRITE_ONLY_CLEANUP"
	IndexState_WriteOnlyValidateUnique IndexState = "WRITE_ONLY_VALIDATE_UNIQUE"
	IndexState_ReadWrite               IndexState = "READ_WRITE"
)

type Index struct {
	Name           string
	Type           IndexType
	Table          string
	ParentTable    string
	IsPrimaryKey   bool
	IsUnique       bool
	IsNullFiltered bool
	State          IndexState
	Columns        []*IndexColumn
}

func GetIndexes(ctx context.Context, client *spanner.Client) ([]*Index, error) {
	stmt := spanner.NewStatement(`
select 
INDEXES.INDEX_NAME,
indexes.INDEX_TYPE,
indexes.PARENT_TABLE_NAME,
indexes.IS_UNIQUE,
indexes.IS_NULL_FILTERED,
indexes.INDEX_STATE,
index_columns.TABLE_NAME,
index_columns.COLUMN_NAME,
index_columns.ORDINAL_POSITION,
index_columns.IS_NULLABLE
from INFORMATION_SCHEMA.INDEXES
inner join INFORMATION_SCHEMA.INDEX_COLUMNS
using (INDEX_NAME)
where INDEX_COLUMNS.TABLE_SCHEMA = '' and INDEXES.TABLE_SCHEMA = ''
order by ORDINAL_POSITION`)

	indexes := make(map[string]*Index)

	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.ColumnByName("INDEX_NAME", &name); err != nil {
			return err
		}
		if _, ok := indexes[name]; !ok {
			var itype string
			var parent string
			var isUnique bool
			var isNullFiltered bool
			var state spanner.NullString
			var table string
			if err := r.ColumnByName("INDEX_TYPE", &itype); err != nil {
				return err
			}
			if err := r.ColumnByName("PARENT_TABLE_NAME", &parent); err != nil {
				return err
			}
			if err := r.ColumnByName("IS_UNIQUE", &isUnique); err != nil {
				return err
			}
			if err := r.ColumnByName("IS_NULL_FILTERED", &isNullFiltered); err != nil {
				return err
			}
			if err := r.ColumnByName("INDEX_STATE", &state); err != nil {
				return err
			}
			if err := r.ColumnByName("TABLE_NAME", &table); err != nil {
				return err
			}
			stt := IndexState_Unknown
			if state.Valid {
				stt = IndexState(state.StringVal)
			}
			indexes[name] = &Index{
				Name:           name,
				Type:           IndexType(itype),
				Table:          table,
				ParentTable:    parent,
				IsPrimaryKey:   name == "PRIMARY_KEY",
				IsUnique:       isUnique,
				IsNullFiltered: isNullFiltered,
				State:          stt,
			}
		}

		var colName string
		var ordinalPos int64
		var isNullableStr string
		if err := r.ColumnByName("COLUMN_NAME", &colName); err != nil {
			return err
		}
		if err := r.ColumnByName("ORDINAL_POSITION", &ordinalPos); err != nil {
			return err
		}
		if err := r.ColumnByName("IS_NULLABLE", &isNullableStr); err != nil {
			return err
		}
		indexes[name].Columns = append(indexes[name].Columns, &IndexColumn{
			Column:          &Column{Name: colName},
			OrdinalPosition: ordinalPos,
			IsNullable:      isNullableStr == "YES",
		})
		return nil
	}); err != nil {
		return nil, err
	}

	var idxes []*Index
	for _, idx := range indexes {
		idxes = append(idxes, idx)
	}
	return idxes, nil
}

func GetPrimaryKeyColumns(ctx context.Context, client *spanner.Client, table string) ([]*Column, error) {
	stmt := spanner.NewStatement("select column_name from INFORMATION_SCHEMA.INDEX_COLUMNS where table_name = @tableName and index_type = 'PRIMARY_KEY' order by ordinal_position")
	stmt.Params["tableName"] = table
	var pks []*Column
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.Column(0, &name); err != nil {
			return err
		}
		pks = append(pks, &Column{Name: name})
		return nil
	}); err != nil {
		return nil, err
	}
	return pks, nil
}

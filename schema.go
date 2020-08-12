package spankeys

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

type Table struct {
	Name       string
	Interleave *Interleave
}

type Column struct {
	Name            string

	// ORDINAL_POSITION is nullable
	// https://cloud.google.com/spanner/docs/information-schema#information_schemaindex_columns
	OrdinalPosition spanner.NullInt64
}

type IndexColumn struct {
	Column
}

type Interleave struct {
	Table    string
	OnDelete OnDelete
}

type OnDelete int

const (
	OnDeleteNoAction OnDelete = iota
	OnDeleteCascade
)

type IndexType string
type IndexState string

const (
	IndexTypeIndex                    IndexType  = "INDEX"
	IndexTypePrimaryKey               IndexType  = "PRIMARY_KEY"
	IndexStatePrepare                 IndexState = "PREPARE"
	IndexStateUnknown                 IndexState = ""
	IndexStateWriteOnly               IndexState = "WRITE_ONLY"
	IndexStateWriteOnlyCleanup        IndexState = "WRITE_ONLY_CLEANUP"
	IndexStateWriteOnlyValidateUnique IndexState = "WRITE_ONLY_VALIDATE_UNIQUE"
	IndexStateReadWrite               IndexState = "READ_WRITE"
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

func GetTables(ctx context.Context, client *spanner.Client) ([]*Table, error) {
	stmt := spanner.NewStatement(`
select * from INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = ''
`)
	var ts []*Table
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.ColumnByName("TABLE_NAME", &name); err != nil {
			return err
		}
		t := &Table{
			Name:       name,
			Interleave: nil,
		}

		var parentTable spanner.NullString
		if err := r.ColumnByName("PARENT_TABLE_NAME", &parentTable); err != nil {
			return err
		}
		var onDelete spanner.NullString
		if err := r.ColumnByName("ON_DELETE_ACTION", &onDelete); err != nil {
			return err
		}
		if parentTable.Valid {
			onDeleteAction := OnDeleteNoAction
			if onDelete.StringVal == "CASCADE" {
				onDeleteAction = OnDeleteCascade
			}
			t.Interleave = &Interleave{Table: parentTable.StringVal, OnDelete: onDeleteAction}
		}
		ts = append(ts, t)
		return nil
	}); err != nil {
		return nil, err
	}
	return ts, nil
}

func GetInterleaveChildren(ctx context.Context, client *spanner.Client, parentTable string) ([]*Interleave, error) {
	stmt := spanner.NewStatement(fmt.Sprintf(`
select * from INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = '' and PARENT_TABLE_NAME = '%s';
`, parentTable))

	var is []*Interleave
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var table string
		if err := r.ColumnByName("TABLE_NAME", &table); err != nil {
			return err
		}
		var onDelete string
		if err := r.ColumnByName("ON_DELETE_ACTION", &onDelete); err != nil {
			return err
		}
		onDeleteAction := OnDeleteNoAction
		if onDelete == "CASCADE" {
			onDeleteAction = OnDeleteCascade
		}
		is = append(is, &Interleave{
			Table:    table,
			OnDelete: onDeleteAction,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return is, nil
}

func GetSecondaryIndexes(ctx context.Context, client *spanner.Client, table string) ([]*Index, error) {
	idxes, err := GetTableIndexes(ctx, client, table)
	if err != nil {
		return nil, err
	}
	var sis []*Index
	for _, idx := range idxes {
		if !idx.IsPrimaryKey {
			sis = append(sis, idx)
		}
	}
	return sis, nil
}

func GetTableIndexes(ctx context.Context, client *spanner.Client, table string) ([]*Index, error) {
	idxes, err := GetIndexes(ctx, client)
	if err != nil {
		return nil, err
	}
	var tis []*Index
	for _, idx := range idxes {
		if idx.Table == table {
			tis = append(tis, idx)
		}
	}
	return tis, nil
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
from INFORMATION_SCHEMA.INDEX_COLUMNS
left join INFORMATION_SCHEMA.INDEXES
using (INDEX_NAME)
where INDEX_COLUMNS.TABLE_SCHEMA = '' and INDEXES.TABLE_SCHEMA = ''
order by ORDINAL_POSITION`)

	indexes := make(map[string]*Index)
	colKeys := make(map[string]struct{})

	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var table string
		if err := r.ColumnByName("TABLE_NAME", &table); err != nil {
			return err
		}
		var name string
		if err := r.ColumnByName("INDEX_NAME", &name); err != nil {
			return err
		}
		key := fmt.Sprintf("%s_%s", table, name)

		if _, ok := indexes[key]; !ok {
			var itype string
			if err := r.ColumnByName("INDEX_TYPE", &itype); err != nil {
				return err
			}
			var parent string
			if err := r.ColumnByName("PARENT_TABLE_NAME", &parent); err != nil {
				return err
			}
			var isUnique bool
			if err := r.ColumnByName("IS_UNIQUE", &isUnique); err != nil {
				return err
			}
			var isNullFiltered bool
			if err := r.ColumnByName("IS_NULL_FILTERED", &isNullFiltered); err != nil {
				return err
			}
			var state spanner.NullString
			if err := r.ColumnByName("INDEX_STATE", &state); err != nil {
				return err
			}
			stt := IndexStateUnknown
			if state.Valid {
				stt = IndexState(state.StringVal)
			}
			indexes[key] = &Index{
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
		if err := r.ColumnByName("COLUMN_NAME", &colName); err != nil {
			return err
		}
		var op spanner.NullInt64
		if err := r.ColumnByName("ORDINAL_POSITION", &op); err != nil {
			return err
		}
		colKey := fmt.Sprintf("%s_%s", key, colName)
		if _, exists := colKeys[colKey]; !exists {
			indexes[key].Columns = append(indexes[key].Columns, &IndexColumn{
				Column{Name: colName, OrdinalPosition: op},
			})
			colKeys[colKey] = struct{}{}
		}
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

func GetColumns(ctx context.Context, client *spanner.Client, table string) ([]*Column, error) {
	stmt := spanner.NewStatement("select column_name, ordinal_position from INFORMATION_SCHEMA.COLUMNS where table_name = @tableName order by ordinal_position")
	stmt.Params["tableName"] = table
	var cols []*Column
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.Column(0, &name); err != nil {
			return err
		}
		var op spanner.NullInt64
		if err := r.Column(1, &op); err != nil {
			return err
		}
		cols = append(cols, &Column{Name: name, OrdinalPosition: op})
		return nil
	}); err != nil {
		return nil, err
	}
	return cols, nil
}

func GetPrimaryKeyColumns(ctx context.Context, client *spanner.Client, table string) ([]*Column, error) {
	stmt := spanner.NewStatement("select column_name, ordinal_position from INFORMATION_SCHEMA.INDEX_COLUMNS where table_name = @tableName and index_type = 'PRIMARY_KEY' order by ordinal_position")
	stmt.Params["tableName"] = table
	var pks []*Column
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.Column(0, &name); err != nil {
			return err
		}
		var op spanner.NullInt64
		if err := r.Column(1, &op); err != nil {
			return err
		}
		pks = append(pks, &Column{Name: name, OrdinalPosition: op})
		return nil
	}); err != nil {
		return nil, err
	}
	return pks, nil
}

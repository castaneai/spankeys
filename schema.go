package spankeys

import (
	"context"

	"cloud.google.com/go/spanner"
)

type Column struct {
	Name string
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

package spankeys

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
)

type CountableKeyRange struct {
	*spanner.KeyRange
	RowCount int64
}

func PartitionsKeyRanges(ctx context.Context, client *spanner.Client, tableName string, pkColumns []*Column, mutationBatchSize, selectLimit int) ([]*CountableKeyRange, error) {
	var pkns []string
	for _, col := range pkColumns {
		pkns = append(pkns, fmt.Sprintf("`%s`", col.Name))
	}
	if len(pkns) < 1 {
		return nil, errors.New("at least one of Primary Key is required")
	}
	sql := fmt.Sprintf("SELECT %s FROM `%s` ORDER BY %s ASC LIMIT %d", strings.Join(pkns, ","), tableName, pkns[0], selectLimit)
	stmt := spanner.NewStatement(sql)

	var keySets []*CountableKeyRange
	var startKey spanner.Key
	var currentKey spanner.Key
	cnt := 0
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var key spanner.Key
		for _, col := range pkColumns {
			var gcv spanner.GenericColumnValue
			if err := r.ColumnByName(col.Name, &gcv); err != nil {
				return err
			}
			var k interface{}
			if err := DecodeToInterface(&gcv, &k); err != nil {
				return err
			}
			key = append(key, k)
		}
		currentKey = key
		if cnt == 0 {
			startKey = key
		}
		cnt++
		if cnt >= mutationBatchSize {
			endKey := key
			keySets = append(keySets, &CountableKeyRange{
				KeyRange: &spanner.KeyRange{Start: startKey, End: endKey, Kind: spanner.ClosedClosed},
				RowCount: int64(cnt),
			})
			cnt = 0
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if cnt > 0 && currentKey != nil {
		keySets = append(keySets, &CountableKeyRange{
			KeyRange: &spanner.KeyRange{Start: startKey, End: currentKey, Kind: spanner.ClosedClosed},
			RowCount: int64(cnt),
		})
		cnt = 0
	}
	return keySets, nil
}

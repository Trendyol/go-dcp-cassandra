package cassandra

type OperationType string

const (
	Insert OperationType = "insert"
	Update OperationType = "update"
	Delete OperationType = "delete"
	Upsert OperationType = "upsert"
)

type Model interface {
	Convert() *ExecArgs
}

type Raw struct {
	Table     string
	Operation OperationType
	ID        string
	Document  map[string]any
	Filter    map[string]any
	RowKey    map[string]any
	Timestamp int64
}

type ExecArgs struct {
	Table     string
	Operation OperationType
	Document  map[string]any
	Filter    map[string]any
}

func (r *Raw) Convert() *ExecArgs {
	return &ExecArgs{
		Table:     r.Table,
		Document:  r.Document,
		Operation: r.Operation,
		Filter:    r.Filter,
	}
}

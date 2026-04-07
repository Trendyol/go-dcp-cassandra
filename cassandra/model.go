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
	Document  map[string]any
	Operation OperationType
	Filter    map[string]any
	RowKey    map[string]any
	ID        string
	Timestamp int64
}

type ExecArgs struct {
	Document  map[string]any
	Filter    map[string]any
	Table     string
	Operation OperationType
}

func (r *Raw) Convert() *ExecArgs {
	return &ExecArgs{
		Table:     r.Table,
		Document:  r.Document,
		Operation: r.Operation,
		Filter:    r.Filter,
	}
}

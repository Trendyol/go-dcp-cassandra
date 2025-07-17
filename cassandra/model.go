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
	Document  map[string]interface{}
	Operation OperationType
	Filter    map[string]interface{}
	ID        string
}

type ExecArgs struct {
	Table     string
	Document  map[string]interface{}
	Operation OperationType
	Filter    map[string]interface{}
}

func (r *Raw) Convert() *ExecArgs {
	return &ExecArgs{
		Table:     r.Table,
		Document:  r.Document,
		Operation: r.Operation,
		Filter:    r.Filter,
	}
}

type Fields struct{}

const (
	Id = "id"
)

func (Fields) GetFieldMappings() map[string]string {
	return map[string]string{
		"documentid": Id,
	}
}

func (Fields) GetPrimaryKeys() []string {
	return []string{Id}
}

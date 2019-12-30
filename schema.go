package schema

import (
	"github.com/gohouse/gorose/v2"
	"github.com/gohouse/t"
	"strings"
	"sync"
)

type Schema struct {
	engin     *gorose.Engin
	all       map[string][]TableColumn
	tableList map[string]string
}

var once sync.Once
var database *Schema

func NewSchema(ge *gorose.Engin) *Schema {
	once.Do(func() {
		database = &Schema{engin: ge, all: make(map[string][]TableColumn), tableList: make(map[string]string,0)}
		// 获取所有表信息
		database.initTableList()
		// 获取所有信息
		all := database.getAll()
		for _, col := range all {
			// 将表的信息推入到 database.all 中
			tableName := strings.TrimPrefix(col.TableName, database.engin.GetPrefix())
			if _, ok := database.all[tableName]; !ok {
				database.all[tableName] = []TableColumn{}
			}
			database.all[tableName] = append(database.all[tableName], col)
		}
	})
	return database
}

func (s *Schema) initTableList() {
	var ts []gorose.Data
	_, err := s.engin.NewSession().Bind(&ts).Query("show table status")
	if err != nil {
		panic(err.Error())
	}
	for _, item := range ts {
		s.tableList[strings.TrimPrefix(item["Name"].(string), s.engin.GetPrefix())] = t.New(item["Comment"]).String()
	}
}

type TableColumn struct {
	ColumnName    string  `gorose:"COLUMN_NAME"`
	Type          string  `gorose:"DATA_TYPE"`
	Nullable      string  `gorose:"IS_NULLABLE"`
	TableName     string  `gorose:"TABLE_NAME"`
	ColumnComment string  `gorose:"COLUMN_COMMENT"`
	ColumnKey     string  `gorose:"COLUMN_KEY"`
	ColumnDefault *string `gorose:"COLUMN_DEFAULT"`
}

func (t *Schema) TableColumnList(tabname string) []TableColumn {
	return t.all[tabname]
}
func (t *Schema) TablePkidName(tabname string) string {
	tabInfo := t.TableColumnList(tabname)
	for _, item := range tabInfo {
		if item.ColumnKey == "PRI" {
			return item.ColumnName
		}
	}
	return ""
}
func (t *Schema) TableColumnInfo(tabname, columnName string) (tc TableColumn) {
	tabInfo := t.TableColumnList(tabname)
	for _, item := range tabInfo {
		if item.ColumnName == columnName {
			return item
		}
	}
	return
}
func (t *Schema) TableFields(tabname string) []string {
	var fields []string
	var columns = t.TableColumnList(tabname)
	for _, column := range columns {
		fields = append(fields, column.ColumnName)
	}
	return fields
}
func (t *Schema) TableList() map[string]string {
	return t.tableList
}
func (t *Schema) All() map[string][]TableColumn {
	return t.all
}

func (t *Schema) getAll() []TableColumn {
	var c []TableColumn
	var querystr = `SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,TABLE_NAME,COLUMN_COMMENT,COLUMN_KEY,COLUMN_DEFAULT
		FROM information_schema.COLUMNS 
		WHERE table_schema = DATABASE()`
	_, err := t.engin.NewSession().Bind(&c).Query(querystr)
	if err != nil {
		panic(err.Error())
	}
	return c
}

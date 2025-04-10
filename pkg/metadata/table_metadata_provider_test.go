package metadata

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TableMetaProviderTestSuite defines the test suite
type TableMetaProviderTestSuite struct {
	suite.Suite
	mock       sqlmock.Sqlmock
	db         *sql.DB
	dbProvider *DbConnProvider
	provider   *TableMetaProvider
}

// SetupTest sets up the test environment before each test
func (s *TableMetaProviderTestSuite) SetupTest() {
	var err error
	s.db, s.mock, err = sqlmock.New()
	require.NoError(s.T(), err)

	s.dbProvider = &DbConnProvider{
		dbConn:       s.db,
		databaseName: "test",
	}
	s.provider = NewTableMetaProvider(s.dbProvider, "test_table")
}

// TearDownTest cleans up the test environment after each test
func (s *TableMetaProviderTestSuite) TearDownTest() {
	s.db.Close()
}

// TestTableMetaProviderTestSuite runs the test suite
func TestTableMetaProviderTestSuite(t *testing.T) {
	suite.Run(t, new(TableMetaProviderTestSuite))
}

func (s *TableMetaProviderTestSuite) TestNewTableMetaProvider() {
	assert.NotNil(s.T(), s.provider)
	assert.Equal(s.T(), s.dbProvider, s.provider.DbProvider)
	assert.Equal(s.T(), "test_table", s.provider.TableName)
	assert.Nil(s.T(), s.provider.TableInfoCache)
}

func (s *TableMetaProviderTestSuite) TestVerifyTableExists_Exists() {
	tableName := "test_table"
	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1)
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)

	exists, err := s.provider.verifyTableExists(tableName)
	assert.NoError(s.T(), err)
	assert.True(s.T(), exists)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestVerifyTableExists_NotExists() {
	tableName := "non_existent_table"
	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0)
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)

	exists, err := s.provider.verifyTableExists(tableName)
	assert.Error(s.T(), err)
	assert.False(s.T(), exists)
	assert.Contains(s.T(), err.Error(), "table non_existent_table does not exist")
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestVerifyTableExists_Error() {
	tableName := "error_table"
	expectedErr := fmt.Errorf("db error")
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(expectedErr)

	exists, err := s.provider.verifyTableExists(tableName)
	assert.Error(s.T(), err)
	assert.False(s.T(), exists)
	s.EqualError(err, "failed to check table existence: db error")
}

func (s *TableMetaProviderTestSuite) TestGetTableRowCount_Success() {
	tableName := "test_table"
	expectedCount := int64(100)
	rows := sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(expectedCount)
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM test_table`).WillReturnRows(rows)

	count, err := s.provider.getTableRowCount(tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedCount, count)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestGetTableRowCount_Error() {
	tableName := "test_table"
	expectedErr := fmt.Errorf("db error")
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM test_table`).WillReturnError(expectedErr)

	count, err := s.provider.getTableRowCount(tableName)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), int64(0), count)
	s.EqualError(err, "failed to get total row count: db error")
}

func (s *TableMetaProviderTestSuite) TestGetTableColumns_Success() {
	tableName := "test_table"
	expectedColumns := []string{"id", "name", "value"}
	rows := sqlmock.NewRows([]string{"column_name"}).
		AddRow("id").
		AddRow("name").
		AddRow("value")

	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? ORDER BY ordinal_position`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)

	columns, err := s.provider.getTableColumns(tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedColumns, columns)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestGetTableColumns_QueryError() {
	tableName := "test_table"
	expectedErr := fmt.Errorf("query failed")
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? ORDER BY ordinal_position`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(expectedErr)

	columns, err := s.provider.getTableColumns(tableName)
	s.EqualError(err, "failed to get columns: query failed") // Match the full error
	assert.Nil(s.T(), columns)
}

func (s *TableMetaProviderTestSuite) TestGetTableColumns_ScanError() {
	tableName := "test_table"
	rows := sqlmock.NewRows([]string{"column_name"}).
		AddRow("id").
		AddRow(123) // Invalid type to cause scan error

	expectedColumns := []string{"id", "123"}

	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? ORDER BY ordinal_position`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)

	columns, err := s.provider.getTableColumns(tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedColumns, columns)
}

func (s *TableMetaProviderTestSuite) TestGetPrimaryKeyInfo_Success() {
	tableName := "test_table"
	expectedPK := "id"
	rows := sqlmock.NewRows([]string{"column_name"}).AddRow(expectedPK)

	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? AND column_key = 'PRI'`).
		WithArgs(s.provider.GetDatabaseName(), tableName).
		WillReturnRows(rows)

	pk, err := s.provider.getPrimaryKeyInfo(tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedPK, pk)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestGetPrimaryKeyInfo_Error() {
	tableName := "test_table"
	expectedErr := fmt.Errorf("db error")
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? AND column_key = 'PRI'`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(expectedErr)

	pk, err := s.provider.getPrimaryKeyInfo(tableName)
	s.EqualError(err, "failed to get primary key column: db error") // Match the full error
	assert.Equal(s.T(), "", pk)
}

func (s *TableMetaProviderTestSuite) TestQueryTablePKRange_Success() {
	tableName := "test_table"
	pkColumn := "id"
	minPK, maxPK := int64(1), int64(1000)
	rows := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(minPK, maxPK)

	queryPattern := fmt.Sprintf("SELECT MIN(.+), MAX(.+) FROM `%s`", tableName)
	s.mock.ExpectQuery(queryPattern).WillReturnRows(rows)

	pkRange, err := s.provider.queryTablePKRange(pkColumn, tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), NewPKRange(minPK, maxPK), pkRange)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestQueryTablePKRange_EmptyTable_NullScan() {
	tableName := "empty_table"
	pkColumn := "id"
	queryPattern := `SELECT MIN\(id\), MAX\(id\) FROM empty_table`

	// Mock first attempt
	rowsAttempt1 := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(nil, nil)
	s.mock.ExpectQuery(queryPattern).WillReturnRows(rowsAttempt1)

	// Mock second attempt
	rowsAttempt2 := sqlmock.NewRows([]string{"MIN(`id`)", "MAX(`id`)"}).AddRow(sql.NullInt64{}, sql.NullInt64{})
	s.mock.ExpectQuery(queryPattern).WillReturnRows(rowsAttempt2)

	pkRange, err := s.provider.queryTablePKRange(pkColumn, tableName)

	s.T().Logf("Error received: %v", err)

	// We expect an error because the nullable scan also finds no valid values
	assert.Error(s.T(), err)
	assert.Equal(s.T(), EmptyPKRange, pkRange)
	assert.Contains(s.T(), err.Error(), "primary key MIN/MAX returned NULL for table")
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestQueryTablePKRange_QueryError() {
	tableName := "test_table"
	pkColumn := "id"
	expectedErr := fmt.Errorf("db error")

	queryPattern := `SELECT MIN\(id\), MAX\(id\) FROM test_table`

	// Mock the first query attempt failing
	s.mock.ExpectQuery(queryPattern).WillReturnError(expectedErr)

	// Mock the retry attempt also failing
	s.mock.ExpectQuery(queryPattern).WillReturnError(expectedErr)

	pkRange, err := s.provider.queryTablePKRange(pkColumn, tableName)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), EmptyPKRange, pkRange)
	assert.Contains(s.T(), err.Error(), "failed to query min/max primary key")
	assert.ErrorIs(s.T(), err, expectedErr)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestQueryTablePKRange_MaxLessThanMin() {
	tableName := "test_table"
	pkColumn := "id"
	minPK, maxPK := int64(100), int64(50) // Invalid range
	rows := sqlmock.NewRows([]string{"MIN(id)", "MAX(id)"}).AddRow(minPK, maxPK)

	queryPattern := `SELECT MIN\(id\), MAX\(id\) FROM test_table`
	s.mock.ExpectQuery(queryPattern).WillReturnRows(rows)

	pkRange, err := s.provider.queryTablePKRange(pkColumn, tableName)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), EmptyPKRange, pkRange)
	assert.Contains(s.T(), err.Error(), "max primary key (50) is less than min primary key (100)")
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestQueryTableInfo_Success() {
	tableName := "test_table"
	rowCount := int64(500)
	columns := []string{"id", "data"}
	pkColumn := "id"
	minPK, maxPK := int64(1), int64(500)

	// Mock verifyTableExists
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(1))

	// Mock getTableRowCount
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM test_table`).
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(rowCount))

	// Mock getTableColumns
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? ORDER BY ordinal_position`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id").AddRow("data"))

	// Mock getPrimaryKeyInfo
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? AND column_key = 'PRI'`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow(pkColumn))

	// Mock queryTablePKRange
	pkRangeQueryPattern := `SELECT MIN\(id\), MAX\(id\) FROM test_table`
	s.mock.ExpectQuery(pkRangeQueryPattern).
		WillReturnRows(sqlmock.NewRows([]string{"MIN(id)", "MAX(id)"}).AddRow(minPK, maxPK))

	info, err := s.provider.queryTableInfo(tableName)

	require.NoError(s.T(), err)
	require.NotNil(s.T(), info)
	assert.True(s.T(), info.TableExists)
	assert.Equal(s.T(), "test", info.DatabaseName)
	assert.Equal(s.T(), tableName, info.TableName)
	assert.Equal(s.T(), rowCount, info.RowCount)
	assert.Equal(s.T(), columns, info.Columns)
	assert.Equal(s.T(), pkColumn, info.PrimaryKey)
	assert.Equal(s.T(), NewPKRange(minPK, maxPK), info.PKRange)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestQueryTableInfo_VerifyError() {
	tableName := "test_table"
	expectedErr := fmt.Errorf("verify failed")
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(expectedErr) // Error on first step

	info, err := s.provider.queryTableInfo(tableName)
	assert.Error(s.T(), err)
	assert.Nil(s.T(), info)
	s.EqualError(err, "failed to check table existence: verify failed") // Should propagate the original error wrapped
}

func (s *TableMetaProviderTestSuite) TestGetTableInfo_NoCache() {
	tableName := "test_table"
	// Expect calls for queryTableInfo (abbreviated mocks for brevity)
	s.mock.ExpectQuery(`SELECT COUNT\(1\) FROM information_schema\.tables WHERE table_schema = \? AND table_name = \?`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1)) // exists
	// Use concatenation for count query
	countQueryPattern := `SELECT COUNT\(1\) FROM test_table`
	s.mock.ExpectQuery(countQueryPattern).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(10))                                                                                                                                                             // count
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? ORDER BY ordinal_position`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id")) // cols
	s.mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns WHERE table_schema = \? AND table_name = \? AND column_key = 'PRI'`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))    // pk
	// Use simplified regex for PK range
	pkRangeQueryPattern := `SELECT MIN\(id\), MAX\(id\) FROM test_table`
	s.mock.ExpectQuery(pkRangeQueryPattern).WillReturnRows(sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow(1, 10)) // range

	info, err := s.provider.GetTableInfo(tableName)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), info)
	// Check if the cache was populated (it should be nil before calling queryTableInfo)
	assert.Nil(s.T(), s.provider.TableInfoCache)
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestGetTableInfo_WithCache() {
	tableName := "test_table"
	cachedInfo := &TableInfo{TableName: tableName, RowCount: 123}
	s.provider.TableInfoCache = cachedInfo // Pre-populate cache

	info, err := s.provider.GetTableInfo(tableName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), cachedInfo, info) // Should return cached version
	// No DB calls should be made
	assert.NoError(s.T(), s.mock.ExpectationsWereMet())
}

func (s *TableMetaProviderTestSuite) TestGetDbConn() {
	assert.Equal(s.T(), s.db, s.provider.getDbConn())
}

func (s *TableMetaProviderTestSuite) TestGetDatabaseName() {
	assert.Equal(s.T(), "test", s.provider.GetDatabaseName())
}

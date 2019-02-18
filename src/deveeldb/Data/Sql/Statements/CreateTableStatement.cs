using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements.Security;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateTableStatement : SqlStatement {
		public CreateTableStatement(ObjectName tableName, IEnumerable<SqlTableColumn> columns, bool temporary = false, bool ifNotExists = true) {
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			Columns = columns ?? throw new ArgumentNullException(nameof(columns));
			Temporary = temporary;
			IfNotExists = ifNotExists;
		}

		public ObjectName TableName { get; }

		public IEnumerable<SqlTableColumn> Columns { get; }

        public bool Temporary { get; }

        public  bool IfNotExists { get; }

        private async Task<TableInfo> CreateTableInfo(IContext context) {
	        var tableName = await context.ResolveTableNameAsync(TableName);

	        var idColumnCount = Columns.Count(x => x.IsIdentity);
	        if (idColumnCount > 1)
		        throw new InvalidOperationException("More than one IDENTITY column specified.");

	        bool ignoreCase = context.IgnoreCase();
	        var columnChecker = new TableColumnChecker(Columns, ignoreCase);


	        var tableInfo = new TableInfo(tableName);

	        foreach (var column in Columns) {
		        var columnInfo = CreateColumnInfo(context, tableName.Name, column, columnChecker);

		        if (column.IsIdentity)
			        columnInfo.DefaultValue = SqlExpression.Function("UNIQUEKEY", SqlExpression.Constant(tableName.ToString()));

		        tableInfo.Columns.Add(columnInfo);
	        }

	        return tableInfo;
        }

        private ColumnInfo CreateColumnInfo(IContext context, string tableName, SqlTableColumn column, TableColumnChecker columnChecker) {
	        var expression = column.DefaultExpression;

	        if (column.IsIdentity && expression != null)
		        throw new InvalidOperationException($"Identity column '{column.ColumnName}' cannot define a DEFAULT expression.");

	        if (expression != null)
		        expression = columnChecker.CheckExpression(expression);


	        var columnName = columnChecker.StripTableName(tableName, column.ColumnName);
			// TODO: support for dynamic types such as #ROW ?
	        var columnType = column.ColumnType;

	        return new ColumnInfo(columnName, columnType, expression);
        }

        protected override async Task<SqlStatement> PrepareStatementAsync(IContext context) {
	        var tableInfo = await CreateTableInfo(context);

	        return new Prepared(tableInfo, IfNotExists, Temporary);
        }

        protected override Task ExecuteStatementAsync(StatementContext context) {
	        throw new NotImplementedException();
        }

        #region Prepared

        class Prepared : SqlStatement {
	        public Prepared(TableInfo tableInfo, bool temporary, bool ifNotExists) {
		        TableInfo = tableInfo;
		        Temporary = temporary;
		        IfNotExists = ifNotExists;
	        }

	        public TableInfo TableInfo { get; }

            public bool Temporary { get; }
            public bool IfNotExists { get; }

            protected override void Require(IRequirementCollection requirements) {
		        requirements.Require(x => x.UserCanCreateInSchema(TableInfo.SchemaName.FullName));
	        }

	        protected override Task ExecuteStatementAsync(StatementContext context) {
		        throw new NotImplementedException();
	        }
        }

        #endregion

        #region TableColumnChecker

        class TableColumnChecker : ColumnChecker {
	        private readonly IEnumerable<SqlTableColumn> columns;
	        private readonly bool ignoreCase;

	        public TableColumnChecker(IEnumerable<SqlTableColumn> columns, bool ignoreCase) {
		        this.columns = columns;
		        this.ignoreCase = ignoreCase;
	        }

	        public override string ResolveColumnName(string columnName) {
		        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
		        string foundColumn = null;

		        foreach (var columnInfo in columns) {
			        if (foundColumn != null)
				        throw new InvalidOperationException(String.Format("Column name '{0}' caused an ambiguous match in table.", columnName));

			        if (String.Equals(columnInfo.ColumnName, columnName, comparison))
				        foundColumn = columnInfo.ColumnName;
		        }

		        return foundColumn;
	        }
        }

        #endregion

	}
}
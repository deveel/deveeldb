using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropTableStatement : SqlStatement {
		public DropTableStatement(string[] tableNames) 
			: this(tableNames, false) {
		}

		public DropTableStatement(string[] tableNames, bool ifExists) {
			if (tableNames == null)
				throw new ArgumentNullException("tableNames");
			if (tableNames.Length == 0)
				throw new ArgumentException("The table name list cannot be empty", "tableNames");

			if (tableNames.Any(String.IsNullOrEmpty))
				throw new ArgumentException("One of the specified table names is null.");

			TableNames = tableNames;
			IfExists = ifExists;
		}

		public DropTableStatement(string tableName)
			: this(tableName, false) {
		}

		public DropTableStatement(string tableName, bool ifExists)
			: this(new[] {tableName}, ifExists) {
		}

		public string[] TableNames { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var tableNameList = TableNames.ToList();
			var dropTables = new List<string>();

			foreach (var tableName in tableNameList) {
				if (dropTables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
					throw new StatementPrepareException(String.Format("Duplicated table name '{0}' in the list of tables to drop.",
						tableName));

				dropTables.Add(tableName);
			}

			var resolvedTableNames = dropTables.Select(context.ResolveTableName);

			return new Prepared(this, resolvedTableNames, IfExists);
		}

		#region Prepared

		class Prepared : SqlPreparedStatement {
			public Prepared(DropTableStatement source, IEnumerable<ObjectName> tableNames, bool ifExists) 
				: base(source) {
				TableNames = tableNames;
				IfExists = ifExists;
			}

			public IEnumerable<ObjectName> TableNames { get; private set; }

			public bool IfExists { get; private set; }

			protected override ITable ExecuteStatement(IQueryContext context) {
				context.DropTables(TableNames, IfExists);
				return FunctionTable.ResultTable(context, 0);
			}
		}

		#endregion
	}
}

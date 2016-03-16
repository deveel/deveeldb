// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// The statement object used to create a table in a database.
	/// </summary>
	public sealed class CreateTableStatement : SqlStatement {
		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="columns"></param>
		public CreateTableStatement(ObjectName tableName, IEnumerable<SqlTableColumn> columns) {
			TableName = tableName;
			Columns = new List<SqlTableColumn>();
			if (columns != null) {
				foreach (var column in columns) {
					Columns.Add(column);
				}
			}
		}

		public ObjectName TableName { get; private set; }

		public IList<SqlTableColumn> Columns { get; private set; }

		public bool IfNotExists { get; set; }

		public bool Temporary { get; set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableInfo = CreateTableInfo(context);

			return new Prepared(tableInfo, IfNotExists, Temporary);
		}

		private TableInfo CreateTableInfo(IRequest context) {
			var tableName = context.Access.ResolveTableName(TableName);

			var idColumnCount = Columns.Count(x => x.IsIdentity);
			if (idColumnCount > 1)
				throw new InvalidOperationException("More than one IDENTITY column specified.");

			bool ignoreCase = context.Query.IgnoreIdentifiersCase();
			var columnChecker = new TableColumnChecker(Columns, ignoreCase);


			var tableInfo = new TableInfo(tableName);

			foreach (var column in Columns) {
				var columnInfo = CreateColumnInfo(tableName.Name, column, columnChecker);
				tableInfo.AddColumn(columnInfo);
			}

			return tableInfo;
		}

		private ColumnInfo CreateColumnInfo(string tableName, SqlTableColumn column, TableColumnChecker columnChecker) {
			var expression = column.DefaultExpression;

			if (column.IsIdentity && expression != null)
				throw new InvalidOperationException(String.Format("Identity column '{0}' cannot define a DEFAULT expression.", column.ColumnName));

			if (expression != null)
				expression = columnChecker.CheckExpression(expression);


			var columnName = columnChecker.StripTableName(tableName, column.ColumnName);

			return new ColumnInfo(columnName, column.ColumnType) {
				DefaultExpression = expression,
				IsNotNull = column.IsNotNull
			};
		}

		#region Prepared

		[Serializable]
		internal class Prepared : SqlStatement {
			private Prepared(SerializationInfo info, StreamingContext context) {
				TableInfo = (TableInfo) info.GetValue("TableInfo", typeof(TableInfo));
				Temporary = info.GetBoolean("Temporary");
				IfNotExists = info.GetBoolean("IfNotExists");
			}

			public TableInfo TableInfo { get; private set; }

			public bool Temporary { get; private set; }

			public bool IfNotExists { get; private set; }

			internal Prepared(TableInfo tableInfo, bool ifNotExists, bool temporary) {
				TableInfo = tableInfo;
				IfNotExists = ifNotExists;
				Temporary = temporary;
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				try {
					var tableName = TableInfo.TableName;

					if (!context.Request.Access.UserCanCreateTable(tableName))
						throw new MissingPrivilegesException(context.Query.UserName(), tableName, Privileges.Create);

					if (context.Request.Access.TableExists(tableName)) {
						if (!IfNotExists)
							throw new InvalidOperationException(
								String.Format("The table {0} already exists and the IF NOT EXISTS clause was not specified.", tableName));

						return;
					}

					context.Request.Access.CreateTable(TableInfo, Temporary);

					// TODO: Check if to do this we must use a system session 
					context.Request.Access.GrantToUserOnTable(TableInfo.TableName, context.Query.UserName(), Privileges.TableAll);
				} catch (SecurityException ex) {
					throw new StatementException(String.Format("A security error occurred while creating the table '{0}'.", TableInfo.TableName), ex);
				}
			}

			protected override void GetData(SerializationInfo info) {
				info.AddValue("TableInfo", TableInfo, typeof(TableInfo));
				info.AddValue("Temporary", Temporary);
				info.AddValue("IfNotExists", IfNotExists);
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

		#region PreparedSerializer

		//internal class PreparedSerializer : ObjectBinarySerializer<Prepared> {
		//	public override void Serialize(Prepared obj, BinaryWriter writer) {
		//		TableInfo.Serialize(obj.TableInfo, writer);
		//		writer.Write(obj.Temporary);
		//		writer.Write(obj.IfNotExists);
		//	}

		//	public override Prepared Deserialize(BinaryReader reader) {
		//		// TODO: Type Resolver!!!
		//		var tableInfo = TableInfo.Deserialize(reader, null);
		//		var temporary = reader.ReadBoolean();
		//		var ifNotExists = reader.ReadBoolean();

		//		return new Prepared(tableInfo, ifNotExists, temporary);
		//	}
		//}

		#endregion
	}
}

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

using Deveel.Data.Index;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// Provides utilities and properties for handling the <c>SYSTEN</c> schema of a database.
	/// </summary>
	/// <remarks>
	/// The <c>SYSTEM</c> schema holds all the core tables and views for making a database system to
	/// be able to work.
	/// </remarks>
	public static class SystemSchema {
		static SystemSchema() {
			// SYSTEM.TABLE_INFO
			TableInfoTableInfo = new TableInfo(TableInfoTableName);
			TableInfoTableInfo.AddColumn("catalog", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("schema", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("name", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("type", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("other", PrimitiveTypes.String());
			TableInfoTableInfo = TableInfoTableInfo.AsReadOnly();

			// SYSTEM.TABLE_COLUMNS
			TableColumnsTableInfo = new TableInfo(TableColumnsTableName);
			TableColumnsTableInfo.AddColumn("schema", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("table", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("column", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("sql_type", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("type_desc", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("size", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("scale", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean());
			TableColumnsTableInfo.AddColumn("default", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("index_str", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric());
			TableColumnsTableInfo = TableColumnsTableInfo.AsReadOnly();

			// SYSTEM.VARIABLES
			VariablesTableInfo = new TableInfo(VariablesTableName);
			VariablesTableInfo.AddColumn("var", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("type", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("value", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("constant", PrimitiveTypes.Boolean());
			VariablesTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean());
			VariablesTableInfo.AddColumn("is_set", PrimitiveTypes.Boolean());
			VariablesTableInfo = VariablesTableInfo.AsReadOnly();

			// SYSTEM.PRODUCT_INFO
			ProductInfoTableInfo = new TableInfo(ProductInfoTableName);
			ProductInfoTableInfo.AddColumn("var", PrimitiveTypes.String());
			ProductInfoTableInfo.AddColumn("value", PrimitiveTypes.String());
			ProductInfoTableInfo = ProductInfoTableInfo.AsReadOnly();

			// SYSTEM.STATS
			StatisticsTableInfo = new TableInfo(StatisticsTableName);
			StatisticsTableInfo.AddColumn("stat_name", PrimitiveTypes.String());
			StatisticsTableInfo.AddColumn("value", PrimitiveTypes.String());
			StatisticsTableInfo = StatisticsTableInfo.AsReadOnly();

			// SYSTEM.SQL_TYPES
			SqlTypesTableInfo = new TableInfo(SqlTypesTableName);
			SqlTypesTableInfo.AddColumn("TYPE_NAME", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("DATA_TYPE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("PRECISION", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("LITERAL_PREFIX", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("LITERAL_SUFFIX", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("CREATE_PARAMS", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("NULLABLE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("CASE_SENSITIVE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("SEARCHABLE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("UNSIGNED_ATTRIBUTE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("FIXED_PREC_SCALE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("AUTO_INCREMENT", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("LOCAL_TYPE_NAME", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("MINIMUM_SCALE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("MAXIMUM_SCALE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("SQL_DATA_TYPE", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("SQL_DATETIME_SUB", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("NUM_PREC_RADIX", PrimitiveTypes.Numeric());
			SqlTypesTableInfo = SqlTypesTableInfo.AsReadOnly();

			// SYSTEM.OPEN_SESSIONS
			OpenSessionsTableInfo = new TableInfo(OpenSessionsTableName);
			OpenSessionsTableInfo.AddColumn("username", PrimitiveTypes.String());
			OpenSessionsTableInfo.AddColumn("host_string", PrimitiveTypes.String());
			OpenSessionsTableInfo.AddColumn("last_command", PrimitiveTypes.DateTime());
			OpenSessionsTableInfo.AddColumn("time_connected", PrimitiveTypes.DateTime());
			OpenSessionsTableInfo = OpenSessionsTableInfo.AsReadOnly();

			// CONNECTION_INFO
			SessionInfoTableInfo = new TableInfo(SessionInfoTableName);
			SessionInfoTableInfo.AddColumn("var", PrimitiveTypes.String());
			SessionInfoTableInfo.AddColumn("value", PrimitiveTypes.String());
			SessionInfoTableInfo = SessionInfoTableInfo.AsReadOnly();

			// SYSTEM.PRIVS
			PrivilegesTableInfo = new TableInfo(PrivilegesTableName);
			PrivilegesTableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			PrivilegesTableInfo.AddColumn("description", PrimitiveTypes.String());
			PrivilegesTableInfo = PrivilegesTableInfo.AsReadOnly();
		}

		/// <summary>
		/// The name of the system schema that contains tables referring to 
		/// system information.
		/// </summary>
		public const string Name = "SYSTEM";

		/// <summary>
		/// The name of the system schema as <see cref="ObjectName"/>.
		/// </summary>
		public static readonly ObjectName SchemaName = new ObjectName(Name);

		#region Table Names

		public static readonly ObjectName SchemaInfoTableName = new ObjectName(SchemaName, "schema_info");

		public static readonly  ObjectName TableInfoTableName = new ObjectName(SchemaName, "table_info");

		public static readonly ObjectName TableColumnsTableName = new ObjectName(SchemaName, "table_cols");

		public static readonly ObjectName ViewTableName = new ObjectName(SchemaName, "view");

		public static readonly ObjectName RoutineTableName = new ObjectName(SchemaName, "routine");

		public static readonly ObjectName TriggerTableName = new ObjectName(SchemaName, "trigger");

		public static readonly ObjectName RoutineParameterTableName = new ObjectName(SchemaName, "routine_params");

		public static readonly ObjectName VariablesTableName = new ObjectName(SchemaName, "vars");

		public static readonly ObjectName ProductInfoTableName = new ObjectName(SchemaName, "product_info");

		public static readonly ObjectName StatisticsTableName = new ObjectName(SchemaName, "stats");

		///<summary>
		/// 
		///</summary>
		public static readonly ObjectName SequenceInfoTableName = new ObjectName(SchemaName, "sequence_info");

		///<summary>
		///</summary>
		public static readonly ObjectName SequenceTableName = new ObjectName(SchemaName, "sequence");

		public static readonly ObjectName PrimaryKeyInfoTableName = new ObjectName(SchemaName, "pkey_info");

		public static readonly ObjectName PrimaryKeyColumnsTableName = new ObjectName(SchemaName, "pkey_cols");

		public static readonly ObjectName ForeignKeyInfoTableName = new ObjectName(SchemaName, "fkey_info");

		public static readonly ObjectName ForeignKeyColumnsTableName = new ObjectName(SchemaName, "fkey_cols");

		public static readonly  ObjectName UniqueKeyInfoTableName = new ObjectName(SchemaName, "unique_info");

		public static readonly ObjectName UniqueKeyColumnsTableName = new ObjectName(SchemaName, "unique_cols");

		public static readonly ObjectName CheckInfoTableName = new ObjectName(SchemaName, "check_info");

		public static readonly ObjectName OldTriggerTableName = new ObjectName(SchemaName, "OLD");

		public static readonly ObjectName NewTriggerTableName = new ObjectName(SchemaName, "NEW");

		public static readonly ObjectName SqlTypesTableName = new ObjectName(SchemaName, "sql_types");

		public static readonly ObjectName SessionInfoTableName = new ObjectName(SchemaName, "session_info");

		public static readonly ObjectName OpenSessionsTableName = new ObjectName(SchemaName, "open_sessions");

		public static readonly ObjectName PrivilegesTableName = new ObjectName(SchemaName, "privs");


		/// <summary>
		/// Gets the fully qualified name of the <c>user</c> table.
		/// </summary>
		public static readonly ObjectName UserTableName = new ObjectName(SchemaName, "user");

		public static readonly ObjectName PasswordTableName = new ObjectName(SchemaName, "password");

		//public static readonly ObjectName UserConnectPrivilegesTableName = new ObjectName(SchemaName, "user_connect_priv");

		public static readonly ObjectName GroupsTableName = new ObjectName(SchemaName, "group");

		public static readonly ObjectName UserGroupTableName = new ObjectName(SchemaName, "user_group");

		public static readonly ObjectName UserGrantsTableName = new ObjectName(SchemaName, "grants");

		public static readonly ObjectName GroupGrantsTable = new ObjectName(SchemaName, "group_grants");

		#endregion

		#region Table Info

		internal static readonly TableInfo TableInfoTableInfo;

		internal static readonly TableInfo TableColumnsTableInfo;

		internal static readonly TableInfo SqlTypesTableInfo;

		internal static readonly TableInfo VariablesTableInfo;

		internal static readonly TableInfo ProductInfoTableInfo;

		internal static readonly TableInfo StatisticsTableInfo;

		internal static readonly TableInfo SessionInfoTableInfo;

		internal static readonly TableInfo OpenSessionsTableInfo;

		internal static readonly TableInfo PrivilegesTableInfo;

		#endregion

		private static void CreateSecurityTables(IQueryContext context) {
			var tableInfo = new TableInfo(UserTableName);
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			// TODO: User table must be completed ...
			tableInfo = tableInfo.AsReadOnly();
			context.CreateSystemTable(tableInfo);

			context.AddPrimaryKey(UserTableName, new []{"name"}, "SYSTEM_USER_PK");

			tableInfo = new TableInfo(PasswordTableName);
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("method", PrimitiveTypes.String());
			tableInfo.AddColumn("method_args", PrimitiveTypes.Binary());
			tableInfo.AddColumn("identifier", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			context.CreateSystemTable(tableInfo);

			tableInfo = new TableInfo(UserGroupTableName);
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("group", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			context.CreateSystemTable(tableInfo);

			//tableInfo = new TableInfo(UserConnectPrivilegesTableName);
			//tableInfo.AddColumn("user", PrimitiveTypes.String());
			//tableInfo.AddColumn("protocol", PrimitiveTypes.String());
			//tableInfo.AddColumn("host", PrimitiveTypes.String());
			//tableInfo.AddColumn("access", PrimitiveTypes.Boolean());
			//tableInfo = tableInfo.AsReadOnly();
			//context.CreateSystemTable(tableInfo);

			tableInfo = new TableInfo(GroupsTableName);
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo = tableInfo.AsReadOnly();
			context.CreateSystemTable(tableInfo);

			context.AddPrimaryKey(GroupsTableName, new[] { "name" }, "SYSTEM_GROUP_PK");

			tableInfo = new TableInfo(UserGrantsTableName);
			tableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("object", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("grant_option", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("granter", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			context.CreateSystemTable(tableInfo);

			tableInfo = new TableInfo(GroupGrantsTable);
			tableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("object", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("group", PrimitiveTypes.String());

			var fkCol = new[] {"user"};
			var gfkCol = new[] {"group"};
			var refCol = new[] {"name"};
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;
			context.AddForeignKey(PasswordTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_PASSWORD_FK");
			context.AddForeignKey(UserGroupTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_PRIV_FK");
			context.AddForeignKey(UserGroupTableName, gfkCol, GroupsTableName, refCol, onDelete, onUpdate, "USER_GROUP_FK");
			context.AddForeignKey(UserGrantsTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_GRANTS_FK");
			context.AddForeignKey(GroupGrantsTable, gfkCol, GroupsTableName, refCol, onDelete, onUpdate, "GROUP_GRANTS_FK");
		}

		private static void CreateRoutineTables(IQueryContext context) {
			var tableInfo = new TableInfo(RoutineTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("return_type", PrimitiveTypes.String());
			tableInfo.AddColumn("body", PrimitiveTypes.Binary());
			tableInfo = tableInfo.AsReadOnly();
			context.CreateTable(tableInfo);

			tableInfo = new TableInfo(RoutineParameterTableName);
			tableInfo.AddColumn("routine_schema", PrimitiveTypes.String());
			tableInfo.AddColumn("routine_name", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("flags", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("default", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			context.CreateTable(tableInfo);

			var fkCol = new[] { "routine_schema", "routine_name" };
			var refCol = new[] { "schema", "name" };
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;

			context.AddForeignKey(RoutineParameterTableName, fkCol, RoutineTableName, refCol, onDelete, onUpdate, "ROUTINE_PARAMS_FK");
		}

		public static void CreateTables(IQueryContext context) {
			CreateSecurityTables(context);
			CreateRoutineTables(context);
		}

		public static void GrantToPublic(IQueryContext context) {
			context.GrantToUserOnTable(ProductInfoTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(SqlTypesTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(PrivilegesTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(StatisticsTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(VariablesTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(RoutineTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(RoutineParameterTableName, User.PublicName, Privileges.TableRead);
			context.GrantToUserOnTable(SessionInfoTableName, User.PublicName, Privileges.TableRead);
		}

		public static void Setup(ITransaction transaction) {
			// -- Primary Keys --
			// The 'id' columns are primary keys on all the system tables,
			var idCol = new[] { "id" };
			transaction.AddPrimaryKey(PrimaryKeyInfoTableName, idCol, "SYSTEM_PK_PK");
			transaction.AddPrimaryKey(ForeignKeyInfoTableName, idCol, "SYSTEM_FK_PK");
			transaction.AddPrimaryKey(UniqueKeyInfoTableName, idCol, "SYSTEM_UNIQUE_PK");
			transaction.AddPrimaryKey(CheckInfoTableName, idCol, "SYSTEM_CHECK_PK");
			transaction.AddPrimaryKey(SchemaInfoTableName, idCol, "SYSTEM_SCHEMA_PK");

			// -- Foreign Keys --
			// Create the foreign key references,
			var fkCol = new string[1];
			var fkRefCol = new[] { "id" };

			fkCol[0] = "pk_id";
			transaction.AddForeignKey(PrimaryKeyColumnsTableName, fkCol, PrimaryKeyInfoTableName, fkRefCol, "SYSTEM_PK_FK");

			fkCol[0] = "fk_id";
			transaction.AddForeignKey(ForeignKeyColumnsTableName, fkCol, ForeignKeyInfoTableName, fkRefCol, "SYSTEM_FK_FK");

			fkCol[0] = "un_id";
			transaction.AddForeignKey(UniqueKeyColumnsTableName, fkCol, UniqueKeyInfoTableName, fkRefCol, "SYSTEM_UNIQUE_FK");

			// pkey_info 'schema', 'table' column is a unique set,
			// (You are only allowed one primary key per table).
			var columns = new[] { "schema", "table" };
			transaction.AddUniqueKey(PrimaryKeyInfoTableName, columns, "SYSTEM_PKEY_ST_UNIQUE");

			// schema_info 'name' column is a unique column,
			columns = new String[] { "name" };
			transaction.AddUniqueKey(SchemaInfoTableName, columns, "SYSTEM_SCHEMA_UNIQUE");

			//    columns = new String[] { "name" };
			columns = new String[] { "name", "schema" };
			// pkey_info 'name' column is a unique column,
			transaction.AddUniqueKey(PrimaryKeyInfoTableName, columns, "SYSTEM_PKEY_UNIQUE");

			// fkey_info 'name' column is a unique column,
			transaction.AddUniqueKey(ForeignKeyInfoTableName, columns, "SYSTEM_FKEY_UNIQUE");

			// unique_info 'name' column is a unique column,
			transaction.AddUniqueKey(UniqueKeyInfoTableName, columns, "SYSTEM_UNIQUE_UNIQUE");

			// check_info 'name' column is a unique column,
			transaction.AddUniqueKey(CheckInfoTableName, columns, "SYSTEM_CHECK_UNIQUE");
		}

		public static ITable GetTableInfoTable(ITransaction transaction) {
			return new TableInfoTable(transaction);
		}

		public static ITable GetTableColumnsTable(ITransaction transaction) {
			return new TableColumnsTable(transaction);
		}

		public static ITable GetSqlTypesTable(ITransaction transaction) {
			return new SqlTypesTable(transaction);
		}

		public static ITable GetProductInfoTable(ITransaction transaction) {
			return new ProductInfoTable(transaction);
		}

		public static ITable GetOpenSessionsTable(ITransaction transaction) {
			return new OpenSessionsTable(transaction);
		}

		public static ITable GetVariablesTable(ITransaction transaction) {
			return new VariablesTable(transaction);
		}

		public static ITable GetPrivilegesTable(ITransaction transaction) {
			return new PrivilegesTable(transaction);
		}

		public static ITable GetSessionInfoTable(IQueryContext context) {
			return new SessionInfoTable(context.Session());
		}

		public static ITable GetStatisticsTable(ITransaction transaction) {
			return new StatisticsTable(transaction);
		}

		#region TableInfoTable

		class TableInfoTable : GeneratedTable {
			private List<TableInfoObject> tableInfoObjects;
			private int rowCount;

			public TableInfoTable(ITransaction transaction)
				: base(transaction.Database.DatabaseContext) {
				Transaction = transaction;
				tableInfoObjects = new List<TableInfoObject>();

				Init();
			}

			public ITransaction Transaction { get; private set; }

			public override TableInfo TableInfo {
				get { return TableInfoTableInfo; }
			}

			public override int RowCount {
				get { return rowCount; }
			}

			private void Init() {
				// All the tables
				var manager = Transaction.GetTableManager();
				var tableNames = manager.GetTableNames();

				var tableList = tableNames.ToArray();
				Array.Sort(tableList);
				rowCount = tableList.Length;

				foreach (var tableName in tableList) {
					string curType = Transaction.GetTableType(tableName);

					// If the table is in the SYSTEM schema, the type is defined as a
					// SYSTEM TABLE.
					if (curType.Equals("TABLE") &&
						tableName.Parent.Name.Equals(Name)) {
						curType = "SYSTEM TABLE";
					}

					tableInfoObjects.Add(new TableInfoObject(null, tableName.Parent.Name, tableName.Name, curType, null));
				}
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= tableInfoObjects.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var tableInfo = tableInfoObjects[(int) rowNumber];

				switch (columnOffset) {
					case 0:
						return DataObject.String(tableInfo.Catalog);
					case 1:
						return DataObject.String(tableInfo.Schema);
					case 2:
						return DataObject.String(tableInfo.Name);
					case 3:
						return DataObject.String(tableInfo.Type);
					case 4:
						return DataObject.String(tableInfo.Comments);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			#region TableInfoObject

			class TableInfoObject {
				public TableInfoObject(string catalog, string schema, string name, string type, string comments) {
					Catalog = catalog;
					Schema = schema;
					Name = name;
					Type = type;
					Comments = comments;
				}

				public string Name { get; private set; }
				public string Schema { get; private set; }
				public string Catalog { get; private set; }
				public string Type { get; private set; }
				public string Comments { get; private set; }
			}
			#endregion
		}

		#endregion

		#region TableColumnsTable

		class TableColumnsTable : GeneratedTable {
			private ITransaction transaction;

			public TableColumnsTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
				this.transaction = transaction;
			}

			public override TableInfo TableInfo {
				get { return TableColumnsTableInfo; }
			}

			public override int RowCount {
				get { return GetRowCount(); }
			}

			private int GetRowCount() {
				// All the tables
				var tableManager = transaction.GetTableManager();
				var list = tableManager.GetTableNames();

				int colCount = 0;
				foreach (var tableName in list) {
					var info = tableManager.GetTableInfo(tableName);
					if (info == null)
						throw new InvalidOperationException(String.Format("Table information not found for '{0}'.", tableName));

					colCount += info.ColumnCount;
				}

				return colCount;
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				// All the tables
				var tableManager = transaction.GetTableManager();
				var list = tableManager.GetTableNames();
				var visibleTables = list.Select(name => transaction.GetTableInfo(name));

				int rs = 0;
				foreach (var info in visibleTables) {
					var schemaName = info.SchemaName == null ? null : info.SchemaName.FullName;

					int b = rs;
					rs += info.ColumnCount;
					if (rowNumber >= b && rowNumber < rs) {
						// This is the column that was requested,
						var seqNo = rowNumber - b;
						var colInfo = info[(int)seqNo];

						var defaultExpression = colInfo.HasDefaultExpression ? colInfo.DefaultExpression.ToString() : null;

						switch (columnOffset) {
							case 0:  // schema
								return GetColumnValue(columnOffset, new SqlString(schemaName));
							case 1:  // table
								return GetColumnValue(columnOffset, new SqlString(info.Name));
							case 2:  // column
								return GetColumnValue(columnOffset, new SqlString(colInfo.ColumnName));
							case 3:  // sql_type
								return GetColumnValue(columnOffset, new SqlNumber((int)colInfo.ColumnType.TypeCode));
							case 4:  // type_desc
								return GetColumnValue(columnOffset, new SqlString(colInfo.ColumnType.ToString()));
							case 5:  // size
								return GetColumnValue(columnOffset, new SqlNumber(colInfo.Size));
							case 6:  // scale
								return GetColumnValue(columnOffset, new SqlNumber(colInfo.Scale));
							case 7:  // not_null
								return GetColumnValue(columnOffset, (SqlBoolean) colInfo.IsNotNull);
							case 8:  // default
								return GetColumnValue(columnOffset, new SqlString(defaultExpression));
							case 9:  // index_str
								return GetColumnValue(columnOffset, new SqlString(colInfo.IndexType));
							case 10:  // seq_no
								return GetColumnValue(columnOffset, new SqlNumber(seqNo));
							default:
								throw new ArgumentOutOfRangeException("columnOffset");
						}
					}

				}  // for each visible table

				throw new ArgumentOutOfRangeException("rowNumber", "Row out of bounds.");
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region SqlTypesTable

		class SqlTypesTable : GeneratedTable {
			private ITransaction transaction;
			private List<SqlTypeInfo> sqlTypes;

			public SqlTypesTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
				this.transaction = transaction;

				sqlTypes = new List<SqlTypeInfo>();

				Init();
			}

			public override TableInfo TableInfo {
				get { return SqlTypesTableInfo; }
			}

			public override int RowCount {
				get { return sqlTypes.Count; }
			}

			private void AddType(string name, string localName, SqlTypeCode type, byte precision, string prefix, string suffix, bool searchable) {
				sqlTypes.Add(new SqlTypeInfo {
					TypeName = name,
					LocalName = localName,
					Type = type,
					Precision = precision,
					LiteralPrefix = prefix,
					LiteralSuffix = suffix,
					Searchable = (byte)(searchable ? 3 : 0)
				});
			}

			private void Init() {
				AddType("BIT", "BOOLEAN", SqlTypeCode.Bit, 1, null, null, true);
				AddType("BOOLEAN", "BOOLEAN", SqlTypeCode.Boolean, 1, null, null, true);
				AddType("TINYINT", "NUMBER", SqlTypeCode.TinyInt, 9, null, null, true);
				AddType("SMALLINT", "NUMBER", SqlTypeCode.SmallInt, 9, null, null, true);
				AddType("INTEGER", "NUMBER", SqlTypeCode.Integer, 9, null, null, true);
				AddType("BIGINT", "NUMBER", SqlTypeCode.BigInt, 9, null, null, true);
				AddType("FLOAT", "NUMBER", SqlTypeCode.Float, 9, null, null, true);
				AddType("REAL", "NUMBER", SqlTypeCode.Real, 9, null, null, true);
				AddType("DOUBLE", "NUMBER", SqlTypeCode.Double, 9, null, null, true);
				AddType("NUMERIC", "NUMBER", SqlTypeCode.Numeric, 9, null, null, true);
				AddType("DECIMAL", "NUMBER", SqlTypeCode.Decimal, 9, null, null, true);
				AddType("CHAR", "STRING", SqlTypeCode.Char, 9, "'", "'", true);
				AddType("VARCHAR", "STRING", SqlTypeCode.VarChar, 9, "'", "'", true);
				AddType("LONGVARCHAR", "STRING", SqlTypeCode.LongVarChar, 9, "'", "'", true);
				AddType("DATE", "DATETIME", SqlTypeCode.Date, 9, null, null, true);
				AddType("TIME", "DATETIME", SqlTypeCode.Time, 9, null, null, true);
				AddType("TIMESTAMP", "DATETIME", SqlTypeCode.TimeStamp, 9, null, null, true);
				AddType("BINARY", "BINARY", SqlTypeCode.Binary, 9, null, null, false);
				AddType("VARBINARY", "BINARY", SqlTypeCode.VarBinary, 9, null, null, false);
				AddType("LONGVARBINARY", "BINARY", SqlTypeCode.LongVarBinary, 9, null, null, false);
				AddType("OBJECT", "OBJECT", SqlTypeCode.Object, 9, null, null, false);
				AddType("TYPE", "TYPE", SqlTypeCode.Type, 9, null, null, false);
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				// TODO: handle also the user-types here?

				if (rowNumber < 0 || rowNumber >= sqlTypes.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var typeInfo = sqlTypes[(int)rowNumber];
				switch (columnOffset) {
					case 0:  // type_name
						return GetColumnValue(columnOffset, new SqlString(typeInfo.TypeName));
					case 1:  // data_type
						return GetColumnValue(columnOffset, new SqlNumber((int)typeInfo.Type));
					case 2:  // precision
						return GetColumnValue(columnOffset, new SqlNumber(typeInfo.Precision));
					case 3:  // literal_prefix
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LiteralPrefix));
					case 4:  // literal_suffix
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LiteralSuffix));
					case 5:  // create_params
						return GetColumnValue(columnOffset, SqlString.Null);
					case 6:  // nullable
						return GetColumnValue(columnOffset, SqlNumber.One);
					case 7:  // case_sensitive
						return GetColumnValue(columnOffset, SqlBoolean.True);
					case 8:  // searchable
						return GetColumnValue(columnOffset, new SqlNumber(typeInfo.Searchable));
					case 9:  // unsigned_attribute
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 10:  // fixed_prec_scale
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 11:  // auto_increment
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 12:  // local_type_name
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LocalName));
					case 13:  // minimum_scale
						return GetColumnValue(columnOffset, SqlNumber.Zero);
					case 14:  // maximum_scale
						return GetColumnValue(columnOffset, new SqlNumber(10000000));
					case 15:  // sql_data_type
						return GetColumnValue(columnOffset, SqlNull.Value);
					case 16:  // sql_datetype_sub
						return GetColumnValue(columnOffset, SqlNull.Value);
					case 17:  // num_prec_radix
						return GetColumnValue(columnOffset, new SqlNumber(10));
					default:
						throw new ArgumentOutOfRangeException("columnOffset");

				}
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				sqlTypes = null;

				base.Dispose(disposing);
			}

			#region SqlTypeInfo

			class SqlTypeInfo {
				public string TypeName;
				public string LocalName;
				public SqlTypeCode Type;
				public byte Precision;
				public string LiteralPrefix;
				public string LiteralSuffix;
				public byte Searchable;
			}

			#endregion
		}

		#endregion

		#region OpenSessionsTable

		private class OpenSessionsTable : GeneratedTable {
			private ITransaction transaction;

			public OpenSessionsTable(ITransaction transaction)
				: base(transaction.Database.DatabaseContext) {
				this.transaction = transaction;
			}

			public override TableInfo TableInfo {
				get { return OpenSessionsTableInfo; }
			}

			public override int RowCount {
				get { return transaction.Database.DatabaseContext.Sessions.Count; }
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= transaction.Database.DatabaseContext.Sessions.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var session = transaction.Database.DatabaseContext.Sessions[(int) rowNumber].SessionInfo;

				switch (columnOffset) {
					case 0:
						return GetColumnValue(0, new SqlString(session.User.Name));
					case 1:
						return GetColumnValue(1, new SqlString(session.EndPoint.ToString()));
					case 2:
						return GetColumnValue(2, ((SqlDateTime)session.LastCommandTime));
					case 3:
						return GetColumnValue(3, (SqlDateTime)session.StartedOn);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region ProductInfoTable

		class ProductInfoTable : GeneratedTable {
			private List<ISqlString> keyValuePairs;

			public ProductInfoTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
				Init();
			}

			public override TableInfo TableInfo {
				get { return ProductInfoTableInfo; }
			}

			public override int RowCount {
				get { return keyValuePairs.Count/2; }
			}

			private void Init() {
				keyValuePairs = new List<ISqlString>();

				var productInfo = ProductInfo.Current;

				// Set up the product variables.
				keyValuePairs.Add(new SqlString("title"));
				keyValuePairs.Add(new SqlString(productInfo.Title));

				keyValuePairs.Add(new SqlString("version"));
				keyValuePairs.Add(new SqlString(productInfo.Version.ToString()));

				keyValuePairs.Add(new SqlString("copyright"));
				keyValuePairs.Add(new SqlString(productInfo.Copyright));

				keyValuePairs.Add(new SqlString("description"));
				keyValuePairs.Add(new SqlString(productInfo.Description));

				keyValuePairs.Add(new SqlString("company"));
				keyValuePairs.Add(new SqlString(productInfo.Company));
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				switch (columnOffset) {
					case 0:  // var
						return GetColumnValue(columnOffset, keyValuePairs[(int)rowNumber * 2]);
					case 1:  // value
						return GetColumnValue(columnOffset, keyValuePairs[(int)(rowNumber * 2) + 1]);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}
		}
		
		#endregion

		#region VariablesTable

		class VariablesTable : GeneratedTable {
			private ITransaction transaction;

			public VariablesTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
				this.transaction = transaction;
			}

			public override TableInfo TableInfo {
				get { return VariablesTableInfo; }
			}

			public override int RowCount {
				get { throw new NotImplementedException(); }
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				throw new NotImplementedException();
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region PrivilegesTable

		class PrivilegesTable : GeneratedTable {
			private readonly IList<KeyValuePair<string, int>> privBits;

			public PrivilegesTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
				privBits = FormPrivilegesValues();
			}

			private IList<KeyValuePair<string, int>> FormPrivilegesValues() {
				var names = Enum.GetNames(typeof (Privileges));
				var values = Enum.GetValues(typeof (Privileges));

				return names.Select((t, i) => new KeyValuePair<string, int>(t, (int) values.GetValue(i))).ToList();
			}

			public override TableInfo TableInfo {
				get { return PrivilegesTableInfo; }
			}

			public override int RowCount {
				get { return privBits.Count; }
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= privBits.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var pair = privBits[(int) rowNumber];
				switch (columnOffset) {
					case 0:
						return DataObject.Integer(pair.Value);
					case 1:
						return DataObject.VarChar(pair.Key);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}
		}

		#endregion

		#region SessionInfoTable

		class SessionInfoTable : GeneratedTable {
			public SessionInfoTable(IUserSession session) 
				: base(session.Database.DatabaseContext) {
			}

			public override TableInfo TableInfo {
				get { throw new NotImplementedException(); }
			}

			public override int RowCount {
				get { throw new NotImplementedException(); }
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region StatisticsTable

		class StatisticsTable : GeneratedTable {
			public StatisticsTable(ITransaction transaction) 
				: base(transaction.Database.DatabaseContext) {
			}

			public override TableInfo TableInfo {
				get { throw new NotImplementedException(); }
			}

			public override int RowCount {
				get { throw new NotImplementedException(); }
			}

			public override DataObject GetValue(long rowNumber, int columnOffset) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}

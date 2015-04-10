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

using Deveel.Data.Sql;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
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
		}

		/// <summary>
		/// The name of the system schema that contains tables refering to 
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


		/// <summary>
		/// Gets the fully qualified name of the <c>user</c> table.
		/// </summary>
		public static readonly ObjectName UserTableName = new ObjectName(SchemaName, "user");

		public static readonly ObjectName PasswordTableName = new ObjectName(SchemaName, "password");

		public static readonly ObjectName UserConnectPrivilegesTableName = new ObjectName(SchemaName, "user_connect_priv");

		public static readonly ObjectName UserPrivilegesTableName = new ObjectName(SchemaName, "user_priv");

		public static readonly ObjectName UserGrantsTableName = new ObjectName(SchemaName, "grants");

		#endregion

		#region Table Info

		internal static readonly TableInfo TableInfoTableInfo;

		internal static readonly TableInfo TableColumnsTableInfo;

		#endregion

		public static void CreateSystemTables(ITransaction transaction) {
			// SYSTEM.SCHEMA_INFO
			var tableInfo = new TableInfo(SchemaInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.String());
			tableInfo.AddColumn("culture", PrimitiveTypes.String());
			tableInfo.AddColumn("other", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.SEQUENCE_INFO
			tableInfo = new TableInfo(SequenceInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.SEQUENCE
			tableInfo = new TableInfo(SequenceTableName);
			tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("increment", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("start", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cache", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.PKEY_INFO
			tableInfo = new TableInfo(PrimaryKeyInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("table", PrimitiveTypes.String());
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.PKEY_COLS
			tableInfo = new TableInfo(PrimaryKeyColumnsTableName);
			tableInfo.AddColumn("pk_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("column", PrimitiveTypes.String());
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.FKEY_INFO
			tableInfo = new TableInfo(ForeignKeyInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("table", PrimitiveTypes.String());
			tableInfo.AddColumn("ref_schema", PrimitiveTypes.String());
			tableInfo.AddColumn("ref_table", PrimitiveTypes.String());
			tableInfo.AddColumn("update_rule", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("delete_rule", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.FKEY_COLS
			tableInfo = new TableInfo(ForeignKeyColumnsTableName);
			tableInfo.AddColumn("fk_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("fcolumn", PrimitiveTypes.String());
			tableInfo.AddColumn("pcolumn", PrimitiveTypes.String());
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.UNIQUE_INFO
			tableInfo = new TableInfo(UniqueKeyInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("table", PrimitiveTypes.String());
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.UNIQUE_COLS
			tableInfo = new TableInfo(UniqueKeyColumnsTableName);
			tableInfo.AddColumn("un_id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("column", PrimitiveTypes.String());
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);

			// SYSTEM.CHECK_INFO
			tableInfo = new TableInfo(CheckInfoTableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("table", PrimitiveTypes.String());
			tableInfo.AddColumn("expression", PrimitiveTypes.String());
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("serialized_expression", PrimitiveTypes.Binary());
			tableInfo = tableInfo.AsReadOnly();
			transaction.CreateTable(tableInfo);
		}

		private static void CreateSecurityTables(IUserSession session) {
			var tableInfo = new TableInfo(UserTableName);
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			// TODO: User table must be completed ...
			tableInfo = tableInfo.AsReadOnly();
			session.CreateTable(tableInfo);

			session.AddPrimaryKey(UserTableName, new []{"name"}, "SYSTEM_USER_PK");

			tableInfo = new TableInfo(PasswordTableName);
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("hash", PrimitiveTypes.String());
			tableInfo.AddColumn("salt", PrimitiveTypes.String());
			tableInfo.AddColumn("hash_algorithm", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			session.CreateTable(tableInfo);

			tableInfo = new TableInfo(UserPrivilegesTableName);
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("group", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			session.CreateTable(tableInfo);

			tableInfo = new TableInfo(UserConnectPrivilegesTableName);
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("protocol", PrimitiveTypes.String());
			tableInfo.AddColumn("host", PrimitiveTypes.String());
			tableInfo.AddColumn("access", PrimitiveTypes.Boolean());
			tableInfo = tableInfo.AsReadOnly();
			session.CreateTable(tableInfo);

			tableInfo = new TableInfo(UserGrantsTableName);
			tableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("object", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("user", PrimitiveTypes.String());
			tableInfo.AddColumn("grant_option", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("granter", PrimitiveTypes.String());
			tableInfo = tableInfo.AsReadOnly();
			session.CreateTable(tableInfo);

			var fkCol = new[] {"user"};
			var refCol = new[] {"name"};
			const ForeignKeyAction onUpdate = ForeignKeyAction.NoAction;
			const ForeignKeyAction onDelete = ForeignKeyAction.Cascade;
			session.AddForeignKey(PasswordTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_PASSWORD_FK");
			session.AddForeignKey(UserPrivilegesTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_PRIV_FK");
			session.AddForeignKey(UserConnectPrivilegesTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_CONNPRIV_FK");
			session.AddForeignKey(UserGrantsTableName, fkCol, UserTableName, refCol, onDelete, onUpdate, "USER_GRANTS_FK");
		}

		public static void CreateTables(IUserSession session) {
			CreateSecurityTables(session);
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

		#region TableInfoTable

		class TableInfoTable : GeneratedTable {
			private List<TableInfoObject> tableInfoObjects;
			private int rowCount;

			public TableInfoTable(ITransaction transaction)
				: base(transaction.Context.Database.Context) {
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
	}
}

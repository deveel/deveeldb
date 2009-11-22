using System;
using System.Collections;
using System.Data;

using Deveel.Collections;
using Deveel.Data.Client;

namespace Deveel.Data.Shell {
	/// <summary>
	/// The representation of the structure of a database on the system
	/// to which the client is connected.
	/// </summary>
	public sealed class Database {
		internal Database(SqlSession session) {
			this.session = session;
			tables = new TreeSet();

			Build();
		}

		private readonly SqlSession session;
		private readonly ISortedSet tables;
		private readonly ISortedSet users;

		public ICollection Tables {
			get { return tables; }
		}

		private void Build() {
			string[] filter = new string[] { null, null, null, "TABLE", "SYSTEM TABLE", "VIEW" };
			System.Data.DataTable tablesSchema = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.Tables, filter);
			for (int i = 0; i < tablesSchema.Rows.Count; i++) {
				DataRow row = tablesSchema.Rows[i];
				BuildTable(row);
			}
		}

		private void BuildTable(DataRow row) {
			string tableSchema = row["TABLE_SCHEMA"].ToString();
			string tableName = row["TABLE_NAME"].ToString();
			string tableType = row["TABLE_TYPE"].ToString();

			Table table = new Table(tableSchema, tableName, tableType);

			string[] filter = new string[] { null, tableSchema, tableName, null };
			System.Data.DataTable dataTable = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.TablePrivileges, filter);

			for (int i = 0; i < dataTable.Rows.Count; i++)
				table.AddPrivilege(BuildPrivilege(dataTable.Rows[i]));

			dataTable = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.Columns, filter);
			for (int i = 0; i < dataTable.Rows.Count; i++)
				BuildColumn(table, dataTable.Rows[i]);

			dataTable = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.PrimaryKeys, filter);
			for (int i = 0; i < dataTable.Rows.Count; i++)
				BuildPrimaryKey(table, dataTable.Rows[i]);

			dataTable = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.ExportedKeys, filter);
			for (int i = 0; i < dataTable.Rows.Count; i++)
				BuildForeignKey(table, dataTable.Rows[i]);

			tables.Add(table);
		}

		private void BuildColumn(Table table, DataRow row) {
			string tableSchema = row["TABLE_SCHEMA"].ToString();
			string tableName = row["TABLE_NAME"].ToString();
			string columnName = row["COLUMN_NAME"].ToString();
			SqlType dataType = (SqlType)(int) row["DATA_TYPE"];

			int position = (int) row["ORDINAL_POSITION"];
			string dataTypeName = row["TYPE_NAME"].ToString();
			int size = (int) row["COLUMN_SIZE"];
			int scale = (int) row["NUM_PREC_RADIX"];
			bool nullable = (bool) row["NULLABLE"];
			Column column = new Column(table, columnName, position, dataTypeName, size, scale, nullable);
			string defaultValue = row["COLUMN_DEFAULT"].ToString();
			if (defaultValue != null && defaultValue.Length > 0)
				column.SetDefault(defaultValue);

			string[] filter = new string[] { null, tableSchema, tableName, columnName };
			System.Data.DataTable columnPrivs = session.Connection.GetSchema(DeveelDbMetadataSchemaNames.ColumnPrivileges, filter);
			for (int i = 0; i < columnPrivs.Rows.Count; i++)
				column.AddPrivilege(BuildPrivilege(columnPrivs.Rows[i]));

			table.AddColumn(column);
		}

		private static Privilege BuildPrivilege(DataRow row) {
			string grantor = row["GRANTOR"].ToString();
			string grantee = row["GRANTEE"].ToString();
			string priv = row["PRIVILEGE"].ToString();
			bool grantable = (bool) row["IS_GRANTABLE"];

			return new Privilege(grantor, grantee, priv, grantable);
		}

		private static void BuildPrimaryKey(Table table, DataRow row) {
			string name = row["PK_NAME"] as string;
			PrimaryKey primaryKey;
			if (name != null) {
				primaryKey = table.GetPrimaryKey(name);
			} else {
				primaryKey = table.PrimaryKey;
			}

			if (primaryKey == null) {
				primaryKey = new PrimaryKey(table, name);
				table.SetPrimaryKey(primaryKey);
			}

			string columnName = row["COLUMN_NAME"].ToString();
			primaryKey.AddColumn(columnName);
		}

		private static void BuildForeignKey(Table table, DataRow row) {
			string name = row["FK_NAME"].ToString();
			ForeignKey fkey = table.GetForeignKey(name);
			if (fkey == null) {
				string refSchema = row["PKTABLE_SCHEMA"].ToString();
				string refTable = row["PKTABLE_NAME"].ToString();
				string updateRule = row["UPDATE_RULE"].ToString();
				string deleteRule = row["DELETE_RULE"].ToString();
				int deferrability = (int) row["DEFERRABILITY"];

				fkey = new ForeignKey(name, refSchema, refTable, updateRule, deleteRule, deferrability);
				table.AddForeignKey(fkey);
			}

			string columnName = row["PKCOLUMN_NAME"].ToString();
			string refColumnName = row["FKCOLUMN_NAME"].ToString();
			fkey.AddPkColumn(columnName);
			fkey.AddFkColumn(refColumnName);
		}

		public Table GetTable(string name) {
			foreach (Table table in tables) {
				if (table.Name == name)
					return table;
			}

			return null;
		}
	}
}
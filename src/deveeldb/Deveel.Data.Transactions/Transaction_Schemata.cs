// 
//  Copyright 2010-2011  Deveel
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

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	internal partial class Transaction {
		/// <summary>
		/// Create a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to create.</param>
		/// <param name="type">The type to assign to the schema.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Any security checks must be performed before this method is called.
		/// </para>
		/// <para>
		/// <b>Note</b>: We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a schema with the same <paramref name="name"/> already exists.
		/// </exception>
		public void CreateSchema(string name, string type) {
			TableName tableName = TableDataConglomerate.SchemaInfoTable;
			IMutableTableDataSource t = GetMutableTable(tableName);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			try {
				// Select entries where;
				//     schema_info.name = name
				if (dt.Exists(1, name))
					throw new StatementException("Schema already exists: " + name);

				// Add the entry to the schema info table.
				DataRow rd = new DataRow(t);
				BigNumber uniqueId = NextUniqueID(tableName);
				rd.SetValue(0, uniqueId);
				rd.SetValue(1, name);
				rd.SetValue(2, type);
				// Third (other) column is left as null
				t.AddRow(rd);
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to drop.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Note that any security checks must be performed before this method 
		/// is called.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void DropSchema(string name) {
			TableName tableName = TableDataConglomerate.SchemaInfoTable;
			IMutableTableDataSource t = GetMutableTable(tableName);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Drop a single entry from dt where column 1 = name
			try {
				if (!dt.Delete(1, name))
					throw new StatementException("Schema doesn't exists: " + name);
			} finally {
				dt.Dispose();
			}				
		}

		/// <summary>
		/// Returns true if the schema exists within this transaction.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public bool SchemaExists(String name) {
			TableName table_name = TableDataConglomerate.SchemaInfoTable;
			ITableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Returns true if there's a single entry in dt where column 1 = name
			try {
				return dt.Exists(1, name);
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Resolves the case of the given schema name if the database is 
		/// performing case insensitive identifier matching.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="ignoreCase"></param>
		/// <returns>
		/// Returns a SchemaDef object that identifiers the schema. 
		/// Returns null if the schema name could not be resolved.
		/// </returns>
		public SchemaDef ResolveSchemaCase(String name, bool ignoreCase) {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(GetTable(TableDataConglomerate.SchemaInfoTable));

			try {
				IRowEnumerator e = dt.GetRowEnumerator();
				if (ignoreCase) {
					SchemaDef result = null;
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						String cur_name = dt.Get(1, rowIndex).Object.ToString();
						if (String.Compare(name, cur_name, true) == 0) {
							if (result != null)
								throw new StatementException("Ambiguous schema name: '" + name + "'");

							string type = dt.Get(2, rowIndex).Object.ToString();
							result = new SchemaDef(cur_name, type);
						}
					}
					return result;

				} else {  // if (!ignore_case)
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						string curName = dt.Get(1, rowIndex).Object.ToString();
						if (name.Equals(curName)) {
							string type = dt.Get(2, rowIndex).Object.ToString();
							return new SchemaDef(curName, type);
						}
					}
					// Not found
					return null;
				}
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Returns an array of <see cref="SchemaDef"/> objects for each schema 
		/// currently setup in the database.
		/// </summary>
		/// <returns></returns>
		public SchemaDef[] GetSchemaList() {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(GetTable(TableDataConglomerate.SchemaInfoTable));
			IRowEnumerator e = dt.GetRowEnumerator();
			SchemaDef[] arr = new SchemaDef[dt.RowCount];
			int i = 0;

			while (e.MoveNext()) {
				int row_index = e.RowIndex;
				string cur_name = dt.Get(1, row_index).Object.ToString();
				string cur_type = dt.Get(2, row_index).Object.ToString();
				arr[i] = new SchemaDef(cur_name, cur_type);
				++i;
			}

			dt.Dispose();
			return arr;
		} 
	}
}
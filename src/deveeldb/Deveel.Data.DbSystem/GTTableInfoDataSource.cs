// 
//  Copyright 2010  Deveel
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
using System.Collections.Generic;

using Deveel.Data.Deveel.Data.DbSystem;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents information about the tables in all schema.
	/// </summary>
	/// <remarks>
	/// <b>Note</b> This is not designed to be a long kept object. It must not 
	/// last beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTTableInfoDataSource : GTDataSource {
		/// <summary>
		/// The transaction that is the view of this information.
		/// </summary>
		private Transaction transaction;

		private List<TTableInfo> tableInfos;

		/// <summary>
		/// The number of rows in this table.
		/// </summary>
		private int rowCount;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DataTableInfo;

		static GTTableInfoDataSource() {

			DataTableInfo info = new DataTableInfo(SystemSchema.TableInfo);

			// Add column definitions
			info.AddColumn("schema", PrimitiveTypes.VarString);
			info.AddColumn("name", PrimitiveTypes.VarString);
			info.AddColumn("type", PrimitiveTypes.VarString);
			info.AddColumn("other", PrimitiveTypes.VarString);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;

		}

		public GTTableInfoDataSource(Transaction transaction)
			: base(transaction.Context) {
			this.transaction = transaction;
			tableInfos = new List<TTableInfo>();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTTableInfoDataSource Init() {
			// All the tables
			TableName[] tableList = transaction.GetTables();
			Array.Sort(tableList);
			rowCount = tableList.Length;

			foreach (TableName tableName in tableList) {
				string curType = transaction.GetTableType(tableName);

				// If the table is in the SYSTEM schema, the type is defined as a
				// SYSTEM TABLE.
				if (curType.Equals("TABLE") &&
					tableName.Schema.Equals("SYSTEM")) {
					curType = "SYSTEM TABLE";
				}

				TTableInfo tableInfo = new TTableInfo();
				tableInfo.Name = tableName.Name;
				tableInfo.Schema = tableName.Schema;
				tableInfo.Type = curType;

				tableInfos.Add(tableInfo);
			}

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return rowCount; }
		}

		public override TObject GetCell(int column, int row) {
			TTableInfo info = tableInfos[row];
			switch (column) {
				case 0:  // schema
					return GetColumnValue(column, info.Schema);
				case 1:  // name
					return GetColumnValue(column, info.Name);
				case 2:  // type
					return GetColumnValue(column, info.Type);
				case 3:  // other
					// Table notes, etc.  (future enhancement)
					return GetColumnValue(column, info.Notes);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			tableInfos = null;
			transaction = null;
		}

		#region TableInfo

		class TTableInfo {
			public string Name;
			public string Schema;
			public string Type;
			public string Notes;

		}

		#endregion
	}
}
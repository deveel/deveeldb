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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IInternalTableInfo"/> that provides a 
	/// number of methods to aid in the productions of the 
	/// <see cref="IInternalTableInfo"/> interface for a transaction specific 
	/// model of a set of tables that is based on a single system table.
	/// </summary>
	/// <remarks>
	/// This would be used to model table views for triggers, views, procedures and
	/// sequences all of which are table sets tied to a single table respectively,
	/// and the number of items in the table represent the number of tables to model.
	/// <para>
	/// This abstraction assumes that the name of the schema/table are in columns 0
	/// and 1 of the backed system table.
	/// </para>
	/// </remarks>
	abstract class InternalTableInfo2 : IInternalTableInfo {
		/// <summary>
		/// The transaction we are connected to.
		/// </summary>
		protected readonly Transaction transaction;
		/// <summary>
		/// The table in the transaction that contains the list of items we 
		/// are modelling.
		/// </summary>
		protected readonly TableName table_name;

		protected InternalTableInfo2(Transaction transaction, TableName table_name) {
			this.transaction = transaction;
			this.table_name = table_name;
		}

		/// <inheritdoc/>
		public int TableCount {
			get { return transaction.TableExists(table_name) ? transaction.GetTable(table_name).RowCount : 0; }
		}

		/// <inheritdoc/>
		public int FindTableName(TableName name) {
			if (transaction.RealTableExists(table_name)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				ITableDataSource table = transaction.GetTable(table_name);
				IRowEnumerator row_e = table.GetRowEnumerator();
				int p = 0;
				while (row_e.MoveNext()) {
					int row_index = row_e.RowIndex;
					TObject ob_name = table.GetCell(1, row_index);
					if (ob_name.Object.ToString().Equals(name.Name)) {
						TObject ob_schema = table.GetCell(0, row_index);
						if (ob_schema.Object.ToString().Equals(name.Schema)) {
							// Match so return this
							return p;
						}
					}
					++p;
				}
			}
			return -1;
		}

		/// <inheritdoc/>
		public TableName GetTableName(int i) {
			if (transaction.RealTableExists(table_name)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				ITableDataSource table = transaction.GetTable(table_name);
				IRowEnumerator row_e = table.GetRowEnumerator();
				int p = 0;
				while (row_e.MoveNext()) {
					int row_index = row_e.RowIndex;
					if (i == p) {
						TObject ob_schema = table.GetCell(0, row_index);
						TObject ob_name = table.GetCell(1, row_index);
						return new TableName(ob_schema.Object.ToString(),
											 ob_name.Object.ToString());
					}
					++p;
				}
			}
			throw new Exception("Out of bounds.");
		}

		/// <inheritdoc/>
		public bool ContainsTableName(TableName name) {
			// This set can not contain the table that is backing it, so we always
			// return false for that.  This check stops an annoying recursive
			// situation for table name resolution.
			if (name.Equals(table_name))
				return false;
			return FindTableName(name) != -1;
		}

		/// <inheritdoc/>
		public abstract DataTableInfo GetTableInfo(int i);

		/// <inheritdoc/>
		public abstract String GetTableType(int i);

		/// <inheritdoc/>
		public abstract ITableDataSource CreateInternalTable(int index);

	}
}
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

using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An implementation of <see cref="IInternalTableContainer"/> that provides a 
	/// number of methods to aid in the productions of the 
	/// <see cref="IInternalTableContainer"/> interface for a transaction specific 
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
	abstract class TransactionInternalTableContainer : IInternalTableContainer {
		/// <summary>
		/// The transaction we are connected to.
		/// </summary>
		protected ITransaction Transaction { get; private set; }

		/// <summary>
		/// The table in the transaction that contains the list of items we 
		/// are modelling.
		/// </summary>
		protected TableName TableName { get; private set; }

		protected TransactionInternalTableContainer(ITransaction transaction, TableName tableName) {
			Transaction = transaction;
			TableName = tableName;
		}

		/// <inheritdoc/>
		public int TableCount {
			get { return Transaction.TableExists(TableName) ? Transaction.GetTable(TableName).RowCount : 0; }
		}

		/// <inheritdoc/>
		public int FindTableName(TableName name) {
			if (Transaction.RealTableExists(TableName)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				ITableDataSource table = Transaction.GetTable(TableName);
				IRowEnumerator rowE = table.GetRowEnumerator();
				int p = 0;
				while (rowE.MoveNext()) {
					int rowIndex = rowE.RowIndex;
					TObject obName = table.GetCell(1, rowIndex);
					if (obName.Object.ToString().Equals(name.Name)) {
						TObject obSchema = table.GetCell(0, rowIndex);
						if (obSchema.Object.ToString().Equals(name.Schema)) {
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
			if (Transaction.RealTableExists(TableName)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				ITableDataSource table = Transaction.GetTable(TableName);
				IRowEnumerator rowE = table.GetRowEnumerator();
				int p = 0;
				while (rowE.MoveNext()) {
					int rowIndex = rowE.RowIndex;
					if (i == p) {
						TObject obSchema = table.GetCell(0, rowIndex);
						TObject obName = table.GetCell(1, rowIndex);
						return new TableName(obSchema.Object.ToString(),
											 obName.Object.ToString());
					}
					++p;
				}
			}
			throw new Exception("Out of bounds.");
		}

		/// <inheritdoc/>
		public bool ContainsTable(TableName name) {
			// This set can not contain the table that is backing it, so we always
			// return false for that.  This check stops an annoying recursive
			// situation for table name resolution.
			if (name.Equals(TableName))
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
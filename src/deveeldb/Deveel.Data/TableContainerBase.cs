// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// A base implementation of the <see cref="ITableContainer"/> interface to provide
	/// utility methods to resolve objects that have a standard name.
	/// </summary>
	/// <seealso cref="Deveel.Data.Sql.Tables.ITableContainer" />
	public abstract class TableContainerBase : ITableContainer {
		/// <summary>
		/// Initializes a new instance of the <see cref="TableContainerBase"/> class wrapping
		/// it within the given transaction and using the given name of the table that contains
		/// the objects to resolve.
		/// </summary>
		/// <param name="transaction">The transaction that accesses the objects.</param>
		/// <param name="tableName">The name of the table that contains the objects to resolve.</param>
		/// <exception cref="System.ArgumentNullException">transaction</exception>
		protected TableContainerBase(ITransaction transaction, ObjectName tableName) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
			TableName = tableName;
		}

		/// <summary>
		/// Gets the instance of the transaction that wraps this container.
		/// </summary>
		protected ITransaction Transaction { get; private set; }

		/// <summary>
		/// Gets the name of the table that is used to resolve the objects.
		/// </summary>
		protected ObjectName TableName { get; private set; }

		/// <inheritdoc/>
		public int TableCount {
			get { return Transaction.TableExists(TableName) ? Transaction.GetTable(TableName).RowCount : 0; }
		}

		protected virtual int SchemaColumnOffset {
			get { return 0; }
		}

		protected virtual int NameColumnOffset {
			get { return 1; }
		}

		/// <inheritdoc/>
		public int FindByName(ObjectName name) {
			if (Transaction.RealTableExists(TableName)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				var table = Transaction.GetTable(TableName);
				var rowE = table.GetEnumerator();
				int p = 0;
				while (rowE.MoveNext()) {
					int rowIndex = rowE.Current.RowId.RowNumber;
					var obName = table.GetValue(rowIndex, NameColumnOffset);
					if (obName.Value.ToString().Equals(name.Name)) {
						var obSchema = table.GetValue(rowIndex, SchemaColumnOffset);
						if (obSchema.Value.ToString().Equals(name.ParentName)) {
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
		public ObjectName GetTableName(int offset) {
			if (Transaction.RealTableExists(TableName)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				var table = Transaction.GetTable(TableName);
				var rowE = table.GetEnumerator();
				int p = 0;
				while (rowE.MoveNext()) {
					int rowIndex = rowE.Current.RowId.RowNumber;
					if (offset == p) {
						var obSchema = table.GetValue(rowIndex, SchemaColumnOffset);
						var obName = table.GetValue(rowIndex, NameColumnOffset);
						return new ObjectName(new ObjectName(obSchema.Value.ToString()), obName.Value.ToString());
					}
					++p;
				}
			}

			throw new IndexOutOfRangeException("Out of bounds.");
		}

		/// <inheritdoc/>
		public abstract TableInfo GetTableInfo(int offset);

		/// <inheritdoc/>
		public abstract string GetTableType(int offset);

		/// <inheritdoc/>
		public bool ContainsTable(ObjectName name) {
			// This set can not contain the table that is backing it, so we always
			// return false for that.  This check stops an annoying recursive
			// situation for table name resolution.
			if (name.Equals(TableName))
				return false;

			return FindByName(name) != -1;
		}

		/// <inheritdoc/>
		public abstract ITable GetTable(int offset);
	}
}

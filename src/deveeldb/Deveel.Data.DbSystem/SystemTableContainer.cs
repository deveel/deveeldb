using System;

using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	abstract class SystemTableContainer : ITableContainer {
		protected SystemTableContainer(ITransaction transaction, ObjectName tableName) {
			Transaction = transaction;
			TableName = tableName;
		}

		public ITransaction Transaction { get; private set; }

		public ObjectName TableName { get; private set; }

		public int TableCount {
			get { return Transaction.TableExists(TableName) ? Transaction.GetTable(TableName).RowCount : 0; }
		}

		public int FindByName(ObjectName name) {
			if (Transaction.RealTableExists(TableName)) {
				// Search the table.  We assume that the schema and name of the object
				// are in columns 0 and 1 respectively.
				var table = Transaction.GetTable(TableName);
				var rowE = table.GetEnumerator();
				int p = 0;
				while (rowE.MoveNext()) {
					int rowIndex = rowE.Current.RowId.RowNumber;
					var obName = table.GetValue(rowIndex, 1);
					if (obName.Value.ToString().Equals(name.Name)) {
						var obSchema = table.GetValue(rowIndex, 0);
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
						var obSchema = table.GetValue(rowIndex, 0);
						var obName = table.GetValue(rowIndex, 1);
						return new ObjectName(new ObjectName(obSchema.Value.ToString()), obName.Value.ToString());
					}
					++p;
				}
			}

			throw new Exception("Out of bounds.");
		}

		public abstract TableInfo GetTableInfo(int offset);

		public abstract string GetTableType(int offset);

		public bool ContainsTable(ObjectName name) {
			// This set can not contain the table that is backing it, so we always
			// return false for that.  This check stops an annoying recursive
			// situation for table name resolution.
			if (name.Equals(TableName))
				return false;

			return FindByName(name) != -1;
		}

		public abstract ITable GetTable(int offset);
	}
}

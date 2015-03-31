using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Index;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Transactions {
	class Transaction : ITransaction {
		public Transaction(IDatabase database, long commitId, TransactionIsolation isolation, IEnumerable<TableSource> committedTables, IEnumerable<IIndexSet> indexSets) {
			CommitId = commitId;
			Database = database;
			Isolation = isolation;

			Registry = new TransactionRegistry(this);
		}

		public long CommitId { get; private set; }

		public TransactionIsolation Isolation { get; private set; }

		public bool IsReadOnly { get; private set; }

		ITransactionContext ITransaction.Context {
			get { return Database; }
		}

		public IDatabase Database { get; private set; }

		public IObjectManagerResolver ObjectManagerResolver { get; private set; }

		public TransactionRegistry Registry { get; private set; }

		public ObjectName TryResolveCase(ObjectName objName) {
			throw new NotImplementedException();
		}

		public SqlNumber SetTableId(ObjectName tableName, SqlNumber value) {
			throw new NotImplementedException();
		}

		public SqlNumber NextTableId(ObjectName tableName) {
			throw new NotImplementedException();
		}

		public void Commit() {
			throw new NotImplementedException();
		}

		public void Rollback() {
			throw new NotImplementedException();
		}
	}
}

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	public sealed class TransactionState {
		public TransactionState(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		public TransactionRegistry Registry {
			get { return Transaction.Registry; }
		}

		public IEnumerable<int> VisibleTables { get; set; }

		public IEnumerable<int> SelectedTables { get; set; }

		public IEnumerable<int> TouchedTables { get; set; }

		internal IEnumerable<TableSource> VisibleTableSources {
			get { return VisibleTables == null ? new TableSource[0] : VisibleTables.Select(GetTableSource); }
		}

		internal IEnumerable<TableSource> SelectedTableSources {
			get { return SelectedTables == null ? new TableSource[0] : SelectedTables.Select(GetTableSource); }
		}

		internal IEnumerable<TableSource> TouchedTableSources {
			get { return TouchedTables == null ? new TableSource[0] : TouchedTables.Select(GetTableSource); }
		} 

		private TableSource GetTableSource(int tableId) {
			return Transaction.Context.Database.TableComposite.GetTableSource(tableId);
		}
	}
}

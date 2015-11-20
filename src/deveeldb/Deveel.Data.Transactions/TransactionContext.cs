using System;

using Deveel.Data.Services;

namespace Deveel.Data.Transactions {
	public class TransactionContext : Context, ITransactionContext	{
		public TransactionContext (IDatabaseContext databaseContext)
			: base(databaseContext) {
		}

		protected override string ContextName {
			get { return ContextNames.Transaction; }
		}

		public ISessionContext CreateSessionContext() {
			throw new NotImplementedException();
		}
	}
}
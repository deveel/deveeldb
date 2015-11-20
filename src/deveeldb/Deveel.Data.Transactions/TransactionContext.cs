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

	    public IDatabaseContext DatabaseContext {
	        get { return (IDatabaseContext) ParentContext; }
	    }

	    public ISessionContext CreateSessionContext() {
			return new SessionContext(this);
		}
	}
}
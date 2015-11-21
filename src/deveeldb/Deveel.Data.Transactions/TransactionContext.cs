using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Transactions {
	public class TransactionContext : Context, ITransactionContext	{
		public TransactionContext (IDatabaseContext databaseContext)
			: base(databaseContext) {
			VariableManager = new VariableManager(this);
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

		public IVariableManager VariableManager { get; private set; }

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (VariableManager != null)
					VariableManager.Dispose();
			}

			VariableManager = null;
			base.Dispose(disposing);
		}
	}
}
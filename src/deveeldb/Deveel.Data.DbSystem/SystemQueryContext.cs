using System;

using Deveel.Data.Sql.Query;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class SystemQueryContext : QueryContextBase {
		public SystemQueryContext(ITransaction transaction, string currentSchema) {
			Transaction = transaction;
			CurrentSchema = currentSchema;
		}

		public ITransaction Transaction { get; private set; }

		public string CurrentSchema { get; set; }

		public override ISystemContext SystemContext {
			get { return Transaction.Context.SystemContext; }
		}

		public override IUserSession Session {
			get { throw new NotImplementedException(); }
		}

		public override IQueryPlanContext QueryPlanContext {
			get { throw new NotImplementedException(); }
		}
	}
}

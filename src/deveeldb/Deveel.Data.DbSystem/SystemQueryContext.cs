using System;

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class SystemQueryContext : QueryContextBase {
		private IUserSession session;

		public SystemQueryContext(ITransaction transaction, string currentSchema) {
			Transaction = transaction;
			CurrentSchema = currentSchema;

			session = new UserSession(transaction.Context.Database, transaction, User.System, ConnectionEndPoint.Embedded);
		}

		public ITransaction Transaction { get; private set; }

		public string CurrentSchema { get; set; }

		public override IDatabaseContext DatabaseContext {
			get { return Transaction.Context.Database.Context; }
		}

		public override IUserSession Session {
			get { return session; }
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				session.Dispose();
			}

			session = null;
			base.Dispose(disposing);
		}
	}
}

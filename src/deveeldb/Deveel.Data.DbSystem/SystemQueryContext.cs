using System;

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class SystemQueryContext : QueryContextBase {
		private IUserSession session;
		private string currentSchema;

		public SystemQueryContext(ITransaction transaction, string currentSchema) {
			Transaction = transaction;
			this.currentSchema = currentSchema;

			session = new UserSession(transaction.Context.Database, transaction, User.System, ConnectionEndPoint.Embedded);
		}

		public ITransaction Transaction { get; private set; }

		public override string CurrentSchema {
			get { return currentSchema; }
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

using System;

using Deveel.Data.Caching;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.DbSystem {
	public class SessionQueryContext : IQueryContext {
		public SessionQueryContext(IUserSession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
		}

		public IUserSession Session { get; private set; }

		public IDatabaseContext DatabaseContext {
			get { return Session.Database.Context; }
		}

		public void Dispose() {
		}

		public ICache TableCache { get; private set; }


		public SqlNumber NextRandom(int bitSize) {
			throw new NotImplementedException();
		}
	}
}

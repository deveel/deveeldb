using System;

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.DbSystem {
	public class SessionQueryContext : IQueryContext {
		public SessionQueryContext(IUserSession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
		}

		public IUserSession Session { get; private set; }

		public void Dispose() {
		}

		public ISystemContext SystemContext {
			get {
				// TODO:
				return null;
			}
		}

		public ICache TableCache { get; private set; }


		public bool IsExceptionState { get; private set; }

		public IRoutineResolver RoutineResolver { get; private set; }

		public SqlNumber NextRandom(int bitSize) {
			throw new NotImplementedException();
		}

		public void SetExceptionState(Exception exception) {
			throw new NotImplementedException();
		}

		public Exception GetException() {
			throw new NotImplementedException();
		}

		public IDbObject GetObject(ObjectName objName) {
			throw new NotImplementedException();
		}
	}
}

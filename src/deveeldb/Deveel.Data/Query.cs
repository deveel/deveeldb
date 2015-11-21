using System;

using Deveel.Data;

namespace Deveel.Data {
	public sealed class Query : IQuery {
		internal Query(IUserSession session) {
			Session = session;

			QueryContext = session.SessionContext.CreateQueryContext();
			// TODO: put this query into the scope
		}

		~Query() {
			Dispose(false);
		}

		public IQueryContext QueryContext { get; private set; }

		public IUserSession Session { get; private set; }


		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (QueryContext != null)
					QueryContext.Dispose();
			}

			QueryContext = null;
			Session = null;
		}
	}
}

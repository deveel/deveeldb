using System;

using Deveel.Data;
using Deveel.Data.Services;

namespace Deveel.Data {
	public sealed class Query : IQuery, IBlockParent {
		internal Query(IUserSession session) {
			Session = session;

			QueryContext = session.SessionContext.CreateQueryContext();
			QueryContext.RegisterInstance<IQuery>(this);
		}

		~Query() {
			Dispose(false);
		}

		IBlockContext IBlockParent.CreateBlockContext() {
			return QueryContext.CreateBlockContext();
		}

		public IBlock CreateBlock() {
			return new Block(this);
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

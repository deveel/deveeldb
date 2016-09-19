using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Remotion.Linq;

namespace Deveel.Data.Linq {
	public class SessionQueryContext : IDisposable {
		public SessionQueryContext(ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
			Provider = new SessionQueryProvider(session);
		}

		protected ISession Session { get; private set; }

		internal IQueryProvider Provider { get; private set; }

		public IQueryTable<TType> Table<TType>() where TType : class {
			return new QueryTable<TType>(this, new LinqQueryable<TType>(Provider));
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
			}

			Session = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#region SessionQueryProvider

		class SessionQueryProvider : QueryProviderBase {
			public SessionQueryProvider(ISession session)
				: base(Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault(), new QueryExecutor(session)) {
			}

			public override IQueryable<T> CreateQuery<T>(Expression expression) {
				return new LinqQueryable<T>(this, expression);
			}
		}

		#endregion

		#region LinqQueryable

		class LinqQueryable<T> : QueryableBase<T> {
			public LinqQueryable(IQueryProvider provider, Expression expression)
				: base(provider, expression) {
			}

			public LinqQueryable(IQueryProvider provider)
				: base(provider) {
			}
		}

		#endregion
	}
}

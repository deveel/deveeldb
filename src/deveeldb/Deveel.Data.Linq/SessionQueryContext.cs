using System;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Design;

using Remotion.Linq;

namespace Deveel.Data.Linq {
	public class SessionQueryContext : IDisposable {
		public SessionQueryContext(ISession session) 
			: this(session, null) {
		}

		public SessionQueryContext(ISession session, DbCompiledModel model) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
			SetModel(model);
		}

		protected DbCompiledModel Model { get; private set; }

		protected ISession Session { get; private set; }

		private IQueryProvider Provider { get; set; }

		private void SetModel(DbCompiledModel model) {
			if (model == null) {
				var builder = new DbModelBuilder();
				OnBuildModel(builder);

				var builtModel = builder.Build();
				model = builtModel.Compile();
			}

			Model = model;
			Provider = new SessionQueryProvider(Session, model);
		}

		public QueryTable<TType> Table<TType>() where TType : class {
			return new QueryTable<TType>(this, new LinqQueryable<TType>(Provider));
		}

		protected virtual void Dispose(bool disposing) {
			Provider = null;
			Session = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void OnBuildModel(DbModelBuilder modelBuilder) {
		}

		internal DbTypeInfo FindTypeInfo(Type type) {
			return Model.GetTypeInfo(type);
		}

		#region SessionQueryProvider

		class SessionQueryProvider : QueryProviderBase {
			public SessionQueryProvider(ISession session, DbCompiledModel model)
				: base(Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault(), new QueryExecutor(session, model)) {
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

		internal IQuery CreateQuery() {
			return Session.CreateQuery();
		}
	}
}

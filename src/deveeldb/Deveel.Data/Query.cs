using System;
using System.Collections.Generic;

using Deveel.Data;
using Deveel.Data.Diagnostics;
using Deveel.Data.Services;
using Deveel.Data.Sql;

namespace Deveel.Data {
	public sealed class Query : IQuery {
		private Dictionary<string, object> metadata;

		internal Query(IUserSession session) 
			: this(session, null) {
		}

		internal Query(IUserSession session, SqlQuery sourceQuery) {
			Session = session;
			SourceQuery = sourceQuery;

			QueryContext = session.SessionContext.CreateQueryContext();
			QueryContext.RegisterInstance<IQuery>(this);

			StartedOn = DateTimeOffset.UtcNow;

			metadata = GetMetadata();
		}

		private Dictionary<string, object> GetMetadata() {
			return new Dictionary<string, object> {
				{ "query.startTime", StartedOn },
				{ "query.source", SourceQuery }
			};
		}

		~Query() {
			Dispose(false);
		}

		IBlockContext IRequest.CreateBlockContext() {
			return QueryContext.CreateBlockContext();
		}

		public IBlock CreateBlock() {
			return new Block(this);
		}

		IQuery IRequest.Query {
			get { return this; }
		}

		public IQueryContext QueryContext { get; private set; }

		public IUserSession Session { get; private set; }

		public DateTimeOffset StartedOn { get; private set; }

		public SqlQuery SourceQuery { get; private set; }

		public bool HasSourceQuery {
			get { return SourceQuery != null; }
		}

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

		IContext IEventSource.Context {
			get { return QueryContext; }
		}

		IEventSource IEventSource.ParentSource {
			get { return Session; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return metadata; }
		}
	}
}

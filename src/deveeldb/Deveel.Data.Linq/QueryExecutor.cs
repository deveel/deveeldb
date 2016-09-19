using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design;
using Deveel.Data.Sql.Statements;

using Remotion.Linq;
using Remotion.Linq.Clauses.ResultOperators;

namespace Deveel.Data.Linq {
	class QueryExecutor : IQueryExecutor {
		private ISession session;
		private CompiledModel model;

		public QueryExecutor(ISession session) {
			this.session = session;
			model = session.GetObjectModel();
		}

		private SelectStatement ToQueryExpression(IQuery context, QueryModel queryModel) {
			return SqlQueryGenerator.GenerateSelect(context, model, queryModel);
		}

		public T ExecuteScalar<T>(QueryModel queryModel) {
			return ExecuteCollection<T>(queryModel).First();
		}

		public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty) {
			return returnDefaultWhenEmpty
				? ExecuteCollection<T>(queryModel).FirstOrDefault()
				: ExecuteCollection<T>(queryModel).First();
		}


		public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel) {
			var query = session.CreateQuery();
			var expression = ToQueryExpression(query, queryModel);
			var result = query.ExecuteStatement(expression);
			if (result.Type == StatementResultType.Exception)
				throw result.Error;

			IEnumerable<T> collection;
			if (result.Type == StatementResultType.CursorRef) {
				collection = result.Cursor.Select(x => x.ToObject<T>(model));
			} else if (result.Type == StatementResultType.Result) {
				collection = result.Result.Select(x => x.ToObject<T>(model));
			} else {
				collection = new T[0];
			}

			return collection;
		}
	}
}

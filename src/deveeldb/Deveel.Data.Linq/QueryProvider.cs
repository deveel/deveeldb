using System;

using Deveel.Data.Sql.Statements;

using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class QueryProvider : IQToolkit.Data.EntityProvider {
		public QueryProvider(IRequest context, QueryMapping mapping, QueryPolicy policy) 
			: base(new DeveelDbQueryLanguage(), mapping, policy) {
			Context = context;
		}

		public IRequest Context { get; private set; }

		protected override QueryExecutor CreateExecutor() {
			return new SqlQueryExecutor(this);
		}

		public override void DoTransacted(Action action) {
			throw new NotImplementedException();
		}

		public override void DoConnected(Action action) {
			action();
		}

		public override int ExecuteCommand(string commandText) {
			var query = Context.Query;
			var results = query.ExecuteQuery(commandText);
			if (results == null)
				throw new InvalidOperationException();

			if (results.Length > 1)
				throw new NotSupportedException();

			var result = results[0];
			if (result.Type == StatementResultType.Exception)
				throw new InvalidOperationException("The execution caused an exception", result.Error);

			return result.Result.RowCount;
		}
	}
}

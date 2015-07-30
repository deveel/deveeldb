using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public abstract class SqlNonPreparableStatement : IStatement, IPreparedStatement {
		public SqlQuery SourceQuery { get; private set; }

		IPreparedStatement IStatement.Prepare(IExpressionPreparer preparer, IQueryContext context) {
			return this;
		}

		IStatement IPreparedStatement.Source {
			get { return this; }
		}

		public abstract ITable Execute(IQueryContext context);
	}
}

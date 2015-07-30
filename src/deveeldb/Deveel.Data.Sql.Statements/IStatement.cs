using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public interface IStatement {
		SqlQuery SourceQuery { get; }

		IPreparedStatement Prepare(IExpressionPreparer preparer, IQueryContext context);
	}
}

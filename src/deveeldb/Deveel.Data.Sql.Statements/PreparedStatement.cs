using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	public abstract class PreparedStatement {
		public Statement Source { get; internal set; }

		public StatementType StatementType {
			get { return Source.StatementType; }
		}

		public SqlQuery SourceQuery {
			get { return Source.SourceQuery; }
		}

		public abstract ITable Evaluate(IQueryContext context);
	}
}

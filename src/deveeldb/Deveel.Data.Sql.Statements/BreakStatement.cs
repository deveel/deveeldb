using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	public sealed class BreakStatement : SqlNonPreparableStatement {
		public BreakStatement() 
			: this(null) {
		}

		public BreakStatement(string label) {
			Label = label;
		}

		public string Label { get; private set; }

		public override ITable Execute(IQueryContext context) {
			// TODO: break the current loop or until the loop labeled
			throw new NotImplementedException();
		}
	}
}

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	public sealed class GoToStatement : SqlNonPreparableStatement {
		public GoToStatement(string label) {
			if (String.IsNullOrEmpty(label))
				throw new ArgumentNullException("label");

			Label = label;
		}

		public string Label { get; private set; }

		public override ITable Execute(IQueryContext context) {
			// TODO: find in the context the labeled statement and execute it...
			throw new NotImplementedException();
		}
	}
}

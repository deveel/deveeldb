using System;
using System.Collections.Generic;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropUserStatementNode : SqlStatementNode {
		public IEnumerable<string> UserNames { get; private set; }

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			foreach (var userName in UserNames) {
				builder.Objects.Add(new DropUserStatement(userName));
			}
		}
	}
}
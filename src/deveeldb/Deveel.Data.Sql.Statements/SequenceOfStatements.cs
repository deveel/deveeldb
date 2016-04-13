using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Statements {
	class SequenceOfStatements : SqlStatement {
		public SequenceOfStatements() {
			Statements = new List<SqlStatement>();
		}

		public ICollection<SqlStatement> Statements { get; private set; }
	}
}

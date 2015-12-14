using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Diagnostics {
	public sealed class StatementEvent : Event {
		public StatementEvent(IStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			Statement = statement;
		}

		public IStatement Statement { get; private set; }
	}
}

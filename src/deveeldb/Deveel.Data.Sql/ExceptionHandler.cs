using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public sealed class ExceptionHandler {
		public ExceptionHandler(HandledExceptions handled) {
			if (handled == null)
				throw new ArgumentNullException("handled");

			Handled = handled;
			Statements = new List<SqlStatement>();
		}

		public HandledExceptions Handled { get; private set; }

		public ICollection<SqlStatement> Statements { get; private set; }

		public bool Handles(string exceptionName) {
			return Handled.ExceptionNames.Any(x => String.Equals(x, exceptionName, StringComparison.OrdinalIgnoreCase)) || 
				Handled.IsForOthers;
		}
	}
}

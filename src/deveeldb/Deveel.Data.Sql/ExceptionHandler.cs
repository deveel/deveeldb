using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public sealed class ExceptionHandler : IPreparable {
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

		private ExceptionHandler PrepareExpressions(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}
	}
}

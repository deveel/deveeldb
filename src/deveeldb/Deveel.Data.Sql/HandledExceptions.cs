using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql {
	public sealed class HandledExceptions {
		public static readonly HandledExceptions Others = new HandledExceptions(null, true);

		private HandledExceptions(IEnumerable<string> exceptionNames, bool others) {
			if (!others) {
				if (exceptionNames == null)
					throw new ArgumentNullException("exceptionNames");

				if (exceptionNames.Any(String.IsNullOrEmpty))
					throw new ArgumentException();
			}

			ExceptionNames = exceptionNames;
			IsForOthers = others;
		}

		public HandledExceptions(IEnumerable<string> exceptionNames)
			: this(exceptionNames, false) {
		}

		public HandledExceptions(string exceptionName)
			: this(new[] {exceptionName}) {
		}

		public bool IsForOthers { get; private set; }

		public IEnumerable<string> ExceptionNames { get; private set; }
	}
}

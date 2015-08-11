using System;

namespace Deveel.Data.Sql.Statements {
	public class PreparationRequiredException : StatementException {
		public string TypeName { get; set; }

		public PreparationRequiredException(string typeName)
			: this(typeName, String.Format("The Statement '{0}' requires preparation before being executed", typeName)) {
		}

		public PreparationRequiredException(string typeName, string message)
			: base(message) {
			TypeName = typeName;
		}
	}
}

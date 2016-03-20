using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropFunctionStatement : SqlStatement {
		public DropFunctionStatement(ObjectName functionName) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");

			FunctionName = functionName;
		}

		public ObjectName FunctionName { get; private set; }

		public bool IfExists { get; set; }
	}
}

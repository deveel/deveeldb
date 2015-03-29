using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	class CreateTriggerNode : SqlNode, IStatementNode {
		public ObjectName TriggerName { get; private set; }

		public bool IfNotExists { get; private set; }

		public bool Callback { get; private set; }

		public ObjectName ProcedureName { get; private set; }

		public IExpressionNode[] ProcedureArguments { get; private set; }

		public bool IsBefore { get; private set; }

		public bool IsAfter { get; private set; }

		public IEnumerable<string> Events { get; private set; } 
	}
}

using System;

namespace Deveel.Data.Sql.Statements {
	public interface IAlterTriggerAction : ISqlFormattable {
		AlterTriggerActionType ActionType { get; }
	}
}

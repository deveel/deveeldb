using System;

namespace Deveel.Data.Sql.Statements {
	public interface IAlterUserAction {
		AlterUserActionType ActionType { get; }
	}
}

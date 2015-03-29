using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public interface IAlterTableAction : IPreparable {
		AlterTableActionType ActionType { get; }
	}
}

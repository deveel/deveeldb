using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropPrimaryKeyAction : IAlterTableAction {
		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return new DropPrimaryKeyAction();
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return AlterTableActionType.DropPrimaryKey; }
		}
	}
}

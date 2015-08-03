using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetUserGroupsAction : IAlterUserAction {
		public SetUserGroupsAction(IEnumerable<SqlExpression> groups) {
			if (groups == null)
				throw new ArgumentNullException("groups");

			Groups = groups;
		}

		public IEnumerable<SqlExpression> Groups { get; private set; }

		public AlterUserActionType ActionType {
			get { return AlterUserActionType.SetGroups; }
		}
	}
}

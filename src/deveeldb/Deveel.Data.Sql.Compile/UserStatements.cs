using System;
using System.Collections.Generic;

using Antlr4.Runtime.Misc;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class UserStatements {
		public static SqlStatement Create(PlSqlParser.CreateUserStatementContext context) {
			var userName = context.userName().GetText();
			string password = null;
			if (context.byPassword() != null) {
				password = InputString.AsNotQuoted(context.byPassword().CHAR_STRING().GetText());
			} else if (context.externalId() != null) {
				throw new NotImplementedException();
			} else if (context.globalId() != null) {
				throw new NotImplementedException();
			} else {
				throw new ParseCanceledException();
			}

			return new CreateUserStatement(userName, SqlExpression.Constant(password));
		}

		private static SetPasswordAction SetPassword(PlSqlParser.AlterUserIdActionContext context) {
			string password = null;
			if (context.byPassword() != null) {
				password = InputString.AsNotQuoted(context.byPassword().CHAR_STRING().GetText());
			} else if (context.externalId() != null) {
				throw new NotImplementedException();
			} else if (context.globalId() != null) {
				throw new NotImplementedException();
			} else {
				throw new ParseCanceledException();
			}

			return new SetPasswordAction(SqlExpression.Constant(password));
		}

		public static SqlStatement Alter(PlSqlParser.AlterUserStatementContext context) {
			var userName = context.userName().GetText();

			var actions = new List<IAlterUserAction>();

			foreach (var actionContext in context.alterUserAction()) {
				if (actionContext.alterUserIdAction() != null) {
					actions.Add(SetPassword(actionContext.alterUserIdAction()));
				} else if (actionContext.setAccountAction() != null) {
					actions.Add(SetAccount(actionContext.setAccountAction()));
				} else if (actionContext.setRoleAction() != null) {
					actions.Add(SetRole(actionContext.setRoleAction()));
				}
			}

			if (actions.Count == 1)
				return new AlterUserStatement(userName, actions[0]);

			var seq = new SequenceOfStatements();
			foreach (var action in actions) {
				seq.Statements.Add(new AlterUserStatement(userName, action));
			}

			return seq;
		}

		private static IAlterUserAction SetRole(PlSqlParser.SetRoleActionContext context) {
			var roleName = context.regular_id().GetText();

			return new SetUserRolesAction(new[] {SqlExpression.Constant(roleName)});
		}

		private static IAlterUserAction SetAccount(PlSqlParser.SetAccountActionContext context) {
			UserStatus status;
			if (context.LOCK() != null) {
				status = UserStatus.Locked;
			} else if (context.UNLOCK() != null) {
				status = UserStatus.Unlocked;
			} else {
				throw new ParseCanceledException();
			}

			return new SetAccountStatusAction(status);
		}
	}
}

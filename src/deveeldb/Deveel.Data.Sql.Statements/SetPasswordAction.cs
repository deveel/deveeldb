using System;
using System.IO;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetPasswordAction : IAlterUserAction, IPreparable {
		public SetPasswordAction(SqlExpression passwordExpression) {
			if (passwordExpression == null)
				throw new ArgumentNullException("passwordExpression");

			PasswordExpression = passwordExpression;
		}

		public AlterUserActionType ActionType {
			get { return AlterUserActionType.SetPassword; }
		}

		public SqlExpression PasswordExpression { get; private set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var preparedExp = PasswordExpression.Prepare(preparer);
			return new SetPasswordAction(preparedExp);
		}

		public static void Serialize(SetPasswordAction action, BinaryWriter writer) {
			SqlExpression.Serialize(action.PasswordExpression, writer);
		}

		public static SetPasswordAction Deserialize(BinaryReader reader) {
			var exp = SqlExpression.Deserialize(reader);
			return new SetPasswordAction(exp);
		}
	}
}

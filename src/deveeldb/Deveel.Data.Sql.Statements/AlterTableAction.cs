using System;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public abstract class AlterTableAction : IAlterTableAction, IPreparable, IStatementPreparable {
		protected AlterTableAction() {
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return ActionType; }
		}

		protected abstract AlterTableActionType ActionType { get; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			GetObjectData(info, context);
		}

		protected virtual void GetObjectData(SerializationInfo info, StreamingContext context) {
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {
		}
		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		protected virtual AlterTableAction PrepareExpressions(IExpressionPreparer preparer) {
			return this;
		}

		object IStatementPreparable.Prepare(IRequest context) {
			return PrepareStatement(context);
		}

		protected virtual AlterTableAction PrepareStatement(IRequest context) {
			return this;
		}
	}
}

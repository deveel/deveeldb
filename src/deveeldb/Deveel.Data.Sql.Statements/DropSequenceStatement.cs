using System;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropSequenceStatement : SqlStatement {
		public DropSequenceStatement(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
		}

		public ObjectName SequenceName { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var seqName = context.Access.ResolveObjectName(DbObjectType.Sequence, SequenceName);
			return new DropSequenceStatement(seqName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanDrop(DbObjectType.Sequence, SequenceName))
				throw new MissingPrivilegesException(context.User.Name, SequenceName, Privileges.Drop);

			if (!context.DirectAccess.DropObject(DbObjectType.Sequence, SequenceName))
				throw new StatementException(String.Format("Cannot drop sequence '{0}': maybe not found.", SequenceName));

			context.DirectAccess.RevokeAllGrantsOn(DbObjectType.Sequence, SequenceName);
		}
	}
}

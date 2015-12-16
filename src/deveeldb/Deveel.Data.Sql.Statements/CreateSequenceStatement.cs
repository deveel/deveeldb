using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateSequenceStatement : SqlStatement, IPreparableStatement, IPreparable {
		public CreateSequenceStatement(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
		}

		public ObjectName SequenceName { get; private set; }

		public SqlExpression StartWith { get; set; }

		public SqlExpression IncrementBy { get; set; }

		public SqlExpression MinValue { get; set; }

		public SqlExpression MaxValue { get; set; }

		public SqlExpression Cache { get; set; }

		public bool Cycle { get; set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}
	}
}

using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropSequenceStatement : SqlStatement {
		public DropSequenceStatement(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
		}

		public ObjectName SequenceName { get; private set; }
	}
}

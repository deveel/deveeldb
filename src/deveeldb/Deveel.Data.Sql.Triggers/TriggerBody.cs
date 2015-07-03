using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerBody {
		private readonly List<SqlStatement> statements;

		internal TriggerBody(TriggerInfo triggerInfo) {
			if (triggerInfo == null)
				throw new ArgumentNullException("triggerInfo");

			TriggerInfo = triggerInfo;

			statements = new List<SqlStatement>();
		}

		public TriggerInfo TriggerInfo { get; private set; }

		private void AssertStatementIsAllowed(SqlStatement statement) {
			// TODO: validate this statement
		}


		public void AddStatement(SqlStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			if (TriggerInfo.TriggerType != TriggerType.Procedure)
				throw new ArgumentException(String.Format("The trigger '{0}' is not a PROCEDURE TRIGGER and cannot have any body.",
					TriggerInfo.TriggerName));

			AssertStatementIsAllowed(statement);

			statements.Add(statement);
		}
	}
}

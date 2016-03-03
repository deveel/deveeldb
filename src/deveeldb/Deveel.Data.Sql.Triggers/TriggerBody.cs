// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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

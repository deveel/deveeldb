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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public sealed class ExceptionHandler : IPreparable {
		public ExceptionHandler(HandledExceptions handled) {
			if (handled == null)
				throw new ArgumentNullException("handled");

			Handled = handled;
			Statements = new List<SqlStatement>();
		}

		public HandledExceptions Handled { get; private set; }

		public ICollection<SqlStatement> Statements { get; private set; }

		public bool Handles(string exceptionName) {
			return Handled.ExceptionNames.Any(x => String.Equals(x, exceptionName, StringComparison.OrdinalIgnoreCase)) || 
				Handled.IsForOthers;
		}

		public void Handle(ExecutionContext context) {
			throw new NotImplementedException();
		}

		private ExceptionHandler PrepareExpressions(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}
	}
}

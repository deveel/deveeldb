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

namespace Deveel.Data.Sql.Statements {
	public class PlSqlBlock : IPreparable, IDisposable {
		private ICollection<SqlStatement> statements;
		private ICollection<ExceptionHandler> exceptionHandlers; 
		 
		public PlSqlBlock() {
			statements = new List<SqlStatement>();
			exceptionHandlers = new List<ExceptionHandler>();
		}

		~PlSqlBlock() {
			Dispose(false);
		}

		public string Label { get; set; }

		public IEnumerable<SqlStatement> Statements {
			get { return statements.AsEnumerable(); }
		}

		public IEnumerable<ExceptionHandler> ExceptionHandlers {
			get { return exceptionHandlers.AsEnumerable(); }
		}

		public void AddStatement(SqlStatement statement) {
			// TODO: make further checks, such as if a labeled statement with
			//       the same label already exists
			statements.Add(statement);
		}

		public void AddExceptionHandler(ExceptionHandler handler) {
			// TODO: make further checks here ...
			exceptionHandlers.Add(handler);
		}

		/*
		protected virtual BlockExecuteContext CreateExecuteContext() {
			throw new NotImplementedException();
		}
		*/

		protected virtual PlSqlBlock Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return Prepare(preparer);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (statements != null)
					statements.Clear();
				if (exceptionHandlers != null)
					exceptionHandlers.Clear();
			}

			statements = null;
			exceptionHandlers = null;
		}
	}
}

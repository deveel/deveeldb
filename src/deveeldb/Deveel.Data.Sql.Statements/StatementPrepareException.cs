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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// An exception that happens during the <see cref="SqlStatement.Prepare(IExpressionPreparer, IQueryContext)"/>.
	/// </summary>
	public class StatementPrepareException : SqlErrorException {
		public StatementPrepareException() 
			: this(SqlModelErrorCodes.StatementPrepare) {
		}

		public StatementPrepareException(int errorCode) 
			: base(errorCode) {
		}

		public StatementPrepareException(string message) 
			: this(SqlModelErrorCodes.StatementPrepare, message) {
		}

		public StatementPrepareException(int errorCode, string message) 
			: base(errorCode, message) {
		}

		public StatementPrepareException(string message, Exception innerException) 
			: this(SqlModelErrorCodes.StatementPrepare, message, innerException) {
		}

		public StatementPrepareException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}

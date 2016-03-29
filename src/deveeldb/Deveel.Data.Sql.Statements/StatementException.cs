// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Statements {
	public class StatementException : SqlErrorException {
		public StatementException() 
			: this(SqlModelErrorCodes.StatementExecute) {
		}

		public StatementException(int errorCode) 
			: base(errorCode) {
		}

		public StatementException(string message) 
			: this(SqlModelErrorCodes.StatementExecute, message) {
		}

		public StatementException(int errorCode, string message) 
			: base(errorCode, message) {
		}

		public StatementException(string message, Exception innerException) 
			: this(SqlModelErrorCodes.StatementExecute, message, innerException) {
		}

		public StatementException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}

// 
//  Copyright 2010  Deveel
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

using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Control {
	public sealed class StatementEventArgs : EventArgs {
		internal StatementEventArgs(Statement statement) {
			this.statement = statement;
		}

		internal StatementEventArgs(Statement statement, object result)
			: this(statement) {
			this.result = result;
		}

		internal StatementEventArgs(Statement statement, Exception error)
			: this(statement) {
			this.error = error;
		}

		private readonly Statement statement;
		private readonly Exception error;
		private readonly object result;

		public object Result {
			get { return result; }
		}

		public Exception Error {
			get { return error; }
		}

		public Statement Statement {
			get { return statement; }
		}
	}
}
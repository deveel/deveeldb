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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	public delegate void StatementEventHandler(object sender, StatementEventArgs args);

	public sealed class StatementEventArgs : EventArgs {
		internal StatementEventArgs(Statement statement) {
			this.statement = statement;
			start = DateTime.Now;
		}

		private readonly DateTime start;
		private DateTime end;
		private Table result;
		private readonly Statement statement;
		private StatementException error;

		public Statement Statement {
			get { return statement; }
		}

		public bool HasError {
			get { return error != null; }
		}

		public StatementException Error {
			get { return error; }
		}

		public DateTime ExecutionStart {
			get { return start; }
		}

		public DateTime ExecutionEnd {
			get { return end; }
		}

		public TimeSpan ExecutionTime {
			get { return end.Subtract(start); }
		}

		public Table Result {
			get { return result; }
		}

		internal void SetResult(Table value) {
			result = value;
			end = DateTime.Now;
		}

		internal void SetError(StatementException value) {
			error = value;
			end = DateTime.Now;
		}
	}
}
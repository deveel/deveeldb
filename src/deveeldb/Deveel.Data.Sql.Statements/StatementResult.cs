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

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class StatementResult : IDisposable {
		~StatementResult() {
			Dispose(false);
		}

		public StatementResult(ITable result) 
			: this(result, new ConstraintInfo[0]) {
		}

		public StatementResult(ITable result, ConstraintInfo[] constraints) {
			if (result == null)
				throw new ArgumentNullException("result");

			Result = result;
			Constraints = constraints;
			Type = StatementResultType.Result;
		}

		public StatementResult(ICursor cursor) 
			: this(cursor, new ConstraintInfo[0]) {
		}

		public StatementResult(ICursor cursor, ConstraintInfo[] constraints) {
			if (cursor == null)
				throw new ArgumentNullException("cursor");

			Cursor = cursor;
			Constraints = constraints;
			Type = StatementResultType.CursorRef;
		}

		public StatementResult(Exception error) {
			if (error == null)
				throw new ArgumentNullException("error");

			Type = StatementResultType.Exception;
			Error = error;
		}

		public StatementResult() {
			Type = StatementResultType.Empty;
		}

		public ICursor Cursor { get; private set; }

		public ITable Result { get; private set; }

		public ConstraintInfo[] Constraints { get; set; }

		public StatementResultType Type { get; private set; }

		public Exception Error { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Result != null)
					Result.Dispose();

				if (Cursor != null)
					Cursor.Dispose();
			}

			Cursor = null;
			Result = null;
		}
	}
}

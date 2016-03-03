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

using Deveel.Data.Security;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql {
	public sealed class ExecutionContext : IDisposable {
		public ExecutionContext(IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			Request = request;
		}

		public IRequest Request { get; private set; }

		public ITable Result { get; private set; }

		public bool HasResult { get; private set; }

		public bool HasTermination { get; private set; }

		public User User {
			get { return Request.Query.User(); }
		}

		public IQuery Query {
			get { return Request.Query; }
		}

		public void SetResult(ITable result) {
			if (result != null) {
				Result = result;
				HasResult = true;
			}
		}

		public void SetResult(int value) {
			SetResult(FunctionTable.ResultTable(Request, value));
		}

		public void Terminate() {
			HasTermination = true;
		}

		public void Dispose() {
			Request = null;
		}
	}
}

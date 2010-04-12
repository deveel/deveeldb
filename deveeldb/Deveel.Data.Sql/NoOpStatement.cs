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

namespace Deveel.Data.Sql {
	/// <summary>
	/// A no operation statement.
	/// </summary>
	public class NoOpStatement : Statement {

		// ---------- Implemented from Statement ----------

		protected override void Prepare() {
			// Nothing to prepare
		}

		protected override Table Evaluate() {
			// No-op returns a result value of '0'
			return FunctionTable.ResultTable(new DatabaseQueryContext(Connection), 0);
		}

	}
}
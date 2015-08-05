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

namespace Deveel.Data.Sql.Statements {
	public sealed class BreakStatement : SqlNonPreparableStatement {
		public BreakStatement() 
			: this(null) {
		}

		public BreakStatement(string label) {
			Label = label;
		}

		public string Label { get; private set; }

		public override ITable Execute(IQueryContext context) {
			// TODO: break the current loop or until the loop labeled
			throw new NotImplementedException();
		}
	}
}

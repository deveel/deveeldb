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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data {
	public sealed class BlockExecuteContext {
		public BlockExecuteContext(IQuery query) 
			: this(query, null) {
		}

		public BlockExecuteContext(IQuery query, IVariableResolver resolver) {
			Query = query;
			VariableResolver = resolver;
		}

		public IQuery Query { get; private set; }

		public IVariableResolver VariableResolver { get; private set; }

		public ITable Result { get; private set; }

		public bool HasResult { get; private set; }

		public void SetResult(ITable table) {
			Result = table;
			HasResult = true;
		}
	}
}

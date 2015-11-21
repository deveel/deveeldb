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

using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Index {
	public static class SystemContextExtensions {
		public static IIndexFactory ResolveIndexFactory(this ISystemContext context, string indexType) {
			return context.ResolveService<IIndexFactory>(indexType);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType,
			ColumnIndexContext indexContext) {
			var factory = context.ResolveIndexFactory(indexType);
			if (factory == null)
				throw new NotSupportedException(String.Format("None index factory for type '{0}' was configured in the system.", indexType));

			return factory.CreateIndex(indexContext);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType, ITable table,
			int columnOffset) {
			return CreateColumnIndex(context, indexType, table, columnOffset, null);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType, ITable table,
			int columnOffset, IEnumerable<KeyValuePair<string, object>> metadata) {
			return context.CreateColumnIndex(indexType, new ColumnIndexContext(table, columnOffset, metadata));
		}
	}
}

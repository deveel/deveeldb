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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql {
	public sealed class View : IDbObject {
		public View(ViewInfo viewInfo) {
			if (viewInfo == null)
				throw new ArgumentNullException("viewInfo");

			ViewInfo = viewInfo;
		}

		public ViewInfo ViewInfo { get; private set; }

		public IQueryPlanNode QueryPlan {
			get { return ViewInfo.QueryPlan; }
		}

		public SqlQueryExpression QueryExpression {
			get { return ViewInfo.QueryExpression; }
		}

		ObjectName IDbObject.FullName {
			get { return ViewInfo.ViewName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.View; }
		}
	}
}

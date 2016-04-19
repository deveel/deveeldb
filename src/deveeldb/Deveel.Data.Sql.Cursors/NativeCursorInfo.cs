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

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Cursors {
	class NativeCursorInfo : IObjectInfo {
		public NativeCursorInfo(IQueryPlanNode queryPlan) 
			: this(queryPlan, false) {
		}

		public NativeCursorInfo(IQueryPlanNode queryPlan, bool forUpdate) {
			QueryPlan = queryPlan;
			ForUpdate = forUpdate;
		}

		public IQueryPlanNode QueryPlan { get; private set; }

		public bool ForUpdate { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Cursor; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(NativeCursor.NativeCursorName); }
		}

		string IObjectInfo.Owner {
			get { return null; }
		}
	}
}

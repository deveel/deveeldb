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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class LimitNode : SingleQueryPlanNode {
		public long Offset { get; private set; }

		public long Count { get; private set; }

		public LimitNode(IQueryPlanNode child, long offset, long count) 
			: base(child) {
			Offset = offset;
			Count = count;
		}

		private LimitNode(ObjectData data)
			: base(data) {
			Offset = data.GetInt64("Offset");
			Count = data.GetInt64("Count");
		}

		public override ITable Evaluate(IRequest context) {
			var table = Child.Evaluate(context);
			return new LimitedTable(table, Offset, Count);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("Offset", Offset);
			data.SetValue("Count", Count);
		}
	}
}

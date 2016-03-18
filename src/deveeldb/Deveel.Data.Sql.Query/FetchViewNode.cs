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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Views;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class FetchViewNode : IQueryPlanNode {
		public FetchViewNode(ObjectName viewName, ObjectName aliasName) {
			ViewName = viewName;
			AliasName = aliasName;
		}

		private FetchViewNode(SerializationInfo info, StreamingContext context) {
			ViewName = (ObjectName)info.GetValue("ViewName", typeof(ObjectName));
			AliasName = (ObjectName) info.GetValue("AliasName", typeof(ObjectName));
		}

		public ObjectName ViewName { get; private set; }

		public ObjectName AliasName { get; private set; }

		private IQueryPlanNode CreateChildNode(IRequest context) {
			return context.Access.GetViewQueryPlan(ViewName);
		}

		public ITable Evaluate(IRequest context) {
			IQueryPlanNode node = CreateChildNode(context);
			var t = node.Evaluate(context);

			return AliasName != null ? new ReferenceTable(t, AliasName) : t;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("ViewName", ViewName); 
			info.AddValue("AliasName", AliasName);
		}
	}
}

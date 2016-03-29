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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public sealed class SqlQueryObject : ISqlObject, ISerializable {
		public static readonly SqlQueryObject Null = new SqlQueryObject((IQueryPlanNode) null);

		public SqlQueryObject(IQueryPlanNode queryPlan) {
			QueryPlan = queryPlan;
		}

		private SqlQueryObject(SerializationInfo info, StreamingContext context) {
			QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof(IQueryPlanNode));
		}

		public IQueryPlanNode QueryPlan { get; private set; }

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException("SQL queries cannot be compared.");
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException("SQL queries cannot be compared.");
		}

		public bool IsNull {
			get { return QueryPlan == null; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("QueryPlan", QueryPlan);
		}
	}
}
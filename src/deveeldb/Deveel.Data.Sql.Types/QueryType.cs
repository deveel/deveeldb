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
using System.IO;
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Types {
	[Serializable]
	public sealed class QueryType : SqlType {
		public QueryType()
			: base("QUERY", SqlTypeCode.QueryPlan) {
		}

		private QueryType(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		public override bool IsIndexable {
			get { return false; }
		}

		public override bool IsStorable {
			get { return false; }
		}
	}
}
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

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	class QueryReferenceExpression : SqlExpression {
		public QueryReferenceExpression(QueryReference reference) {
			QueryReference = reference;
		}

		private QueryReferenceExpression(SerializationInfo info, StreamingContext context) {
			QueryReference = (QueryReference) info.GetValue("Reference", typeof(QueryReference));
		}

		public QueryReference QueryReference { get; private set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Reference; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Reference", QueryReference, typeof(QueryReference));
		}
	}
}
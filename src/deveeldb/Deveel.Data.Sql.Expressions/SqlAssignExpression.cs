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

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlAssignExpression : SqlExpression {
		internal SqlAssignExpression(SqlExpression reference, SqlExpression valueExpression) {
			if (reference == null)
				throw new ArgumentNullException("reference");
			if (valueExpression == null)
				throw new ArgumentNullException("valueExpression");

			ValueExpression = valueExpression;
			ReferenceExpression = reference;
		}

		private SqlAssignExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		public SqlExpression ReferenceExpression { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Assign; }
		}

		public override bool CanEvaluate {
			get { return true; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Reference", ReferenceExpression, typeof(SqlExpression));
			info.AddValue("Value", ValueExpression, typeof(SqlExpression));
		}
	}
}
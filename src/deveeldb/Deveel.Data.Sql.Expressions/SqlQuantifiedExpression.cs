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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlQuantifiedExpression : SqlExpression {
		private readonly SqlExpressionType type;

		internal SqlQuantifiedExpression(SqlExpressionType type, SqlExpression value) {
			if (type != SqlExpressionType.All &&
				type != SqlExpressionType.Any)
				throw new ArgumentException("Invalid quantified type");

			if (value == null)
				throw new ArgumentNullException("value");

			this.type = type;
			ValueExpression = value;
		}

		private SqlQuantifiedExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ValueExpression = (SqlExpression) info.GetValue("Value", typeof (SqlExpression));
			type = (SqlExpressionType) info.GetInt32("Type");
		}

		public override SqlExpressionType ExpressionType {
			get { return type; }
		}

		public SqlExpression ValueExpression { get; private set; }

		public bool IsArrayValue {
			get {
				return ValueExpression.ExpressionType == SqlExpressionType.Constant &&
				       ((SqlConstantExpression) ValueExpression).Value.Type is ArrayType;
			}
		}

		public bool IsTupleValue {
			get { return ValueExpression.ExpressionType == SqlExpressionType.Tuple; }
		}

		public bool IsSubQueryValue {
			get { return ValueExpression.ExpressionType == SqlExpressionType.Query; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Type", type);
			info.AddValue("Value", ValueExpression);
			base.GetData(info, context);
		}
	}
}

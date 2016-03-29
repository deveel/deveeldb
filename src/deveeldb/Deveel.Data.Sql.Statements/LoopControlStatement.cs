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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class LoopControlStatement : SqlStatement, IPlSqlStatement {
		public LoopControlStatement(LoopControlType controlType) 
			: this(controlType, (SqlExpression) null) {
		}

		public LoopControlStatement(LoopControlType controlType, SqlExpression whenExpression) 
			: this(controlType, null, whenExpression) {
		}

		public LoopControlStatement(LoopControlType controlType, string label) 
			: this(controlType, label, null) {
		}

		public LoopControlStatement(LoopControlType controlType, string label, SqlExpression whenExpression) {
			Label = label;
			WhenExpression = whenExpression;
			ControlType = controlType;
		}

		protected LoopControlStatement(SerializationInfo info, StreamingContext context) {
			Label = info.GetString("Label");
			ControlType = (LoopControlType)info.GetInt32("ControlType");
			WhenExpression = (SqlExpression) info.GetValue("When", typeof (SqlExpression));
		}

		public LoopControlType ControlType { get; private set; }

		public string Label { get; set; }

		public SqlExpression WhenExpression { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var label = Label;
			var whenExp = WhenExpression;
			if (whenExp != null)
				whenExp = whenExp.Prepare(preparer);

			return new LoopControlStatement(ControlType, label, whenExp);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (WhenExpression != null) {
				var eval = WhenExpression.EvaluateToConstant(context.Request, null);
				if (!eval.AsBoolean())
					return;
			}

			context.Control(ControlType, Label);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Label", Label);
			info.AddValue("When", WhenExpression);
			info.AddValue("ControlType", (int)ControlType);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			var type = ControlType.ToString().ToUpperInvariant();
			builder.Append(type);

			if (!String.IsNullOrEmpty(Label))
				builder.Append(" '{0}'", Label);

			if (WhenExpression != null) {
				builder.Append(" WHEN ");
				builder.Append(WhenExpression.ToString());
			}
		}
	}
}

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
using System.IO;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class LoopControlStatement : SqlStatement, IPreparable {
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

		private LoopControlStatement(ObjectData data) {
			Label = data.GetString("ControlType");
			ControlType = (LoopControlType) data.GetInt32("ControlType");
			WhenExpression = data.GetValue<SqlExpression>("WhenExpression");
		}

		public LoopControlType ControlType { get; private set; }

		public string Label { get; set; }

		public SqlExpression WhenExpression { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var label = Label;
			var whenExp = WhenExpression;
			if (whenExp != null)
				whenExp = whenExp.Prepare(preparer);

			return new LoopControlStatement(ControlType, label, whenExp);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			throw new NotImplementedException();
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("Label", Label);
			data.SetValue("WhenExpression", WhenExpression);
			data.SetValue("ControlType", (int)ControlType);
		}
	}
}

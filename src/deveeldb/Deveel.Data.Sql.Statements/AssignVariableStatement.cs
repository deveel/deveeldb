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
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AssignVariableStatement : SqlStatement {
		public AssignVariableStatement(SqlExpression varRef, SqlExpression valueExpression) {
			if (varRef == null)
				throw new ArgumentNullException("varRef");
			if (valueExpression == null)
				throw new ArgumentNullException("valueExpression");

			if (!(varRef is SqlReferenceExpression) &&
				!(varRef is SqlVariableReferenceExpression))
				throw new ArgumentException("Reference expression not supported.");

			VariableReference = varRef;
			ValueExpression = valueExpression;
		}

		private AssignVariableStatement(ObjectData data)
			: base(data) {
			VariableReference = data.GetValue<SqlExpression>("Variable");
			ValueExpression = data.GetValue<SqlExpression>("Value");
		}

		public SqlExpression VariableReference { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Variable", VariableReference);
			data.SetValue("Value", ValueExpression);
		}
	}
}

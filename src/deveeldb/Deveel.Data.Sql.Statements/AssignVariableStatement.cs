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
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AssignVariableStatement : SqlStatement, IPlSqlStatement {
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

		private AssignVariableStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			VariableReference = (SqlExpression)info.GetValue("Variable", typeof(SqlExpression));
			ValueExpression = (SqlExpression)info.GetValue("Value", typeof(SqlExpression));
		}

		public SqlExpression VariableReference { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Variable", VariableReference);
			info.AddValue("Value", ValueExpression);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var varRef = VariableReference.Prepare(preparer);
			var value = ValueExpression.Prepare(preparer);
			return new AssignVariableStatement(varRef, value);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			string varName;
			if (VariableReference is SqlVariableReferenceExpression) {
				varName = ((SqlVariableReferenceExpression) VariableReference).VariableName;
			} else if (VariableReference is SqlReferenceExpression) {
				varName = ((SqlReferenceExpression) VariableReference).ReferenceName.Name;
			} else {
				throw new StatementException("The type of variable reference is invalid.");
			}

			var variable = context.Request.Context.FindVariable(varName);

			if (variable == null) {
				var varType = ValueExpression.ReturnType(context.Request, null);
				variable = context.Request.Context.DeclareVariable(varName, varType);
			}

			variable.SetValue(ValueExpression);

			context.SetResult(variable.GetValue(context.Request));
		}
	}
}

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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeclareVariableStatement : SqlStatement, IPreparable, IDeclarationStatement {
		public DeclareVariableStatement(string variableName, SqlType variableType) {
			if (String.IsNullOrEmpty(variableName))
				throw new ArgumentNullException("variableName");
			if (variableType == null)
				throw new ArgumentNullException("variableType");

			VariableName = variableName;
			VariableType = variableType;
		}

		public string VariableName { get; private set; }

		public SqlType VariableType { get; private set; }

		public bool IsConstant { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool IsNotNull { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var statement = new DeclareVariableStatement(VariableName, VariableType);
			if (DefaultExpression != null)
				statement.DefaultExpression = DefaultExpression.Prepare(preparer);

			statement.IsConstant = IsConstant;
			return statement;
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			throw new NotImplementedException();
		}
	}
}

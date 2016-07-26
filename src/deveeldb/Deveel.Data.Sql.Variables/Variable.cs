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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class Variable : IDbObject, IEquatable<Variable> {
		public Variable(VariableInfo variableInfo) 
			: this(variableInfo, (SqlExpression) null) {
		}

		public Variable(VariableInfo variableInfo, Field value)
			: this(variableInfo, SqlExpression.Constant(value)) {
		}

		public Variable(VariableInfo variableInfo, SqlExpression value) {
			if (variableInfo == null)
				throw new ArgumentNullException("variableInfo");

			Expression = value;
			VariableInfo = variableInfo;
		}

		public VariableInfo VariableInfo { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return VariableInfo; }
		}

		public string Name {
			get { return VariableInfo.VariableName; }
		}

		public SqlType Type {
			get { return VariableInfo.Type; }
		}

		public SqlExpression Expression { get; private set; }


		public bool IsConstant {
			get { return VariableInfo.IsConstant; }
		}

		public bool IsNotNull {
			get { return VariableInfo.IsNotNull; }
		}

		public bool Equals(Variable other) {
			if (other == null)
				return false;

			if (!Name.Equals(other.Name) ||
				!Type.Equals(other.Type))
				return false;

			if (Expression == null &&
			    other.Expression == null)
				return true;
			if (Expression == null)
				return false;

			return Expression.Equals(other.Expression);
		}

		public void SetValue(SqlExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (IsConstant)
				throw new ConstantVariableViolationException(Name);

			Expression = expression;
		}

		public void SetValue(Field value) {
			if (!IsNotNull &&
			    (value.IsNull || Field.IsNullField(value)))
				throw new NotNullVariableViolationException(Name);

			if (!Type.Equals(value.Type)) {
				if (!value.Type.CanCastTo(Type))
					throw new ArgumentException(String.Format("Trying to assign a value of type '{0}' to a variable of type '{1}'.", value.Type, Type));

				value = value.CastTo(Type);
			}

			SetValue(SqlExpression.Constant(value));
		}

		public Field Evaluate(IRequest context) {
			var toEval = Expression;
			if (toEval == null)
				toEval = VariableInfo.DefaultExpression;

			if (toEval == null)
				toEval = SqlExpression.Constant(null);

			return toEval.EvaluateToConstant(context, null);
		}
	}
}

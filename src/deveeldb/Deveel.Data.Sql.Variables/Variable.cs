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

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class Variable : IDbObject, IEquatable<Variable> {
		public Variable(VariableInfo variableInfo) {
			if (variableInfo == null)
				throw new ArgumentNullException("variableInfo");

			VariableInfo = variableInfo;
		}

		public VariableInfo VariableInfo { get; private set; }

		public string Name {
			get { return VariableInfo.VariableName; }
		}

		public SqlType Type {
			get { return VariableInfo.Type; }
		}

		public SqlExpression Expression { get; private set; }

		public bool ValueFromExpression {
			get { return Expression != null; }
		}

		public Field Value { get; private set; }

		public bool IsConstant {
			get { return VariableInfo.IsConstant; }
		}

		public bool IsNotNull {
			get { return VariableInfo.IsNotNull; }
		}

		ObjectName IDbObject.FullName {
			get { return new ObjectName(VariableInfo.VariableName); }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Variable; }
		}

		public bool Equals(Variable other) {
			if (other == null)
				return false;

			if (!Name.Equals(other.Name) ||
				!Type.Equals(other.Type))
				return false;

			if (ValueFromExpression &&
			    other.ValueFromExpression) {
				// TODO: Determine if the two expressions are equal!
				return true;
			}

			return Value.Equals(other.Value);
		}

		public void SetValue(IRequest context, SqlExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (IsConstant)
				throw new InvalidOperationException(String.Format("The variable '{0}' is constant and cannot be assigned.", Name));

			if (!expression.IsConstant())
				throw new ArgumentException("The value is not constant.");

			Expression = expression;

			var exp = expression.Evaluate(context, null);
			if (exp.ExpressionType != SqlExpressionType.Constant)
				throw new InvalidOperationException("The evaluation of the assignment value is not constant.");

			var value = ((SqlConstantExpression) exp).Value;
			SetValue(value);
		}

		public void SetValue(Field value) {
			if (IsConstant)
				throw new InvalidOperationException();

			if (!IsNotNull && value.IsNull)
				throw new ArgumentException();

			if (!Type.Equals(value.Type)) {
				if (!value.Type.CanCastTo(Type))
					throw new ArgumentException();

				value = value.CastTo(Type);
			}

			Value = value;
		}
	}
}

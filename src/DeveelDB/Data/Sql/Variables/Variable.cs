// 
//  Copyright 2010-2018 Deveel
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
using System.Threading.Tasks;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class Variable : IDbObject, ISqlFormattable {
		private static readonly char[] InvalidChars = " $.|\\:/#'".ToCharArray();

		public Variable(VariableInfo variableInfo) {
			if (variableInfo == null)
				throw new ArgumentNullException(nameof(variableInfo));

			VariableInfo = variableInfo;
		}

		public Variable(string name, SqlType type, bool constant, SqlExpression defaultValue)
			: this(new VariableInfo(name, type, constant, defaultValue)) {
		}

		public Variable(string name, SqlType type)
			: this(name, type, null) {
		}

		public Variable(string name, SqlType type, SqlExpression defaultValue)
			: this(name, type, false, defaultValue) {
		}

		public VariableInfo VariableInfo { get; }

		public string Name => VariableInfo.Name;

		public bool Constant => VariableInfo.Constant;

		public SqlType Type => VariableInfo.Type;

		public SqlExpression Value { get; private set; }

		IDbObjectInfo IDbObject.ObjectInfo => VariableInfo;

		public SqlExpression SetValue(SqlExpression value, QueryContext context) {
			if (value == null)
				throw new ArgumentNullException(nameof(value));

			if (VariableInfo.Constant)
				throw new VariableException($"Cannot set constant variable {VariableInfo.Name}");

			var valueType = value.GetSqlType(context);
			if (!valueType.Equals(VariableInfo.Type) &&
				!valueType.IsComparable(VariableInfo.Type))
				throw new ArgumentException($"The type {valueType} of the value is not compatible with the variable type '{VariableInfo.Type}'");

			Value = value;
			return Value;
		}

		public Task<SqlExpression> Evaluate(QueryContext context) {
			var expression = Value;
			if (expression == null)
				expression = VariableInfo.DefaultValue;

			if (expression == null)
				throw new VariableException($"Variable {VariableInfo.Name} has no value set");

			return expression.ReduceAsync(context);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat(":{0}", VariableInfo.Name);
			builder.Append(" ");
			if (VariableInfo.Constant)
				builder.Append("CONSTANT ");

			VariableInfo.Type.AppendTo(builder);

			if (VariableInfo.HasDefaultValue) {
				builder.Append(" := ");
				VariableInfo.DefaultValue.AppendTo(builder);
			}
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		public static bool IsValidName(string name) {
			return name.IndexOfAny(InvalidChars) == -1;
		}
	}
}
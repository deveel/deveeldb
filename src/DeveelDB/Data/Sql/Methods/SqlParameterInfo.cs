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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Methods {
	public sealed class SqlParameterInfo : ISqlFormattable {
		public SqlParameterInfo(string name, SqlType parameterType) 
			: this(name, parameterType, null) {
		}

		public SqlParameterInfo(string name, SqlType parameterType, SqlExpression defaultValue) 
			: this(name, parameterType, defaultValue, SqlParameterDirection.In) {
		}

		public SqlParameterInfo(string name, SqlType parameterType, SqlParameterDirection direction) : this(name, parameterType, null, direction) {
		}

		public SqlParameterInfo(string name, SqlType parameterType, SqlExpression defaultValue, SqlParameterDirection direction) {
			if (String.IsNullOrWhiteSpace(name))
				throw new ArgumentNullException(nameof(name));
			if (parameterType == null)
				throw new ArgumentNullException(nameof(parameterType));

			Name = name;
			ParameterType = parameterType;
			DefaultValue = defaultValue;
			Direction = direction;
		}

		public SqlType ParameterType { get; }

		public string Name { get; }

		public SqlParameterDirection Direction { get; }

		public SqlExpression DefaultValue { get; }

		public bool HasDefaultValue => DefaultValue != null;

		public int Offset { get; internal set; }

		public bool IsOutput => (Direction & SqlParameterDirection.Out) != 0;

		public bool IsInput => (Direction & SqlParameterDirection.In) != 0;

		public bool IsRequired => IsInput && !HasDefaultValue;

		public bool IsDeterministic => ParameterType is SqlDeterministicType;

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.Append(Name);
			builder.Append(" ");
			ParameterType.AppendTo(builder);

			if (Direction != SqlParameterDirection.In) {
				builder.Append(" ");

				if (IsInput)
					builder.Append("INPUT");
				if (IsInput && IsOutput)
					builder.Append(" ");
				if (IsOutput)
					builder.Append("OUTPUT");
			}

			if (HasDefaultValue) {
				builder.Append(" := ");
				DefaultValue.AppendTo(builder);
			}
		}

		public override string ToString() {
			return this.ToSqlString();
		}
	}
}
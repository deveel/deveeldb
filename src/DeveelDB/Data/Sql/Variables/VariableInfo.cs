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

namespace Deveel.Data.Sql.Variables {
	public sealed class VariableInfo : IDbObjectInfo {
		public VariableInfo(string name, SqlType type, bool constant, SqlExpression defaultValue) {
			if (String.IsNullOrWhiteSpace(name))
				throw new ArgumentNullException(nameof(name));
			if (type == null)
				throw new ArgumentNullException(nameof(type));

			if (!Variable.IsValidName(name))
				throw new ArgumentException($"The variable name '{name}' is invalid");

			if (constant && defaultValue == null)
				throw new ArgumentNullException(nameof(defaultValue), "A constant variable must define a default value");

			Name = name;
			Type = type;
			Constant = constant;
			DefaultValue = defaultValue;
		}

		public string Name { get; }

		public SqlType Type { get; }

		public bool Constant { get; }

		public SqlExpression DefaultValue { get; }

		public bool HasDefaultValue => DefaultValue != null;

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Variable;

		ObjectName IDbObjectInfo.FullName => new ObjectName(Name);
	}
}
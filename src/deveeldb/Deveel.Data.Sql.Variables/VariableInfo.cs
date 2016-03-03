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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public sealed class VariableInfo : IObjectInfo {
		public VariableInfo(string variableName, SqlType type, bool isConstant) {
			if (String.IsNullOrEmpty(variableName))
				throw new ArgumentNullException("variableName");
			if (type == null)
				throw new ArgumentNullException("type");

			VariableName = variableName;
			Type = type;
			IsConstant = isConstant;
		}

		public string VariableName { get; private set; }

		public SqlType Type { get; private set; }

		public bool IsConstant { get; private set; }

		public bool IsNotNull { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Variable; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(VariableName); }
		}
	}
}
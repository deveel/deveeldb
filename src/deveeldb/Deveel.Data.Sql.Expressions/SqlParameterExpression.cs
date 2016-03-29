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

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlParameterExpression : SqlExpression {
		public SqlParameterExpression() 
			: this(QueryParameter.Marker) {
		}

		public SqlParameterExpression(string parameterName) {
			if (String.IsNullOrEmpty(parameterName))
				throw new ArgumentNullException("parameterName");

			ParameterName = parameterName;
		}

		public string ParameterName { get; private set; }

		public bool IsMarker {
			get { return String.Equals(ParameterName, QueryParameter.Marker); }
		}

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Parameter; }
		}
	}
}

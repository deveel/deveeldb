// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql {
	public sealed class JoinPart {
		internal JoinPart(JoinType joinType, ObjectName tableName, SqlExpression onExpression) {
			if (tableName == null) 
				throw new ArgumentNullException("tableName");

			OnExpression = onExpression;
			JoinType = joinType;
			TableName = tableName;
		}

		internal JoinPart(JoinType joinType, SqlQueryExpression subQuery, SqlExpression onExpression) {
			if (subQuery == null) 
				throw new ArgumentNullException("subQuery");

			OnExpression = onExpression;
			JoinType = joinType;
			SubQuery = subQuery;
		}

		public JoinType JoinType { get; private set; }

		public ObjectName TableName { get; private set; }

		public SqlQueryExpression SubQuery { get; private set; } 

		public SqlExpression OnExpression { get; private set; }
	}
}
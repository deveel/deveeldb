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
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	public sealed class ConstraintMapInfo {
		internal ConstraintMapInfo(MemberInfo member, string columnName, ConstraintType constraintType, string checkExpression) {
			Member = member;
			ColumnName = columnName;
			ConstraintType = constraintType;
			CheckExpression = checkExpression;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public ConstraintType ConstraintType { get; private set; }

		public string CheckExpression { get; private set; }

		internal AddConstraintAction AsAddConstraintAction() {
			var constraint = new SqlTableConstraint(ConstraintType, new[] { ColumnName });
			if (ConstraintType == ConstraintType.Check &&
				!String.IsNullOrEmpty(CheckExpression)) {
				constraint.CheckExpression = SqlExpression.Parse(CheckExpression);
			}

			// TODO: Implement the foreign keys

			return new AddConstraintAction(constraint);
		}
	}
}
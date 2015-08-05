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

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlTableConstraint : IPreparable {
		public SqlTableConstraint(string name, ConstraintType constraintType, string[] columns) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (columns == null || columns.Length == 0)
				throw new ArgumentNullException("columns");

			Name = name;
			ConstraintType = constraintType;
			Columns = columns;
		}

		public string Name { get; private set; }

		public ConstraintType ConstraintType { get; private set; }

		public string[] Columns { get; private set; }

		public SqlExpression CheckExpression { get; set; }

		public string ReferenceTable { get; set; }

		public string[] ReferenceColumns { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var checkExpression = CheckExpression;
			if (checkExpression != null)
				checkExpression = checkExpression.Prepare(preparer);

			return new SqlTableConstraint(Name, ConstraintType, Columns) {
				CheckExpression = checkExpression,
				ReferenceTable = ReferenceTable,
				ReferenceColumns = ReferenceColumns
			};
		}
	}
}

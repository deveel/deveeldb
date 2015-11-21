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
using System.IO;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlTableConstraint : IPreparable {
		public SqlTableConstraint(ConstraintType constraintType, string[] columns) 
			: this(null, constraintType, columns) {
		}

		public SqlTableConstraint(string constraintName, ConstraintType constraintType, string[] columns) {
			ConstraintName = constraintName;
			ConstraintType = constraintType;
			Columns = columns;
		}

		public string ConstraintName { get; private set; }

		public ConstraintType ConstraintType { get; private set; }

		public string[] Columns { get; private set; }

		public SqlExpression CheckExpression { get; set; }

		public string ReferenceTable { get; set; }

		public string[] ReferenceColumns { get; set; }

		public ForeignKeyAction OnDelete { get; set; }

		public ForeignKeyAction OnUpdate { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var checkExpression = CheckExpression;
			if (checkExpression != null)
				checkExpression = checkExpression.Prepare(preparer);

			return new SqlTableConstraint(ConstraintName, ConstraintType, Columns) {
				CheckExpression = checkExpression,
				ReferenceTable = ReferenceTable,
				ReferenceColumns = ReferenceColumns
			};
		}

		public static SqlTableConstraint UniqueKey(string constraintName, string[] columns) {
			return new SqlTableConstraint(constraintName, ConstraintType.Unique, columns);
		}

		public static SqlTableConstraint PrimaryKey(string constraintName, string[] columns) {
			return new SqlTableConstraint(constraintName, ConstraintType.PrimaryKey, columns);
		}

		public static SqlTableConstraint Check(string constraintName, SqlExpression expression) {
			return new SqlTableConstraint(constraintName, ConstraintType.Check, null) {
				CheckExpression = expression
			};
		}

		public static SqlTableConstraint ForeignKey(string constraintName, string[] columns, string refTable,
			string[] refcolumns, ForeignKeyAction onDelete, ForeignKeyAction onUpdate) {
			return new SqlTableConstraint(constraintName, ConstraintType.ForeignKey, columns) {
				ReferenceTable = refTable,
				ReferenceColumns = refcolumns,
				OnDelete = onDelete,
				OnUpdate = onUpdate
			};
		}

		public static void Serialize(SqlTableConstraint constraint, BinaryWriter writer) {
			throw new NotImplementedException();
		}

		public static SqlTableConstraint Deserialize(BinaryReader reader) {
			throw new NotImplementedException();
		}
	}
}

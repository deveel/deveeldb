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

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class ConstraintInfo {
		public ConstraintInfo(ConstraintType constraintType, ObjectName tableName, string[] columnNames) 
			: this(null, constraintType, tableName, columnNames) {
		}

		public ConstraintInfo(string constraintName, ConstraintType constraintType, ObjectName tableName, string[] columnNames) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (columnNames == null)
				throw new ArgumentNullException("columnNames");

			if (columnNames.Length == 0)
				throw new ArgumentException("The provided column names for the constraint is empty.", "columnNames");

			ConstraintName = constraintName;
			ColumnNames = columnNames;
			TableName = tableName;
			ConstraintType = constraintType;
		}

		public ConstraintType ConstraintType { get; private set; }

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; set; }

		public string[] ColumnNames { get; private set; }

		public SqlExpression CheckExpression { get; set; }

		public ObjectName ForeignTable { get; set; }

		public string[] ForeignColumnNames { get; set; }

		public ForeignKeyAction OnDelete { get; set; }

		public ForeignKeyAction OnUpdate { get; set; }

		public ConstraintDeferrability Deferred { get; set; }

		public static ConstraintInfo Unique(ObjectName tableName, params string[] columnNames) {
			return Unique(null, tableName, columnNames);
		}

		public static ConstraintInfo Unique(string constraintName, ObjectName tableName, string[] columnNames) {
			return new ConstraintInfo(constraintName, ConstraintType.Unique, tableName, columnNames);
		}

		public static ConstraintInfo Check(ObjectName tableName, SqlExpression expression, params string[] columnNames) {
			return Check(null, tableName, expression, columnNames);
		}

		public static ConstraintInfo Check(string constraintName, ObjectName tableName, SqlExpression expression, params string[] columnNames) {
			return new ConstraintInfo(constraintName, ConstraintType.Check, tableName, columnNames) {
				CheckExpression = expression
			};
		}

		public static ConstraintInfo PrimaryKey(ObjectName tableName, params string[] columnNames) {
			return PrimaryKey(null, tableName, columnNames);
		}

		public static ConstraintInfo PrimaryKey(string constraintName, ObjectName tableName, params string[] columnNames) {
			return new ConstraintInfo(constraintName, ConstraintType.PrimaryKey, tableName, columnNames);
		}

		public static ConstraintInfo ForeignKey(ObjectName tableName, string columnName, ObjectName refTable, string refColumn) {
			return ForeignKey(tableName, new[] {columnName}, refTable, new[] {refColumn});
		}

		public static ConstraintInfo ForeignKey(ObjectName tableName, string[] columnNames, ObjectName refTable, string[] refColumns) {
			return ForeignKey(null, tableName, columnNames, refTable, refColumns);
		}

		public static ConstraintInfo ForeignKey(string constraintName, ObjectName tableName, string[] columnNames,
			ObjectName refTable, string[] refColumns) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (refTable == null)
				throw new ArgumentNullException("refTable");
			if (columnNames == null || columnNames.Length == 0)
				throw new ArgumentException("At least one column is required", "columnNames");
			if (refColumns == null || refColumns.Length == 0)
				throw new ArgumentException("At least one referenced column is required.", "refColumns");

			if (columnNames.Length != refColumns.Length)
				throw new ArgumentException("The number of columns in the constraint must match the number of columns referenced.");

			var constraint = new ConstraintInfo(constraintName, ConstraintType.ForeignKey, tableName, columnNames);
			constraint.ForeignTable = refTable;
			constraint.ForeignColumnNames = refColumns;
			return constraint;
		}
	}
}
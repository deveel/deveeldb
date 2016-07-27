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
using System.IO;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlTableConstraint : IPreparable, ISerializable {
		public SqlTableConstraint(ConstraintType constraintType) 
			: this(constraintType, new string[0]) {
		}

		public SqlTableConstraint(ConstraintType constraintType, string[] columns) 
			: this(null, constraintType, columns) {
		}

		public SqlTableConstraint(string constraintName, ConstraintType constraintType) 
			: this(constraintName, constraintType, new string[0]) {
		}

		public SqlTableConstraint(string constraintName, ConstraintType constraintType, string[] columns) {
			if (columns == null)
				columns = new string[0];

			ConstraintName = constraintName;
			ConstraintType = constraintType;
			Columns = columns;
		}

		private SqlTableConstraint(SerializationInfo info, StreamingContext context) {
			ConstraintName = info.GetString("Name");
			ConstraintType = (ConstraintType) info.GetInt32("Type");
			Columns = (string[]) info.GetValue("Columns", typeof(string[]));
			CheckExpression = (SqlExpression) info.GetValue("Check", typeof(SqlExpression));
			ReferenceTable = info.GetString("ReferenceTable");
			ReferenceColumns = (string[]) info.GetValue("ReferenceColumns", typeof(string[]));
			OnDelete = (ForeignKeyAction) info.GetInt32("OnDelete");
			OnUpdate = (ForeignKeyAction) info.GetInt32("OnUpdate");
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
				ReferenceColumns = ReferenceColumns,
				OnDelete = OnDelete,
				OnUpdate = OnUpdate
			};
		}

		public static SqlTableConstraint UniqueKey(string constraintName, string[] columns) {
			return new SqlTableConstraint(constraintName, ConstraintType.Unique, columns);
		}

		public static SqlTableConstraint PrimaryKey(string constraintName, string[] columns) {
			return new SqlTableConstraint(constraintName, ConstraintType.PrimaryKey, columns);
		}

		public static SqlTableConstraint Check(string constraintName, SqlExpression expression) {
			return new SqlTableConstraint(constraintName, ConstraintType.Check, new string[0]) {
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

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", ConstraintName);
			info.AddValue("Type", (int)ConstraintType);
			info.AddValue("Columns", Columns);
			info.AddValue("ReferenceTable", ReferenceTable);
			info.AddValue("ReferenceColumns", ReferenceColumns);
			info.AddValue("Check", CheckExpression);
			info.AddValue("OnDelete", (int)OnDelete);
			info.AddValue("OnUpdate", (int)OnUpdate);
		}
	}
}

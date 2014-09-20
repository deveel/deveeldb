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
using System.Collections.Generic;

namespace Deveel.Data.DbSystem {
	[Serializable]
	public sealed class DataConstraintInfo {
		/// <summary>
		/// The name of the constraint.
		/// </summary>
		private string name;

		private TableName tableName;

		/// <summary>
		/// The type of this constraint.
		/// </summary>
		private readonly ConstraintType type;

		/// <summary>
		/// In case of a <see cref="ConstraintType.Check"/> constraint,
		/// this is the expression to check.
		/// </summary>
		private Expression checkExpression;
		
		/// <summary>
		/// The serializable plain check expression as originally parsed
		/// </summary>
		internal Expression OriginalCheckExpression;

		// The first column list
		private List<string> columns;

		// The second column list
		private List<string> refColumns = new List<string>();

		// The name of the table if referenced.
		private TableName refTableName;

		// The foreign key update rule
		private ConstraintAction updateRule;

		// The foreign key delete rule
		private ConstraintAction deleteRule;

		// Whether this constraint is deferred to when the transaction commits.
		// ( By default we are 'initially immediate deferrable' )
		private ConstraintDeferrability deferred = ConstraintDeferrability.InitiallyImmediate;

		private DataConstraintInfo(ConstraintType type) {
			this.type = type;
		}

		public ConstraintAction DeleteRule {
			get { return deleteRule; }
		}

		public ConstraintAction UpdateRule {
			get { return updateRule; }
		}

		public TableName ReferencedTableName {
			get { return refTableName; }
		}

		/// <summary>
		/// Gets or sets the name of the constraint.
		/// </summary>
		public string Name {
			get { return name; }
			set { name = value; }
		}

		public TableName TableName {
			get { return tableName; }
			set { tableName = value; }
		}

		/// <summary>
		/// Gets the type of constraint.
		/// </summary>
		/// <seealso cref="ConstraintType"/>
		public ConstraintType Type {
			get { return type; }
		}

		public string [] Columns {
			get { return columns.ToArray(); }
		}

		public string [] ReferencedColumns {
			get { return refColumns.ToArray(); }
		}

		/// <summary>
		/// If this constraint is a <see cref="ConstraintType.Check"/>, this property
		/// gets or sets the <see cref="Expression"/> that is checked.
		/// </summary>
		/// <exception cref="ArgumentException">
		/// If this constraint is not of type <see cref="ConstraintType.Check"/> and
		/// the user tries to set this property.
		/// </exception>
		public Expression CheckExpression {
			get { return checkExpression; }
			set {
				if (type != ConstraintType.Check)
					throw new ArgumentException("Cannot set the value of this constraint.");

				checkExpression = value;
				try {
					OriginalCheckExpression = (Expression)value.Clone();
				} catch (Exception e) {
					throw new ApplicationException(e.Message);
				}
			}
		}

		public ConstraintDeferrability Deferred {
			get { return deferred; }
			set { deferred = value; }
		}

		public static DataConstraintInfo PrimaryKey(string name, IEnumerable<string> columnNames) {
			DataConstraintInfo constraint = new DataConstraintInfo(ConstraintType.PrimaryKey);
			constraint.name = name;
			constraint.columns = new List<string>(columnNames);
			return constraint;
		}

		public static DataConstraintInfo Unique(string name, IEnumerable<string> columnNames) {
			DataConstraintInfo constraint = new DataConstraintInfo(ConstraintType.Unique);
			constraint.name = name;
			constraint.columns = new List<string>(columnNames);
			return constraint;
		}

		public static DataConstraintInfo ForeignKey(string name, IEnumerable<string> columns, TableName refTableName, IEnumerable<string> refColumns, 
			ConstraintAction onDelete, ConstraintAction onUpdate) {
			DataConstraintInfo constraint = new DataConstraintInfo(ConstraintType.ForeignKey);
			constraint.name = name;
			constraint.columns = new List<string>(columns);
			constraint.refTableName = refTableName;
			constraint.refColumns = new List<string>(refColumns);
			constraint.deleteRule = onDelete;
			constraint.updateRule = onUpdate;
			return constraint;
		}

		public static DataConstraintInfo Check(string name, Expression expression) {
			DataConstraintInfo constraint = new DataConstraintInfo(ConstraintType.Check);
			constraint.name = name;
			constraint.checkExpression = expression;
			constraint.OriginalCheckExpression = expression;
			return constraint;
		}
	}
}
// 
//  Copyright 2010  Deveel
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
using System.Collections;

namespace Deveel.Data {
	public sealed class DataTableConstraintDef {
		/// <summary>
		/// The name of the constraint.
		/// </summary>
		private string name;

		/// <summary>
		/// The type of this constraint.
		/// </summary>
		private ConstraintType type;

		/// <summary>
		/// In case of a <see cref="ConstraintType.Check"/> constraint,
		/// this is the expression to check.
		/// </summary>
		private Expression check_expression;
		
		/// <summary>
		/// The serializable plain check expression as originally parsed
		/// </summary>
		internal Expression original_check_expression;

		// The first column list
		private ArrayList columns;

		// The second column list
		private ArrayList refColumns = new ArrayList();

		// The name of the table if referenced.
		private string refTableName;

		// The foreign key update rule
		private ConstraintAction updateRule;

		// The foreign key delete rule
		private ConstraintAction deleteRule;

		// Whether this constraint is deferred to when the transaction commits.
		// ( By default we are 'initially immediate deferrable' )
		private ConstraintDeferrability deferred = ConstraintDeferrability.InitiallyImmediate;

		private DataTableConstraintDef(ConstraintType type) {
			this.type = type;
		}

		public ConstraintAction DeleteRule {
			get { return deleteRule; }
		}

		public ConstraintAction UpdateRule {
			get { return updateRule; }
		}

		public string ReferencedTableName {
			get { return refTableName; }
		}

		/// <summary>
		/// Gets or sets the name of the constraint.
		/// </summary>
		public string Name {
			get { return name; }
			set { name = value; }
		}

		/// <summary>
		/// Gets the type of constraint.
		/// </summary>
		/// <seealso cref="ConstraintType"/>
		public ConstraintType Type {
			get { return type; }
		}

		public string [] Columns {
			get { return (string[]) columns.ToArray(typeof(string)); }
		}

		internal ArrayList ColumnsList {
			get { return columns; }
		}

		public string [] ReferencedColumns {
			get { return (string[]) refColumns.ToArray(typeof(string)); }
		}

		internal ArrayList ReferencedColumnsList {
			get { return refColumns; }
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
			get { return check_expression; }
			set {
				if (type != ConstraintType.Check)
					throw new ArgumentException("Cannot set the value of this constraint.");

				check_expression = value;
				try {
					original_check_expression = (Expression)value.Clone();
				} catch (Exception e) {
					throw new ApplicationException(e.Message);
				}
			}
		}

		public ConstraintDeferrability Deferred {
			get { return deferred; }
			set { deferred = value; }
		}

		internal void SetReferencedTableName(string tableName) {
			refTableName = tableName;
		}

		public static DataTableConstraintDef PrimaryKey(string name, string[] columnNames) {
			DataTableConstraintDef constraint = new DataTableConstraintDef(ConstraintType.PrimaryKey);
			constraint.name = name;
			constraint.columns = new ArrayList(columnNames);
			return constraint;
		}

		public static DataTableConstraintDef Unique(string name, string[] columnNames) {
			DataTableConstraintDef constraint = new DataTableConstraintDef(ConstraintType.Unique);
			constraint.name = name;
			constraint.columns = new ArrayList(columnNames);
			return constraint;
		}

		public static DataTableConstraintDef ForeignKey(string name, string[] columns, string refTableName, string[] refColumns, 
			ConstraintAction onDelete, ConstraintAction onUpdate) {
			DataTableConstraintDef constraint = new DataTableConstraintDef(ConstraintType.ForeignKey);
			constraint.name = name;
			constraint.columns = new ArrayList(columns);
			constraint.refTableName = refTableName;
			constraint.refColumns = new ArrayList(refColumns);
			constraint.deleteRule = onDelete;
			constraint.updateRule = onUpdate;
			return constraint;
		}

		public static DataTableConstraintDef Check(string name, Expression expression) {
			DataTableConstraintDef constraint = new DataTableConstraintDef(ConstraintType.Check);
			constraint.name = name;
			constraint.check_expression = expression;
			constraint.original_check_expression = expression;
			return constraint;
		}
	}
}
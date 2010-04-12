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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a constraint definition (description) for a table.
	/// </summary>
	[Serializable]
	sealed class SqlConstraint : IStatementTreeObject {
		private SqlConstraint(ConstraintType type) {
			this.type = type;
		}

		// The type of constraint (from types in DataTableConstraintDef)
		private ConstraintType type;

		// The name of the constraint or null if the constraint has no name (in
		// which case it must be given an auto generated unique name at some point).
		private String name;

		// The Check Expression
		private Expression check_expression;
		// The serializable plain check expression as originally parsed
		internal Expression original_check_expression;

		// The first column list
		internal ArrayList column_list;

		// The second column list
		internal ArrayList column_list2;

		// The name of the table if referenced.
		private String reference_table_name;

		// The foreign key update rule
		private ConstraintAction update_rule;

		// The foreign key delete rule
		private ConstraintAction delete_rule;

		// Whether this constraint is deferred to when the transaction commits.
		// ( By default we are 'initially immediate deferrable' )
		private ConstraintDeferrability deferred = ConstraintDeferrability.InitiallyImmediate;

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

		///<summary>
		/// Sets object up for a primary key constraint.
		///</summary>
		///<param name="list"></param>
		public static SqlConstraint PrimaryKey(string[] columns) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.PrimaryKey);
			constraint.column_list = new ArrayList(columns);
			return constraint;
		}

		///<summary>
		/// Sets object up for a unique constraint.
		///</summary>
		///<param name="list"></param>
		public static SqlConstraint Unique(string[] columns) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.Unique);
			constraint.column_list = new ArrayList(columns);
			return constraint;
		}

		///<summary>
		/// Sets object up for a check constraint.
		///</summary>
		///<param name="exp"></param>
		///<exception cref="ApplicationException"></exception>
		public static SqlConstraint Check(Expression exp) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.Check);
			constraint.CheckExpression = exp;
			return constraint;
		}

		///<summary>
		/// Sets object up for foreign key reference.
		///</summary>
		///<param name="ref_table"></param>
		///<param name="col_list"></param>
		///<param name="ref_col_list"></param>
		///<param name="delete_rule"></param>
		///<param name="update_rule"></param>
		public static SqlConstraint ForeignKey(String ref_table, string [] col_list,
								  string[] ref_col_list,
								  ConstraintAction delete_rule, ConstraintAction update_rule) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.ForeignKey);
			constraint.reference_table_name = ref_table;
			constraint.column_list = new ArrayList(col_list);
			constraint.column_list2 = new ArrayList(ref_col_list);
			constraint.delete_rule = delete_rule;
			constraint.update_rule = update_rule;

			return constraint;
		}

		///<summary>
		/// Sets that this constraint is initially deferred.
		///</summary>
		internal void SetInitiallyDeferred() {
			deferred = ConstraintDeferrability.InitiallyDeferred;
		}

		/// <summary>
		/// Sets that this constraint is not deferrable.
		/// </summary>
		internal void SetNotDeferrable() {
			deferred = ConstraintDeferrability.NotDeferrable;
		}


		/// <summary>
		/// Returns the first column list as a string array.
		/// </summary>
		public string[] ColumnList {
			get { return (String[]) column_list.ToArray(typeof (string)); }
			set { column_list = new ArrayList(value); }
		}

		/// <summary>
		/// Returns the first column list as a string array.
		/// </summary>
		public string[] ColumnList2 {
			get { return (String[]) column_list2.ToArray(typeof (string)); }
			set { column_list2 = new ArrayList(value); }
		}

		/// <summary>
		/// Returns the delete rule if this is a foreign key reference.
		/// </summary>
		public ConstraintAction DeleteRule {
			get { return delete_rule; }
		}

		/// <summary>
		/// Returns the update rule if this is a foreign key reference.
		/// </summary>
		public ConstraintAction UpdateRule {
			get { return update_rule; }
		}

		public string ReferenceTable {
			get { return reference_table_name; }
			set { reference_table_name = value; }
		}

		public ConstraintDeferrability Deferrability {
			get { return deferred; }
			set { deferred = value; }
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (check_expression != null) {
				check_expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			SqlConstraint v = (SqlConstraint)MemberwiseClone();
			if (check_expression != null) {
				v.check_expression = (Expression)check_expression.Clone();
			}
			if (column_list != null) {
				v.column_list = (ArrayList)column_list.Clone();
			}
			if (column_list2 != null) {
				v.column_list2 = (ArrayList)column_list2.Clone();
			}
			return v;
		}

	}
}
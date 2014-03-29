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
using System.Collections.Generic;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a constraint definition (description) for a table.
	/// </summary>
	[Serializable]
	sealed class SqlConstraint : IStatementTreeObject {
		private SqlConstraint(ConstraintType type) {
			this.type = type;
		}

		// The type of constraint (from types in DataConstraintInfo)
		private readonly ConstraintType type;

		// The name of the constraint or null if the constraint has no name (in
		// which case it must be given an auto generated unique name at some point).
		private String name;

		// The Check Expression
		private Expression checkExpression;

		// The serializable plain check expression as originally parsed
		internal Expression original_check_expression;

		// The first column list
		private List<string> column_list;

		// The second column list
		internal List<string> column_list2;

		// The name of the table if referenced.
		private String reference_table_name;

		// The foreign key update rule
		private ConstraintAction updateRule;

		// The foreign key delete rule
		private ConstraintAction deleteRule;

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
			get { return checkExpression; }
			set {
				if (type != ConstraintType.Check)
					throw new ArgumentException("Cannot set the value of this constraint.");

				checkExpression = value;
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
		///<param name="columns"></param>
		public static SqlConstraint PrimaryKey(IEnumerable<string> columns) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.PrimaryKey);
			constraint.column_list = new List<string>(columns);
			return constraint;
		}

		///<summary>
		/// Sets object up for a unique constraint.
		///</summary>
		///<param name="columns"></param>
		public static SqlConstraint Unique(IEnumerable<string> columns) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.Unique);
			constraint.column_list = new List<string>(columns);
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
		///<param name="refTable"></param>
		///<param name="columns"></param>
		///<param name="refColumns"></param>
		///<param name="deleteRule"></param>
		///<param name="updateRule"></param>
		public static SqlConstraint ForeignKey(string refTable, IEnumerable<string> columns, IEnumerable<string> refColumns,
								  ConstraintAction deleteRule, ConstraintAction updateRule) {
			SqlConstraint constraint = new SqlConstraint(ConstraintType.ForeignKey);
			constraint.reference_table_name = refTable;
			constraint.column_list = new List<string>(columns);
			constraint.column_list2 = new List<string>(refColumns);
			constraint.deleteRule = deleteRule;
			constraint.updateRule = updateRule;

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
		public IList<string> ColumnList {
			get { return column_list; }
		}

		/// <summary>
		/// Returns the first column list as a string array.
		/// </summary>
		public IList<string> ColumnList2 {
			get { return column_list2; }
		}

		/// <summary>
		/// Returns the delete rule if this is a foreign key reference.
		/// </summary>
		public ConstraintAction DeleteRule {
			get { return deleteRule; }
		}

		/// <summary>
		/// Returns the update rule if this is a foreign key reference.
		/// </summary>
		public ConstraintAction UpdateRule {
			get { return updateRule; }
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
			if (checkExpression != null) {
				checkExpression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public object Clone() {
			SqlConstraint v = (SqlConstraint)MemberwiseClone();
			if (checkExpression != null) {
				v.checkExpression = (Expression)checkExpression.Clone();
			}
			if (column_list != null) {
				v.column_list = new List<string>(column_list);
			}
			if (column_list2 != null) {
				v.column_list2 = new List<string>(column_list2);
			}
			return v;
		}

	}
}
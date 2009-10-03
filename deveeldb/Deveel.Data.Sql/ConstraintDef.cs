// 
//  ConstraintDef.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a constraint definition (description) for a table.
	/// </summary>
	[Serializable]
	public sealed class ConstraintDef : IStatementTreeObject {
		// ---------- Statics that represent the base types of constraints ----------

		// The type of constraint (from types in DataTableConstraintDef)
		internal int type;

		// The name of the constraint or null if the constraint has no name (in
		// which case it must be given an auto generated unique name at some point).
		private String name;

		// The Check Expression
		internal Expression check_expression;
		// The serializable plain check expression as originally parsed
		internal Expression original_check_expression;

		// The first column list
		internal ArrayList column_list;

		// The second column list
		internal ArrayList column_list2;

		// The name of the table if referenced.
		internal String reference_table_name;

		// The foreign key update rule
		String update_rule;

		// The foreign key delete rule
		String delete_rule;

		// Whether this constraint is deferred to when the transaction commits.
		// ( By default we are 'initially immediate deferrable' )
		internal short deferred = Transaction.INITIALLY_IMMEDIATE;

		/// <summary>
		/// Gets or sets the name of the constraint.
		/// </summary>
		public string Name {
			get { return name; }
			set { name = value; }
		}

		///<summary>
		/// Sets object up for a primary key constraint.
		///</summary>
		///<param name="list"></param>
		public void SetPrimaryKey(ArrayList list) {
			type = ConstraintType.PrimaryKey;
			column_list = list;
		}

		///<summary>
		/// Sets object up for a unique constraint.
		///</summary>
		///<param name="list"></param>
		public void SetUnique(ArrayList list) {
			type = ConstraintType.Unique;
			column_list = list;
		}

		///<summary>
		/// Sets object up for a check constraint.
		///</summary>
		///<param name="exp"></param>
		///<exception cref="ApplicationException"></exception>
		public void SetCheck(Expression exp) {
			type = ConstraintType.Check;
			check_expression = exp;
			try {
				original_check_expression = (Expression)exp.Clone();
			} catch (Exception e) {
				throw new ApplicationException(e.Message);
			}
		}

		///<summary>
		/// Sets object up for foreign key reference.
		///</summary>
		///<param name="ref_table"></param>
		///<param name="col_list"></param>
		///<param name="ref_col_list"></param>
		///<param name="delete_rule"></param>
		///<param name="update_rule"></param>
		public void SetForeignKey(String ref_table, ArrayList col_list,
								  ArrayList ref_col_list,
								  String delete_rule, String update_rule) {
			type = ConstraintType.ForeignKey;
			reference_table_name = ref_table;
			column_list = col_list;
			column_list2 = ref_col_list;
			this.delete_rule = delete_rule;
			this.update_rule = update_rule;

			//    Console.Out.WriteLine("ConstraintDef setting rules: " + delete_rule + ", " + update_rule);
		}

		///<summary>
		/// Sets that this constraint is initially deferred.
		///</summary>
		public void SetInitiallyDeferred() {
			deferred = Transaction.INITIALLY_DEFERRED;
		}

		/// <summary>
		/// Sets that this constraint is not deferrable.
		/// </summary>
		public void SetNotDeferrable() {
			deferred = Transaction.NOT_DEFERRABLE;
		}


		/// <summary>
		/// Returns the first column list as a string array.
		/// </summary>
		public string[] ColumnList {
			get { return (String[]) column_list.ToArray(typeof (string)); }
		}

		/// <summary>
		/// Returns the first column list as a string array.
		/// </summary>
		public string[] ColumnList2 {
			get { return (String[]) column_list2.ToArray(typeof (string)); }
		}

		/// <summary>
		/// Returns the delete rule if this is a foreign key reference.
		/// </summary>
		public string DeleteRule {
			get { return delete_rule; }
		}

		/// <summary>
		/// Returns the update rule if this is a foreign key reference.
		/// </summary>
		public string UpdateRule {
			get { return update_rule; }
		}


		/// <inheritdoc/>
		public void PrepareExpressions(IExpressionPreparer preparer) {
			if (check_expression != null) {
				check_expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			ConstraintDef v = (ConstraintDef)MemberwiseClone();
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
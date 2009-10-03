// 
//  SelectColumn.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a column selected to be in the output of a select
	/// statement.
	/// </summary>
	/// <remarks>
	/// This includes being either an aggregate function, a column or "*" 
	/// which is the entire set of columns.
	/// </remarks>
	[Serializable]
	public sealed class SelectColumn : IStatementTreeObject {
		/// <summary>
		/// If the column represents a glob of columns (eg. 'Part.*' or '*') then 
		/// this is set to the glob string and 'expression' is left blank.
		/// </summary>
		public String glob_name;

		/// <summary>
		/// The fully resolved name that this column is given in the resulting table.
		/// </summary>
		public Variable resolved_name;

		/// <summary>
		/// The alias of this column string.
		/// </summary>
		public String alias;

		/// <summary>
		/// The expression of this column.
		/// </summary>
		/// <remarks>
		/// This is only NOT set when name == "*" indicating all the columns.
		/// </remarks>
		public Expression expression;

		/// <summary>
		/// The name of this column used internally to reference it.
		/// </summary>
		public Variable internal_name;


		/// <inheritdoc/>
		public void PrepareExpressions(IExpressionPreparer preparer) {
			if (expression != null) {
				expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			SelectColumn v = (SelectColumn)MemberwiseClone();
			if (resolved_name != null) {
				v.resolved_name = (Variable)resolved_name.Clone();
			}
			if (expression != null) {
				v.expression = (Expression)expression.Clone();
			}
			if (internal_name != null) {
				v.internal_name = (Variable)internal_name.Clone();
			}
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			String str = "";
			if (glob_name != null) str += " GLOB_NAME = " + glob_name;
			if (resolved_name != null) str += " RESOLVED_NAME = " + resolved_name;
			if (alias != null) str += " ALIAS = " + alias;
			if (expression != null) str += " EXPRESSION = " + expression;
			if (internal_name != null) str += " INTERNAL_NAME = " + internal_name;
			return str;
		}
	}
}
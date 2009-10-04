//  
//  ByColumn.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Object used to represent a column in the <i>ORDER BY</i> and 
	/// <i>GROUP BY</i> clauses of a select statement.
	/// </summary>
	[Serializable]
	public sealed class ByColumn : IStatementTreeObject {
		/// <summary>
		/// The name of the column in the 'by'.
		/// </summary>
		public Variable name;

		/// <summary>
		/// The expression that we are ordering by.
		/// </summary>
		public Expression exp;

		/// <summary>
		/// If 'order by' then true if sort is ascending (default).
		/// </summary>
		public bool ascending = true;


		public void PrepareExpressions(IExpressionPreparer preparer) {
			if (exp != null) {
				exp.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			ByColumn v = (ByColumn)MemberwiseClone();
			if (name != null) {
				v.name = (Variable)name.Clone();
			}
			if (exp != null) {
				v.exp = (Expression)exp.Clone();
			}
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			return "ByColumn(" + name + ", " + exp + ", " + ascending + ")";
		}

	}
}
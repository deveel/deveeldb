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
using System.Text;

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
		/// Constructs a new <see cref="SelectColumn"/> for the given
		/// expression and aliased with the given name.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used for select
		/// a column within a <c>SELECT</c> statement.</param>
		/// <param name="alias">The name to alias the resulted expression.</param>
		public SelectColumn(Expression expression, string alias) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			this.expression = expression;
			this.alias = alias;
		}

		/// <summary>
		/// Constructs a new <see cref="SelectColumn"/> for the given
		/// expression.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used for select
		/// a column within a <c>SELECT</c> statement.</param>
		public SelectColumn(Expression expression)
			: this(expression, null) {
		}

		/// <summary>
		/// An internal empty constructor.
		/// </summary>
		internal SelectColumn() {
		}

		/// <summary>
		/// If the column represents a glob of columns (eg. 'Part.*' or '*') then 
		/// this is set to the glob string and 'expression' is left blank.
		/// </summary>
		internal String glob_name;

		/// <summary>
		/// The fully resolved name that this column is given in the resulting table.
		/// </summary>
		internal VariableName resolved_name;

		/// <summary>
		/// The alias of this column string.
		/// </summary>
		private String alias;

		/// <summary>
		/// The expression of this column.
		/// </summary>
		/// <remarks>
		/// This is only NOT set when name == "*" indicating all the columns.
		/// </remarks>
		private Expression expression;

		/// <summary>
		/// The name of this column used internally to reference it.
		/// </summary>
		internal VariableName internal_name;

		/// <summary>
		/// Returns a new instance of a <see cref="SelectColumn"/> that is
		/// used to select the <c>IDENTITY</c> of a table.
		/// </summary>
		public static SelectColumn Identity {
			get {
				SelectColumn column = new SelectColumn();
				column.resolved_name = new VariableName("IDENTITY");
				return column;
			}
		}

		/// <summary>
		/// Gets the expression used to select the column.
		/// </summary>
		/// <remarks>
		/// This will always be <b>null</b> if the column is an asterisk
		/// (*), which means the expression is selecting all the columns.
		/// </remarks>
		public Expression Expression {
			get { return expression; }
		}

		/// <summary>
		/// Gets the name used to alias the select expression.
		/// </summary>
		public string Alias {
			get { return alias; }
		}

		internal void SetExpression(Expression exp) {
			expression = exp;
		}

		internal void SetAlias(string name) {
			alias = name;
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (expression != null) {
				expression.Prepare(preparer);
			}
		}

		/// <summary>
		/// Creates a special <see cref="SelectColumn"/> that is used to select
		/// all the columns in a table.
		/// </summary>
		/// <param name="glob">The <i>glob</i> name for the column, which can be
		/// a simple asterisk (*) or prefixed by a table name (eg. Table.*).</param>
		/// <returns>
		/// Returns an instance of <see cref="SelectColumn"/> that is specially used
		/// for selecting all the columns from a table.
		/// </returns>
		public static SelectColumn Glob(string glob) {
			SelectColumn column = new SelectColumn();
			column.glob_name = glob;
			return column;
		}

		/// <inheritdoc/>
		public object Clone() {
			SelectColumn v = (SelectColumn)MemberwiseClone();
			if (resolved_name != null) {
				v.resolved_name = (VariableName)resolved_name.Clone();
			}
			if (expression != null) {
				v.expression = (Expression)expression.Clone();
			}
			if (internal_name != null) {
				v.internal_name = (VariableName)internal_name.Clone();
			}
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder sb = new StringBuilder();
			if (glob_name != null) 
				sb.Append(" GLOB_NAME = " + glob_name);
			if (resolved_name != null) 
				sb.Append(" RESOLVED_NAME = " + resolved_name);
			if (alias != null) 
				sb.Append(" ALIAS = " + alias);
			if (expression != null) 
				sb.Append(" EXPRESSION = " + expression);
			if (internal_name != null)
				sb.Append(" INTERNAL_NAME = " + internal_name);
			return sb.ToString();
		}
	}
}
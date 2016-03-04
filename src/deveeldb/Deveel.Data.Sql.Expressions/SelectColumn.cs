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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Represents a column selected to be in the output of a select
	/// statement.
	/// </summary>
	/// <remarks>
	/// This includes being either an aggregate function, a column or "*" 
	/// which is the entire set of columns.
	/// </remarks>
	[Serializable]
	public sealed class SelectColumn : IPreparable, ISerializable {
		/// <summary>
		/// Constructs a new <see cref="SelectColumn"/> for the given
		/// expression and aliased with the given name.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used for select
		/// a column within a <c>SELECT</c> statement.</param>
		/// <param name="alias">The name to alias the resulted expression.</param>
		public SelectColumn(SqlExpression expression, string alias) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			Expression = expression;
			Alias = alias;
		}

		/// <summary>
		/// Constructs a new <see cref="SelectColumn"/> for the given
		/// expression.
		/// </summary>
		/// <param name="expression">The <see cref="Expression"/> used for select
		/// a column within a <c>SELECT</c> statement.</param>
		public SelectColumn(SqlExpression expression)
			: this(expression, null) {
		}

		private SelectColumn(SerializationInfo info, StreamingContext context) {
			Expression = (SqlExpression)info.GetValue("Expression", typeof(SqlExpression));
			Alias = info.GetString("Alias");
		}

		/// <summary>
		/// Gets the expression used to select the column.
		/// </summary>
		public SqlExpression Expression { get; private set; }

		/// <summary>
		/// Gets the name used to alias the select expression.
		/// </summary>
		public string Alias { get; private set; }

		public bool IsGlob {
			get {
				return (Expression is SqlReferenceExpression) &&
				       ((SqlReferenceExpression) Expression).ReferenceName.IsGlob;
			}
		}

		public bool IsAll {
			get {
				return (Expression is SqlReferenceExpression) &&
				       ((SqlReferenceExpression) Expression).ReferenceName.IsGlob &&
					   ((SqlReferenceExpression)Expression).ReferenceName.FullName == "*";
			}
		}

		public ObjectName TableName {
			get {
				var refExp = Expression as SqlReferenceExpression;
				if (refExp == null)
					return null;

				return refExp.ReferenceName.Parent;
			}
		}

		public ObjectName ReferenceName {
			get {
				var refExp = Expression as SqlReferenceExpression;
				if (refExp == null)
					return null;

				return refExp.ReferenceName;
			}
		}

		/// <summary>
		/// The name of this column used internally to reference it.
		/// </summary>
		internal ObjectName InternalName { get; set; }

		/// <summary>
		/// The fully resolved name that this column is given in the resulting table.
		/// </summary>
		internal ObjectName ResolvedName { get; set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Expression", Expression, typeof(SqlExpression));
			info.AddValue("Alias", Alias);
		}

		/// <inheritdoc/>
		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var exp = Expression;
			if (exp != null) {
				exp = exp.Prepare(preparer);
			}

			return new SelectColumn(exp, Alias);
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
			return new SelectColumn(SqlExpression.Reference(ObjectName.Parse(glob)));
		}
	}
}
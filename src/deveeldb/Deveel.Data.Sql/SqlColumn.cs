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

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a column definition (description).
	/// </summary>
	[Serializable]
	sealed class SqlColumn : IStatementTreeObject {
		private string name;
		private readonly bool fromParser;
		private TType type;
		private string index_str;

		private Expression default_expression;
		internal Expression original_default_expression;

		private ColumnConstraints constraints = ColumnConstraints.None;

		internal SqlColumn(bool fromParser) {
			this.fromParser = fromParser;
		}

		/// <summary>
		/// Creates a new instance of <see cref="SqlColumn"/> with the given
		/// <paramref name="name"/> and <see cref="TType">type</see>.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="type">The <see cref="TType"/> of the column.</param>
		public SqlColumn(string name, TType type)
			: this(false) {
			if (name == null)
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			this.name = name;
			this.type = type;
		}

		/// <summary>
		/// Returns true if this column has a primary key constraint set on it.
		/// </summary>
		public bool IsPrimaryKey {
			get { return (constraints & ColumnConstraints.PrimaryKey) != 0; }
			set {
				if (value) {
					constraints |= ColumnConstraints.PrimaryKey;
				} else {
					constraints &= ~ColumnConstraints.PrimaryKey;
				}
			}
		}

		/// <summary>
		/// Returns true if this column has the unique constraint set for it.
		/// </summary>
		public bool IsUnique {
			get { return (constraints & ColumnConstraints.Unique) != 0; }
			set {
				if (value) {
					constraints |= ColumnConstraints.Unique;
				} else {
					constraints &= ~ColumnConstraints.Unique;
				}
			}
		}

		/// <summary>
		/// Returns true if this column has the not null constraint set for it.
		/// </summary>
		public bool IsNotNull {
			get { return (constraints & ColumnConstraints.NotNull) != 0; }
			set {
				if (value) {
					constraints |= ColumnConstraints.NotNull;
				} else {
					constraints &= ~ColumnConstraints.NotNull;
				}
			}
		}

		/// <summary>
		/// Gets or sets the name of the column.
		/// </summary>
		public string Name {
			get { return name; }
		}

		///<summary>
		/// Gets the type of data of this column.
		///</summary>
		public TType Type {
			get { return type; }
		}

		/// <summary>
		/// Gets or sets the <c>DEFAULT</c> expression for the column.
		/// </summary>
		/// <remarks>
		/// This is the value set to the cell if none was specified.
		/// </remarks>
		public Expression Default {
			get { return default_expression; }
			set {
				if (type.SqlType == SqlType.Identity) {
					const string message = "Cannot specify a DEFAULT expression for an IDENTITY column.";
					if (fromParser)
						throw new ParseException(message);
					throw new ArgumentException(message);
				}

				default_expression = value;
				try {
					original_default_expression = (Expression)value.Clone();
				} catch (Exception e) {
					throw new ApplicationException(e.Message);
				}
			}
		}

		/// <summary>
		/// Gets or sets the constraints for the column.
		/// </summary>
		/// <seealso cref="ColumnConstraints"/>
		public ColumnConstraints Constraints {
			get { return constraints; }
			set { constraints = value; }
		}

		///<summary>
		/// Gets or sets the type of index for the column.
		///</summary>
		public string IndexScheme {
			get { return index_str; }
		}

		/// <summary>
		/// Toggles whether the column is an <c>IDENTITY</c> column.
		/// </summary>
		/// <remarks>
		/// An <c>IDENTITY</c> column is a special kind that is fully
		/// constrained and it is used to retrieve a unique value to identify
		/// a row of the table.
		/// <para>
		/// Tables can only have one identity: specifying more will generate
		/// exceptions.
		/// </para>
		/// <para>
		/// The value of an <c>IDENTITY</c> of a table is automatically set
		/// when a new entry is inserted to the table.
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentException">
		/// If the <see cref="Type"/> of this column is not <c>NUMERIC</c>.
		/// </exception>
		public bool Identity {
			get { return type.SqlType == SqlType.Identity; }
			set {
				if (value && !(Type is TNumericType)) {
					const string message = "Cannot set a non-numeric column as IDENTITY.";
					if (fromParser)
						throw new ParseException(message);
					throw new ArgumentException(message, "value");
				}

				if (value) {
					type = TType.GetNumericType(SqlType.Identity, -1, -1);
					// if this is an identity column it MUST be constrained...
					constraints = ColumnConstraints.All;
				}
			}
		}

		internal void SetName(string value) {
			name = value;
		}

		internal void SetType(TType value) {
			type = value;
		}

		///<summary>
		/// Adds a constraint to this column.
		///</summary>
		///<param name="constraint"></param>
		///<exception cref="Exception"></exception>
		internal void AddConstraint(String constraint) {
			if (constraint.Equals("NOT NULL")) {
				constraints |= ColumnConstraints.NotNull;
			} else if (constraint.Equals("NULL")) {
				constraints &= ~ColumnConstraints.NotNull;
			} else if (constraint.Equals("PRIMARY")) {
				constraints |= ColumnConstraints.PrimaryKey;
			} else if (constraint.Equals("UNIQUE")) {
				constraints |= ColumnConstraints.Unique;
			} else {
				throw new Exception("Unknown constraint: " + constraint);
			}
		}


		///<summary>
		/// Sets the indexing.
		///</summary>
		///<param name="t"></param>
		///<exception cref="ParseException"></exception>
		internal void SetIndex(Token t) {
			if (t.kind == SQLConstants.INDEX_NONE) {
				index_str = "BlindSearch";
			} else if (t.kind == SQLConstants.INDEX_BLIST) {
				index_str = "InsertSearch";
			} else {
				throw new ParseException("Unrecognized indexing scheme.");
			}
		}


		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (default_expression != null) {
				default_expression.Prepare(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			SqlColumn v = (SqlColumn)MemberwiseClone();
			if (default_expression != null) {
				v.default_expression = (Expression)default_expression.Clone();
			}
			return v;
		}
	}
}
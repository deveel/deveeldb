// 
//  TableName.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An immutable name of a table and any associated referencing information.
	/// </summary>
	[Serializable]
	public sealed class TableName : IComparable {
		/// <summary>
		/// The constant 'schema_name' that defines a schema that is unknown.
		/// </summary>
		private const String UnknownSchemaName = "##UNKNOWN_SCHEMA##";

		/// <summary>
		/// The name of the schema of the table.  This value can be <b>null</b> which
		/// means the schema is currently unknown.
		/// </summary>
		/// <seealso cref="UnknownSchemaName"/>
		private readonly String schema_name;

		/// <summary>
		/// The name of the table.
		/// </summary>
		private readonly String table_name;

		/// <summary>
		/// Constructs the table name with the given schema and name.
		/// </summary>
		/// <param name="schema_name">The name of the schema owning the table.</param>
		/// <param name="table_name">The name of the table.</param>
		public TableName(String schema_name, String table_name) {
			if (table_name == null)
				throw new ArgumentNullException("table_name");
			if (schema_name == null)
				schema_name = UnknownSchemaName;

			this.schema_name = schema_name;
			this.table_name = table_name;
		}

		public TableName(String table_name)
			: this(UnknownSchemaName, table_name) {
		}

		/// <summary>
		/// Returns the schema name or null if the schema name is unknown.
		/// </summary>
		public string Schema {
			get { return schema_name.Equals(UnknownSchemaName) ? null : schema_name; }
		}

		/// <summary>
		/// Returns the table name.
		/// </summary>
		public string Name {
			get { return table_name; }
		}

		/// <summary>
		/// Resolves a schema reference in a table name.
		/// </summary>
		/// <param name="scheman"></param>
		/// <remarks>
		/// If the schema in this table is 'null' (which means the schema 
		/// is unknown) then it is set to the given schema argument.
		/// </remarks>
		/// <returns></returns>
		public TableName ResolveSchema(String scheman) {
			return schema_name.Equals(UnknownSchemaName) ? new TableName(scheman, Name) : this;
		}

		/// <summary>
		/// Resolves a [schema name].[table name] type syntax to a TableName object.
		/// </summary>
		/// <param name="schemav"></param>
		/// <param name="namev"></param>
		/// <remarks>
		/// Uses <paramref name="schemav"/> only if there is no schema name explicitely specified.
		/// </remarks>
		/// <returns></returns>
		public static TableName Resolve(String schemav, String namev) {
			int i = namev.IndexOf('.');
			return i == -1 ? new TableName(schemav, namev) : new TableName(namev.Substring(0, i), namev.Substring(i + 1));
		}

		/// <summary>
		/// Resolves a [schema name].[table name] type syntax to a <see cref="TableName"/> object.
		/// </summary>
		/// <param name="namev"></param>
		/// <returns></returns>
		public static TableName Resolve(String namev) {
			return Resolve(UnknownSchemaName, namev);
		}

		// ----

		/// <inheritdoc/>
		public override String ToString() {
			return Schema != null ? Schema + "." + Name : Name;
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			TableName tn = (TableName)ob;
			return tn.schema_name.Equals(schema_name) &&
				   tn.table_name.Equals(table_name);
		}

		/// <inheritdoc/>
		public bool EqualsIgnoreCase(TableName tn) {
			return String.Compare(tn.schema_name, schema_name, true) == 0 &&
				   String.Compare(tn.table_name, table_name, true) == 0;
		}

		/// <inheritdoc/>
		public int CompareTo(Object ob) {
			TableName tn = (TableName)ob;
			int v = schema_name.CompareTo(tn.schema_name);
			if (v == 0) {
				return table_name.CompareTo(tn.table_name);
			}
			return v;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return schema_name.GetHashCode() + table_name.GetHashCode();
		}

	}
}
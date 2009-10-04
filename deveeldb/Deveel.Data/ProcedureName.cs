//  
//  ProcedureName.cs
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

namespace Deveel.Data {
	///<summary>
	/// The name of a procedure as understood by a ProcedureManager.
	///</summary>
	public class ProcedureName {
		/// <summary>
		/// The schema of this procedure.
		/// </summary>
		private readonly String schema;
		/// <summary>
		/// The name of this procedure.
		/// </summary>
		private readonly String name;

		///<summary>
		///</summary>
		///<param name="schema"></param>
		///<param name="name"></param>
		public ProcedureName(String schema, String name) {
			this.schema = schema;
			this.name = name;
		}

		///<summary>
		///</summary>
		///<param name="table_name"></param>
		public ProcedureName(TableName table_name)
			: this(table_name.Schema, table_name.Name) {
		}

		///<summary>
		/// Returns the schema of this procedure.
		///</summary>
		public string Schema {
			get { return schema; }
		}

		/// <summary>
		/// Returns the name of this procedure.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <inheritdoc/>
		public override string ToString() {
			return schema + "." + name;
		}

		///<summary>
		/// Returns a version of this procedure qualified to the given schema 
		/// (unless the schema is present).
		///</summary>
		///<param name="current_schema"></param>
		///<param name="proc_name"></param>
		///<returns></returns>
		public static ProcedureName Qualify(String current_schema, String proc_name) {
			int delim = proc_name.IndexOf(".");
			return delim == -1
			       	? new ProcedureName(current_schema, proc_name)
			       	: new ProcedureName(proc_name.Substring(0, delim),
			       	                    proc_name.Substring(delim + 1, proc_name.Length));
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			ProcedureName src_ob = (ProcedureName)ob;
			return (schema.Equals(src_ob.schema) &&
					name.Equals(src_ob.name));
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return schema.GetHashCode() + name.GetHashCode();
		}
	}
}
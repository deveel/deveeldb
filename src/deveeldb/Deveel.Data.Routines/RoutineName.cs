// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Routines {
	///<summary>
	/// The name of a procedure as understood by a RoutinesManager.
	///</summary>
	public class RoutineName {
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
		public RoutineName(String schema, String name) {
			this.schema = schema;
			this.name = name;
		}

		///<summary>
		///</summary>
		///<param name="table_name"></param>
		public RoutineName(TableName table_name)
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
		public static RoutineName Qualify(String current_schema, String proc_name) {
			int delim = proc_name.IndexOf(".");
			return delim == -1
			       	? new RoutineName(current_schema, proc_name)
			       	: new RoutineName(proc_name.Substring(0, delim),
			       	                    proc_name.Substring(delim + 1, proc_name.Length));
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			RoutineName src_ob = (RoutineName)ob;
			return (schema.Equals(src_ob.schema) &&
			        name.Equals(src_ob.name));
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return schema.GetHashCode() + name.GetHashCode();
		}
	}
}
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
		///<summary>
		///</summary>
		///<param name="schema"></param>
		///<param name="name"></param>
		public RoutineName(String schema, String name) {
			Schema = schema;
			Name = name;
		}

		///<summary>
		///</summary>
		///<param name="tableName"></param>
		public RoutineName(TableName tableName)
			: this(tableName.Schema, tableName.Name) {
		}

		///<summary>
		/// Returns the schema of this procedure.
		///</summary>
		public string Schema { get; private set; }

		/// <summary>
		/// Returns the name of this procedure.
		/// </summary>
		public string Name { get; private set; }

		/// <inheritdoc/>
		public override string ToString() {
			return Schema + "." + Name;
		}

		///<summary>
		/// Returns a version of this procedure qualified to the given schema 
		/// (unless the schema is present).
		///</summary>
		///<param name="currentSchema"></param>
		///<param name="procName"></param>
		///<returns></returns>
		public static RoutineName Qualify(String currentSchema, String procName) {
			int delim = procName.IndexOf(".");
			return delim == -1
			       	? new RoutineName(currentSchema, procName)
			       	: new RoutineName(procName.Substring(0, delim),
			       	                    procName.Substring(delim + 1, procName.Length));
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			RoutineName src_ob = (RoutineName)ob;
			return (Schema.Equals(src_ob.Schema) &&
			        Name.Equals(src_ob.Name));
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return Schema.GetHashCode() + Name.GetHashCode();
		}
	}
}
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

namespace Deveel.Data {
	/// <summary>
	/// Defines the properties for a schema in a database context.
	/// </summary>
	public sealed class SchemaDef {
		/// <summary>
		/// The name of the schema (eg. APP).
		/// </summary>
		private readonly String name;
		/// <summary>
		///  The type of this schema (eg. SYSTEM, USER, etc)
		/// </summary>
		private readonly String type;

		///<summary>
		///</summary>
		///<param name="name"></param>
		///<param name="type"></param>
		public SchemaDef(String name, String type) {
			this.name = name;
			this.type = type;
		}

		///<summary>
		/// Returns the case correct name of the schema.
		///</summary>
		public string Name {
			get { return name; }
		}

		///<summary>
		/// Returns the type of this schema.
		///</summary>
		public string Type {
			get { return type; }
		}

		/// <inheritdoc/>
		public override String ToString() {
			return Name;
		}

	}
}
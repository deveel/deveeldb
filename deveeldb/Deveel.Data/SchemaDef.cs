//  
//  SchemaDef.cs
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
//  
//  VariableRef.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	/// The object that references a variable declared within
	/// an opened session.
	/// </summary>
	[Serializable]
	public sealed class VariableRef {
		/// <summary>
		/// Constructs the reference to the given variable name.
		/// </summary>
		/// <param name="varName">The name of the variable to reference.</param>
		public VariableRef(string varName) {
			if (varName == null)
				throw new ArgumentNullException("varName");

			this.varName = varName;
		}

		/// <summary>
		/// The name of the variable to reference.
		/// </summary>
		private readonly string varName;

		/// <summary>
		/// Gets the name of the variable referenced.
		/// </summary>
		public string Variable {
			get { return varName; }
		}

		public override bool Equals(object obj) {
			VariableRef varRef = obj as VariableRef;
			return varRef == null ? false : varName.Equals(varRef.varName);
		}

		public override int GetHashCode() {
			return varName.GetHashCode();
		}

		public override string ToString() {
			return ":" + varName;
		}
	}
}
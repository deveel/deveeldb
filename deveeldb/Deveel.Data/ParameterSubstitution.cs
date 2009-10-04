//  
//  ParameterSubstitution.cs
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
	/// Represents a constant value that is to be lately binded to a constant 
	/// value in an <see cref="Expression"/>.
	/// </summary>
	/// <remarks>
	/// This is used when we have "?" style prepared statement values.
	/// This object is used as a marker in the elements of a expression.
	/// </remarks>
	[Serializable]
	public class ParameterSubstitution {
		/// <summary>
		/// The zero-based numerical number of this parameter substitution.
		/// </summary>
		private readonly int parameter_id;

		///<summary>
		///</summary>
		///<param name="parameter_id"></param>
		public ParameterSubstitution(int parameter_id) {
			this.parameter_id = parameter_id;
		}

		/// <summary>
		/// Returns the number of this parameter id.
		/// </summary>
		public int Id {
			get { return parameter_id; }
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			ParameterSubstitution sub = (ParameterSubstitution)ob;
			return parameter_id == sub.parameter_id;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}
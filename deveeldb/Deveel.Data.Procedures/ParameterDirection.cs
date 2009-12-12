//  
//  ParameterDirection.cs
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

namespace Deveel.Data.Procedures {
	/// <summary>
	/// The possible directions of a procedure parameter.
	/// </summary>
	/// <remarks>
	/// By default a <see cref="ProcedureParameter"/> is
	/// set as <see cref="Input"/>.
	/// </remarks>
	[Flags]
	public enum ParameterDirection {
		/// <summary>
		/// The parameter provides an input value to the 
		/// procedure, but won't be able to output a
		/// value eventually set.
		/// </summary>
		Input = 0x01,

		/// <summary>
		/// The parameter will not provide any input value,
		/// and will output a value set during the execution
		/// of the procedure.
		/// </summary>
		Output = 0x02,

		/// <summary>
		/// The parameter will provide an input value and will
		/// return an output value set during the execution
		/// of the procedure.
		/// </summary>
		InputOutput = Input | Output
	}
}
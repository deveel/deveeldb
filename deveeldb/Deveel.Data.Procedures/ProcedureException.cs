//  
//  ProcedureException.cs
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

namespace Deveel.Data.Procedures {
	///<summary>
	/// An exception that is generated from a stored procedure when some 
	/// erronious condition occurs.
	///</summary>
	/// <remarks>
	/// This error is typically returned back to the client.
	/// </remarks>
	public class ProcedureException : Exception {
		///<summary>
		///</summary>
		///<param name="message"></param>
		/// <param name="innerException"></param>
		public ProcedureException(string message, Exception innerException)
			: base(message, innerException) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		public ProcedureException(string message)
			: base(message) {
		}
	}
}
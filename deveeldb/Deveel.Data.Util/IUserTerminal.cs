//  
//  IUserTerminal.cs
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

namespace Deveel.Data.Util {
	/// <summary>
	/// An interface that represents a terminal for interactions between humanand machine.
	/// </summary>
	/// <remarks>
	/// This interface is intended for an interface in which the user is asked 
	/// questions, or for an automated tool.
	/// </remarks>
	public interface IUserTerminal {
		/// <summary>
		/// Writes a string of information to the terminal.
		/// </summary>
		/// <param name="str">The information string to write.</param>
		void Write(String str);

		/// <summary>
		/// Writes a line terminated string of information to the terminal.
		/// </summary>
		/// <param name="str">The information string to write.</param>
		void WriteLine(String str);

		/// <summary>
		/// Queries a user for an answer to the given question. 
		/// </summary>
		/// <param name="question">The query the terminal asks to the user.</param>
		/// <param name="options">The list of options that the user may select from.</param>
		/// <param name="default_answer">The option that is selected by default.</param>
		int Ask(String question, String[] options, int default_answer);
	}
}
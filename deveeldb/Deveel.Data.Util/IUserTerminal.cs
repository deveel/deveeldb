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
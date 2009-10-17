//  
//  IDebugLogger.cs
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

namespace Deveel.Diagnostics {
	/// <summary>
	/// An interface for logging errors, warnings, messages, and exceptions 
	/// in the system.
	/// </summary>
	/// <remarks>
	/// The implementation of where the log is written (to the console, file, 
	/// window, etc) is implementation defined.
	/// </remarks>
	public interface IDebugLogger : IDisposable {
		/// <summary>
		/// Queries the current debug level.
		/// </summary>
		/// <param name="level"></param>
		/// <remarks>
		/// This can be used to speed up certain complex debug displaying 
		/// operations where the debug listener isn't interested in the 
		/// information be presented.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the debug listener is interested in debug 
		/// information of this given level.
		/// </returns>
		bool IsInterestedIn(DebugLevel level);

		/// <summary>
		/// This writes the given debugging string.
		/// </summary>
		/// <param name="level">The filter level below which the messages are 
		/// sent to the debugger. This must be between 0 and 255 (a level
		/// of 255 is always printed).</param>
		/// <param name="ob">The object instance which issued the call.</param>
		/// <param name="message">The message to log.</param>
		void Write(DebugLevel level, object ob, string message);

		void Write(DebugLevel level, Type type, string message);

		void Write(DebugLevel level, string typeString, string message);

		/// <summary>
		/// This writes the given <see cref="Exception"/>.
		/// </summary>
		/// <param name="e"></param>
		/// <remarks>
		/// Exceptions are always output to the log stream.
		/// </remarks>
		void WriteException(Exception e);

		/// <summary>
		/// This writes the given <see cref="Exception"/> but gives 
		/// it a level.
		/// </summary>
		/// <param name="level"></param>
		/// <param name="e"></param>
		/// <remarks>
		/// This method is used to output a warning exception.
		/// </remarks>
		void WriteException(DebugLevel level, Exception e);
	}
}
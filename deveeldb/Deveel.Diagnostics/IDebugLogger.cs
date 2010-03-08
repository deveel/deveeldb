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

using Deveel.Data.Control;

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
		/// Initialize the logger instance with the configuration
		/// properties specified.
		/// </summary>
		/// <param name="config">The configurations used to configure
		/// the logger.</param>
		void Init(IDbConfig config);

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
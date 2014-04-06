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

using Deveel.Data.Configuration;
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
	public interface ILogger : IDisposable {
		/// <summary>
		/// Initialize the logger instance with the configuration
		/// properties specified.
		/// </summary>
		/// <param name="config">The configurations used to configure
		/// the logger.</param>
		void Init(DbConfig config);

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
		bool IsInterestedIn(LogLevel level);

		/// <summary>
		/// Logs a diagnostic event into the logger.
		/// </summary>
		/// <param name="entry">The object that encasulates the information
		/// about the event to log.</param>
		void Log(LogEntry entry);
	}
}
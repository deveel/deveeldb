// 
//  Copyright 2010-2015 Deveel
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
//

using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Defines a contract for event loggers.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Implementations of this interface have the responsibility
	/// to log events fired from the system into a configured medium.
	/// </para>
	/// </remarks>
	public interface IEventLogger {
		/// <summary>
		/// Logs the given entry to the medium of the logger.
		/// </summary>
		/// <param name="entry">The information to be logged.</param>
		void LogEvent(EventLog entry);

		/// <summary>
		/// Verifies if the logger is listening to the given level of logs.
		/// </summary>
		/// <param name="level">The level of information to log.</param>
		/// <returns>
		/// Returns <c>true</c> if the logger is configured to listen to
		/// the specified level of information, otherwise it returns <c>false</c>.
		/// </returns>
		bool CanLog(LogLevel level);
	}
}

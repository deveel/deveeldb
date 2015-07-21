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
	/// The level listened by a diagnostic logger
	/// </summary>
	public enum LogLevel {
		/// <summary>
		/// The level of the event is undefined.
		/// </summary>
		Undefined = 0,

		/// <summary>
		/// Critical errors that cause the interruption
		/// of the system.
		/// </summary>
		Critical = 5,

		/// <summary>
		/// An exception to the normal execution of the system
		/// that breaks an operation.
		/// </summary>
		Error = 4,

		/// <summary>
		/// Warning messages that define a non-blocking
		/// error, but declare an erroneous state of the operation.
		/// </summary>
		Warning = 3,

		/// <summary>
		/// Informational messages intended to output operative
		/// states of the application.
		/// </summary>
		Information = 2,

		/// <summary>
		/// Augmented verbosity to the logger, that reports an higher
		/// degree of information to the logger.
		/// </summary>
		Verbose = 1
	}
}

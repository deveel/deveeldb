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
	/// The default metadata keys that can be set into the
	/// <see cref="IEvent.EventData"/> container.
	/// </summary>
	public static class EventMetadataKeys {
		/// <summary>
		/// The key for the database name event data.
		/// </summary>
		public const string Database = "Database";

		/// <summary>
		/// The key for the user name who originated the event.
		/// </summary>
		public const string UserName = "UserName";

		/// <summary>
		/// The key of the remote address of a connection.
		/// </summary>
		public const string RemoteAddress = "Remote-Address";

		/// <summary>
		/// The key of the connection protocol name of a session that
		/// originated the event.
		/// </summary>
		public const string Protocol = "Protocol";

		/// <summary>
		/// The key for the error stack trace in an error event.
		/// </summary>
		public const string StackTrace = "StackTrace";

		/// <summary>
		/// The key for the error source in an error event.
		/// </summary>
		public const string Source = "Source";

		/// <summary>
		/// The level of an error event.
		/// </summary>
		public const string ErrorLevel = "Error-Level";
	}
}

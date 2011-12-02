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

namespace Deveel.Data.Client {
	/// <summary>
	/// Constants used in the database communication protocol.
	/// </summary>
	public sealed class ProtocolConstants {
		/// <summary>
		/// Sent as an acknowledgement to a command.
		/// </summary>
		public const int ACKNOWLEDGEMENT = 5;

		/// <summary>
		/// The specified database was not found.
		/// </summary>
		public const int DATABASE_NOT_FOUND = 7;

		/// <summary>
		/// Sent if login passed.
		/// </summary>
		public const int USER_AUTHENTICATION_PASSED = 10;

		/// <summary>
		/// Sent if login failed because username or password were invalid.
		/// </summary>
		public const int USER_AUTHENTICATION_FAILED = 15;

		/// <summary>
		/// Operation was successful.
		/// </summary>
		public const int SUCCESS = 20;

		/// <summary>
		/// Operation failed (followed by a UTF String error message).
		/// </summary>
		public const int FAILED = 25;


		/// <summary>
		/// Operation threw an exception.
		/// </summary>
		public const int EXCEPTION = 30;

		/// <summary>
		/// There was an authentication error.
		/// </summary>
		/// <remarks>
		/// A command couldn't be executed because the user does not have enough rights.
		/// </remarks>
		public const int AUTHENTICATION_ERROR = 35;





		// ---------- Commands ----------

		/// <summary>
		/// Changes the current database for the session.
		/// </summary>
		public const int CHANGE_DATABASE = 40;

		/// <summary>
		/// Query sent to the server for processing.
		/// </summary>
		public const int QUERY = 50;

		/// <summary>
		/// Disposes the server-side resources associated with a result.
		/// </summary>
		public const int DISPOSE_RESULT = 55;

		/// <summary>
		/// Requests a section of a result from the server.
		/// </summary>
		public const int RESULT_SECTION = 60;

		/// <summary>
		/// Requests a section of a streamable object from the server.
		/// </summary>
		public const int STREAMABLE_OBJECT_SECTION = 61;

		/// <summary>
		/// Disposes of the resources associated with a streamable object 
		/// on the server.
		/// </summary>
		public const int DISPOSE_STREAMABLE_OBJECT = 62;

		/// <summary>
		/// For pushing a part of a streamable object onto the server from the client.
		/// </summary>
		public const int PUSH_STREAMABLE_OBJECT_PART = 63;


		/// <summary>
		/// Ping command.
		/// </summary>
		public const int PING = 65;

		/// <summary>
		/// Closes the protocol stream.
		/// </summary>
		public const int CLOSE = 70;

		/// <summary>
		/// Denotes an event from the database (trigger, etc).
		/// </summary>
		public const int DATABASE_EVENT = 75;

		/// <summary>
		/// Denotes a server side request for information.
		/// </summary>
		/// <remarks>
		/// For example, a request for a part of a streamable object.
		/// </remarks>
		public const int SERVER_REQUEST = 80;
	}
}
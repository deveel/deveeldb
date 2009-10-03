// 
//  ProtocolConstants.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		/// A query couldn't be executed because the user does not have enough rights.
		/// </remarks>
		public const int AUTHENTICATION_ERROR = 35;





		// ---------- Commands ----------

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
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

using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// An object that handles the information about a <see cref="IUserSession"/>.
	/// </summary>
	/// <seealso cref="IUserSession.SessionInfo"/>
	public sealed class SessionInfo {
		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo" /> class.
		/// </summary>
		/// <param name="user">The user that owns the session.</param>
		/// <param name="isolation">The isolation level of the transaction.</param>
		/// <param name="endPoint">The source end point of the session.</param>
		public SessionInfo(User user, IsolationLevel isolation, ConnectionEndPoint endPoint)
					: this(-1, user, isolation, endPoint) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo" /> class for the
		/// given user.
		/// </summary>
		/// <param name="user">The user that owns the session.</param>
		public SessionInfo(User user)
			: this(user, IsolationLevel.Unspecified) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo"/> class that has
		/// no unique commit identification.
		/// </summary>
		/// <param name="user">The user that owns the session.</param>
		/// <param name="isolation">The isolation level of the transaction.</param>
		public SessionInfo(User user, IsolationLevel isolation)
			: this(-1, user, isolation) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo"/> class.
		/// </summary>
		/// <param name="commitId">The commit identifier.</param>
		/// <param name="user">The user.</param>
		public SessionInfo(int commitId, User user)
			: this(commitId, user, IsolationLevel.Unspecified) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo"/> class that
		/// references an established session to the database.
		/// </summary>
		/// <param name="commitId">The unique commit identifier.</param>
		/// <param name="user">The user that owns the session.</param>
		/// <param name="isolation">The isolation level of the session.</param>
		/// <remarks>
		/// This constructor sets the origin connection end point as embedded.
		/// </remarks>
		public SessionInfo(int commitId, User user, IsolationLevel isolation)
			: this(commitId, user, isolation, ConnectionEndPoint.Embedded) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SessionInfo"/> class.
		/// </summary>
		/// <param name="commitId">The unique commit identifier.</param>
		/// <param name="user">The user that owns the session.</param>
		/// <param name="isolation">The isolation level of the sessions.</param>
		/// <param name="endPoint">The origin end point of the session.</param>
		/// <exception cref="System.ArgumentNullException">If either the specified user
		/// or endPoint are <c>null</c>.
		/// </exception>
		public SessionInfo(int commitId, User user, IsolationLevel isolation, ConnectionEndPoint endPoint) {
			if (user == null)
				throw new ArgumentNullException("user");
			if (endPoint == null)
				throw new ArgumentNullException("endPoint");

			CommitId = commitId;
			User = user;
			EndPoint = endPoint;
			Isolation = isolation;
			StartedOn = DateTimeOffset.UtcNow;
		}

		/// <summary>
		/// Gets the unique commit identifier of the session.
		/// </summary>
		/// <value>
		/// The unique commit identifier.
		/// </value>
		public int CommitId { get; private set; }

		/// <summary>
		/// Gets the source end point of the session.
		/// </summary>
		/// <value>
		/// The source end point.
		/// </value>
		public ConnectionEndPoint EndPoint { get; private set; }

		/// <summary>
		/// Gets the user that owns the session.
		/// </summary>
		/// <value>
		/// The owner user.
		/// </value>
		public User User { get; private set; }

		/// <summary>
		/// Gets the isolation level of the session.
		/// </summary>
		/// <value>
		/// The isolation level.
		/// </value>
		public IsolationLevel Isolation { get; private set; }

		/// <summary>
		/// Gets the time-stamp of when the session was started.
		/// </summary>
		/// <value>
		/// The started time of the session.
		/// </value>
		public DateTimeOffset StartedOn { get; private set; }

		/// <summary>
		/// Gets the time of the last command issues by the user 
		/// during the session.
		/// </summary>
		/// <value>
		/// The time of the last command.
		/// </value>
		public DateTimeOffset? LastCommandTime { get; private set; }

		// TODO: keep a list of commands issued by the user during the session?

		internal void OnCommand() {
			// TODO: also include the command details?
			LastCommandTime = DateTimeOffset.UtcNow;
		}
	}
}

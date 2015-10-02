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
	public sealed class SessionInfo {
		public SessionInfo(User user, TransactionIsolation isolation, ConnectionEndPoint endPoint) 
			: this(-1, user, isolation, endPoint) {
		}

		public SessionInfo(User user) 
			: this(user, TransactionIsolation.Unspecified) {
		}

		public SessionInfo(User user, TransactionIsolation isolation) 
			: this(-1, user, isolation) {
		}

		public SessionInfo(int commitId, User user) 
			: this(commitId, user, TransactionIsolation.Unspecified) {
		}

		public SessionInfo(int commitId, User user, TransactionIsolation isolation) 
			: this(commitId, user, isolation, ConnectionEndPoint.Embedded) {
		}

		public SessionInfo(int commitId, User user, TransactionIsolation isolation, ConnectionEndPoint endPoint) {
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

		public int CommitId { get; private set; }

		public ConnectionEndPoint EndPoint { get; private set; }

		public User User { get; private set; }

		public TransactionIsolation Isolation { get; private set; }

		public DateTimeOffset StartedOn { get; private set; }

		public DateTimeOffset? LastCommandTime { get; private set; }

		// TODO: keep a list of commands issued by the user during the session?

		internal void OnCommand() {
			// TODO: also include the command details?
			LastCommandTime = DateTimeOffset.UtcNow;
		}
	}
}

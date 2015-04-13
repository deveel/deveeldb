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

using Deveel.Data.Diagnostics;
using Deveel.Data.Protocol;
using Deveel.Data.Security;

namespace Deveel.Data.DbSystem {
	public sealed class SessionInfo {
		public SessionInfo(IUserSession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
			StartedOn = DateTime.UtcNow;
		}

		public IUserSession Session { get; private set; }

		public User User {
			get { return Session.User; }
		}

		public ConnectionEndPoint EndPoint {
			get { return Session.EndPoint; }
		}

		public DateTime StartedOn { get; private set; }

		public DateTime? LastCommandOn { get; private set; }

		public void OnNewEvent(IEvent e) {
			lock (Session) {
				Session.Database.EventRegistry.RegisterEvent(e);
			}
		}
	}
}

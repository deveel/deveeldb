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

using Deveel.Data.Caching;
using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class GrantManager {
		private readonly IQueryContext context;
		private ICache privCache;

		public GrantManager(IUserSession session) {
			Session = session;

			context = new SessionQueryContext(session);
			privCache = new MemoryCache(16, 256, 10);
		}

		public IUserSession Session { get; private set; }

		#region GrantQuery

		struct GrantQuery {
			private readonly DbObjectType objType;
			private readonly ObjectName objName;
			private readonly string userName;
			private readonly int flags;

			public GrantQuery(DbObjectType objType, ObjectName objName, string userName, int flags) :
				this() {
				this.objType = objType;
				this.objName = objName;
				this.userName = userName;
				this.flags = flags;
			}

			public override bool Equals(object obj) {
				var other = (GrantQuery)obj;
				return (objType == other.objType &&
						objName.Equals(other.objName) &&
						userName.Equals(other.userName) &&
						flags == other.flags);
			}

			public override int GetHashCode() {
				return (int)objType + objName.GetHashCode() + userName.GetHashCode() + flags;
			}
		}

		#endregion
	}
}

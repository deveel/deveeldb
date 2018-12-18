// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;

using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class UserSession : Context, ISession {
		public UserSession(IDatabase database, ITransaction transaction, User user)
			: base(database) {
			if (user == null)
				throw new ArgumentNullException(nameof(user));

			if (user.IsSystem)
				throw new ArgumentException("Cannot create a session for the system user");

			Database = database ?? throw new ArgumentNullException(nameof(database));
			Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
			User = user;
		}

		public User User { get; }

		public IDatabase Database { get; }

		public ITransaction Transaction { get; }

		public string CurrentSchema => Transaction.CurrentSchema();

		public ICommand CreateCommand(SqlCommand command) {
			throw new NotImplementedException();
		}

		IEventSource IEventSource.ParentSource => Database;

		IDictionary<string, object> IEventSource.Metadata => GetMetadata();

		private IDictionary<string, object> GetMetadata() {
			throw new NotImplementedException();
		}
	}
}
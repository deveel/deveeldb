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
	public sealed class SystemSession : Context, ISession {
		public SystemSession(IDatabase database, ITransaction transaction, string currentSchema)
			: base(database) {
			Database = database ?? throw new ArgumentNullException(nameof(database));
			Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
			CurrentSchema = currentSchema;
		}

		public IDatabase Database { get; }

		public ITransaction Transaction { get; }

		public string CurrentSchema { get; }

		IEventSource IEventSource.ParentSource => Database;

		IDictionary<string, object> IEventSource.Metadata => new Dictionary<string, object>();

		User ISession.User => User.System;

		ICommand ISession.CreateCommand(SqlCommand command) {
			throw new NotSupportedException();
		}
	}
}
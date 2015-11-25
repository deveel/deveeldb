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
using System.Data;

using Deveel.Data;
using Deveel.Data.Protocol;

namespace Deveel.Data.Client {
	public static class DatabaseExtensions {
		public static IDbConnection CreateDbConnection(this IDatabase database, string userName, string password) {
			if (database == null)
				throw new ArgumentNullException("database");

			var dbHandler = database.Context.SystemContext as IDatabaseHandler;
			if (dbHandler == null)
				dbHandler = new SingleDatabaseHandler(database);

			var serverConnector = new EmbeddedServerConnector(dbHandler);
			var clientConnector = new EmbeddedClientConnector(serverConnector);

			var settings = new DeveelDbConnectionStringBuilder {
				UserName = userName,
				Password = password,
				Database = database.Name
			};

			return new DeveelDbConnection(clientConnector, settings);
		}
	}
}

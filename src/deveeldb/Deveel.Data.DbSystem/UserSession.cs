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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Query;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class UserSession : IUserSession {
		public bool IgnoreCase {
			get { throw new NotImplementedException(); }
		}

		public ITableQueryInfo GetQueryInfo(ObjectName tableName, ObjectName givenName) {
			throw new NotImplementedException();
		}

		public ObjectName ResolveObjectName(string name) {
			throw new NotImplementedException();
		}

		public bool TableExists(ObjectName tableName) {
			throw new NotImplementedException();
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

		public User User {
			get { throw new NotImplementedException(); }
		}

		public DateTimeOffset? LastCommand {
			get { throw new NotImplementedException(); }
		}

		public ITransaction Transaction {
			get { throw new NotImplementedException(); }
		}

		public void CacheTable(ObjectName tableName, ITable table) {
			throw new NotImplementedException();
		}

		public ITable GetCachedTable(ObjectName tableName) {
			throw new NotImplementedException();
		}

		public ILargeObject CreateLargeObject(long size, bool compressed) {
			throw new NotImplementedException();
		}

		public ILargeObject GetLargeObject(ObjectId objId) {
			throw new NotImplementedException();
		}

		public IDbConnection GetDbConnection() {
			throw new NotImplementedException();
		}

		public void Commit() {
			throw new NotImplementedException();
		}

		public void Rollback() {
			throw new NotImplementedException();
		}

		public void Close() {
			throw new NotImplementedException();
		}
	}
}
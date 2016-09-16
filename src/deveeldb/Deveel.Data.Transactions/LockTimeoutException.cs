// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class LockTimeoutException : TransactionException {
		internal LockTimeoutException(ObjectName tableName, AccessType accessType, int timeout)
			: base(SystemErrorCodes.LockTimeout, FormatMessage(tableName, accessType, timeout)) {
			TableName = tableName;
			AccessType = accessType;
			Timeout = timeout;
		}

		public ObjectName TableName { get; private set; }

		public int Timeout { get; private set; }

		public AccessType AccessType { get; private set; }

		public static string FormatMessage(ObjectName tableName, AccessType accessType, int timeout) {
			var timeoutString = timeout == System.Threading.Timeout.Infinite
				? "Infinite"
				: String.Format("{0}ms", timeout);
			var accessTypeString = accessType == AccessType.ReadWrite
				? "read/write"
				: accessType.ToString().ToLowerInvariant();
			return String.Format("A {0} lock on {1} was not released before {2}", accessTypeString, tableName, timeoutString);
		}
	}
}

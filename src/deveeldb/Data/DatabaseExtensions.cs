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

using Deveel.Data.Configurations;
using Deveel.Data.Transactions;

namespace Deveel.Data {
 	public static class DatabaseExtensions {
		 #region Configurations

		 public static bool IsReadOnly(this IDatabase database) {
			 return database.Configuration.GetBoolean("readOnly");
		 }

		 #endregion
		 
		 #region Transactions

		 public static ITransaction CreateTransaction(this IDatabase database) {
			 var isolationLevel = database.GetValue<IsolationLevel?>("isolationLevel");
			 if (isolationLevel == null)
				 isolationLevel = IsolationLevel.Serializable;

			 return database.CreateTransaction(isolationLevel.Value);
		 }

		 #endregion

		 #region Sessions

		 public static SystemSession CreateSystemSession(this IDatabase database, string currentSchema)
			 => new SystemSession(database, database.CreateTransaction(), currentSchema);

		 #endregion
	 }
}
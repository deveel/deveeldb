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
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// An authenticated session to a given database for a given user,
	/// that wraps a transaction for operations.
	/// </summary>
	public interface ISession : IContext, IEventSource {
		/// <summary>
		/// Gets the reference to the user owning the session
		/// </summary>
		User User { get; }

		/// <summary>
		/// Gets the instance of the database that this session is connected to
		/// </summary>
		IDatabase Database { get; }

		/// <summary>
		/// Gets the instance of <see cref="ITransaction"/> that handles the
		/// transactional operations of this session.
		/// </summary>
		ITransaction Transaction { get; }

		/// <summary>
		/// Gets the current schema of the session
		/// </summary>
		string CurrentSchema { get; }


		/// <summary>
		/// Creates a new command object that can be used to execute commands
		/// towards the underlying database of this session.
		/// </summary>
		/// <param name="command">The SQL command for the context</param>
		/// <returns>
		/// Returns an instance of <see cref="ICommand"/> that is used
		/// to execute commands towards the underlying database.
		/// </returns>
		ICommand CreateCommand(SqlCommand command);

		///// <summary>
		///// Commits the latest changes made by the user in the session.
		///// </summary>
		///// <seealso cref="ITransaction"/>
		//Task CommitAsync();

		///// <summary>
		///// Rolls-back all the modifications made by the user in this session
		///// </summary>
		///// <seealse cref="ITransaction"/>
		//Task RollbackAsync();
	}
}
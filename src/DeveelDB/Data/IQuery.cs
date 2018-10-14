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
using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// This is the <see cref="IContext">context</see> that
	/// is the direct child of a <see cref="ISession"/>, as an isolated
	/// context for the execution of <see cref="SqlStatement">statements</see>
	/// </summary>
	/// <remarks>
	/// <para>
	/// Instances of <see cref="IQuery"/> are typically created from
	/// calls to <see cref="ISession.CreateQuery"/>.
	/// </para>
	/// <para>
	/// Any command executed in a query is evaluated against the parent
	/// <see cref="ISession"/> object, to assess the proper rights
	/// of the user against the database objects.
	/// </para>
	/// <para>
	/// Data-definition commands can be executed only at the query-level.
	/// </para>
	/// </remarks>
	/// <seealso cref="ISession"/>
	public interface IQuery : IContext, IEventSource {
		/// <summary>
		/// Gets the SQL query that originated the context
		/// </summary>
		SqlQuery SourceQuery { get; }

		/// <summary>
		/// The parent <see cref="ISession"/> object that
		/// originated this query.
		/// </summary>
		/// <seealso cref="ISession.CreateQuery"/>
		ISession Session { get; }
	}
}
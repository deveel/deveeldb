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

namespace Deveel.Data {
	/// <summary>
	/// Provides an interface to objects that handle references
	/// to <see cref="IDatabase"/> instances.
	/// </summary>
	public interface IDatabaseHandler {
		/// <summary>
		/// Gets the instance of <see cref="IDatabase"/> identified
		/// by the given name.
		/// </summary>
		/// <param name="databaseName">The name of the database to get.</param>
		/// <returns>
		/// Returns an instance of <see cref="IDatabase"/> that is handled
		/// by this object, or <c>null</c> if no database with the given
		/// name was found in this handler.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="databaseName"/> parameter is <c>null</c>
		/// or empty.
		/// </exception>
		/// <exception cref="ObjectDisposedException">
		/// If the handler was disposed and cannot handle databases.
		/// </exception>
		IDatabase GetDatabase(string databaseName);
	}
}

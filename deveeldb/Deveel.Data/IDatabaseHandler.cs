// 
//  Copyright 2010  Deveel
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

using System;

namespace Deveel.Data {
	/// <summary>
	/// An interface implemented by classes which manage databases
	/// within the current system.
	/// </summary>
	public interface IDatabaseHandler {
		/// <summary>
		/// Gets the database instance for the given name.
		/// </summary>
		/// <param name="name">The name of the database to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="Database"/> that is identified
		/// by the <paramref name="name"/> given, or <b>null</b> if none
		/// was specified for the identifier.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// Thrown if <paramref name="name"/> is <b>null</b> or an empty string.
		/// </exception>
		Database GetDatabase(string name);
	}
}
// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// The component used to parse a string into instances
	/// of SQL types
	/// </summary>
    public interface ISqlTypeParser {
		/// <summary>
		/// Reads the input string and turns into an instance
		/// of <see cref="SqlType"/> having the properties defined
		/// in the string.
		/// </summary>
		/// <param name="context">The optional system context used to resolve 
		/// the type</param>
		/// <param name="s">The input string to parse</param>
		/// <remarks>
		/// <para>
		/// The <paramref name="context"/> is not required if the input string is
		/// known to be a primitive type (<see cref="PrimitiveTypes.IsPrimitive(string)"/>).
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlType"/> that is resolved
		/// from the context and having the specified attributes
		/// </returns>
		/// <seealso cref="ISqlTypeResolver"/>
		/// <seealso cref="PrimitiveTypes"/>
        SqlType Parse(IContext context, string s);
    }
}
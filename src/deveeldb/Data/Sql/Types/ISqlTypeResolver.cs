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

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// A container that is used to resolve the types
	/// matching the information specified
	/// </summary>
	/// <remarks>
	/// <para>
	/// The interface exposes the functionality of resolving
	/// types defined at design time (eg. <see cref="PrimitiveTypes">primitive
	/// types</see>) or at runtime, like the User-Defined types.
	/// </para>
	/// <para>
	/// Components extending the core library can register instances
	/// of this library for exposing types defined by the external
	/// libraries (eg. <c>GEOMETRY</c>, <c>XML</c>, etc.)
	/// </para>
	/// </remarks>
	public interface ISqlTypeResolver {
		/// <summary>
		/// Resolves the given information into the given
		/// SQL type corresponding.
		/// </summary>
		/// <param name="resolveInfo">The information used to
		/// resolve the SQL type.</param>
		/// <returns></returns>
		SqlType Resolve(SqlTypeResolveInfo resolveInfo);
	}
}
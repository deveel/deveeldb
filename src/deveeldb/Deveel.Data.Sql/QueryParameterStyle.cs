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

namespace Deveel.Data.Sql {
	/// <summary>
	/// In a SQL query object, this is the form of
	/// parameters passed from the client side to the
	/// server side, defining the identification of
	/// the single parameters passed.
	/// </summary>
	/// <seealso cref="SqlQuery.ParameterStyle"/>
	public enum QueryParameterStyle {
		/// <summary>
		/// No specific form of the parameter was given:
		/// this default to the system default parameter
		/// style configured.
		/// </summary>
		Default = 0,

		/// <summary>
		/// Defines parameters uniquely identified within the
		/// query context by a name. In this form query parameter
		/// names must be prefixed by the <c>@</c> character.
		/// </summary>
		Named = 1,

		/// <summary>
		/// Parameters that are replaced on a zero-based index of
		/// the input parameters of a query. These parameters are not
		/// identified by a unique name, but by a <c>?</c> character
		/// that acts as a place-holder for an input parameter.
		/// </summary>
		Marker = 2
	}
}
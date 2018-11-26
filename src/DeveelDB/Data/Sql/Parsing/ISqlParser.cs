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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parsing {
	/// <summary>
	/// The contract for a component that allows converting a
	/// SQL text into statements that can be interpreted by
	/// the system.
	/// </summary>
	public interface ISqlParser {
		/// <summary>
		/// Gets the descriptive string of the dialect used for parsing
		/// </summary>
		string Dialect { get; }


		/// <summary>
		/// Analyzes the input text and attempts to convert it
		/// into statements that can be interpreted by the system.
		/// </summary>
		/// <param name="context">An optional context used by the parsing. This is
		/// mostly used for getting configurations or firing messages.</param>
		/// <param name="sql">The SQL text to be parsed</param>
		/// <remarks>
		/// <para>
		/// It is not expected this method to throw any exception, but
		/// rather to return errors in the result object for every failure
		/// in parsing, detailing the reason of failure.
		/// </para>
		/// <para>
		/// The scope of the parser is not to interpret the commands contained
		/// in the text, but to convert this text into <see cref="SqlStatement"/>
		/// objects that represent the commands to be passed to the system.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlParseResult"/> that includes
		/// the information on the result of the operation.
		/// </returns>
		/// <seealso cref="SqlParseResult"/>
		SqlParseResult Parse(IContext context, string sql);
	}
}
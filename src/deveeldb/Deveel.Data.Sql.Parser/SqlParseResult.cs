// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// The result of a parse of an SQL input
	/// </summary>
	/// <seealso cref="ISqlParser"/>
	public sealed class SqlParseResult {
		/// <summary>
		/// Constructs a new <see cref="SqlParseResult"/>.
		/// </summary>
		/// <param name="dialect">The SQL dialect of the input.</param>
		public SqlParseResult(string dialect) {
			if (String.IsNullOrEmpty(dialect))
				throw new ArgumentNullException("dialect");

			Dialect = dialect;
			Errors = new List<SqlParseError>();
		}

		/// <summary>
		/// Gets the name of the SQL dialect of the parser
		/// that generated this result.
		/// </summary>
		public string Dialect { get; private set; }

		/// <summary>
		/// Gets or sets the node that is the root of the parsed
		/// nodes from the input.
		/// </summary>
		/// <remarks>
		/// <para>
		/// If the parser produced any tree from the analysis, this
		/// object will be used to construct commands to interact
		/// with the underlying system.
		/// </para>
		/// <para>
		/// In some cases this value is not set, because of previous
		/// errors during the analysis of an input from the parser.
		/// </para>
		/// </remarks>
		/// <seealso cref="ISqlNode"/>
		/// <seealso cref="ISqlNodeVisitor"/>
		public ISqlNode RootNode { get; set; }

		/// <summary>
		/// Gets a boolean value that indicates if the result has
		/// any root node set.
		/// </summary>
		/// <seealso cref="RootNode"/>
		public bool HasRootNode {
			get { return RootNode != null; }
		}

		/// <summary>
		/// Gets a collection of <see cref="SqlParseError"/> that
		/// were found during the parse of an input.
		/// </summary>
		public ICollection<SqlParseError> Errors { get; private set; }

		/// <summary>
		/// Gets or sets the time the parser took to analyze an input provided.
		/// </summary>
		public TimeSpan ParseTime { get; set; }

		/// <summary>
		/// Gets a boolean value indicating if the result has
		/// any error.
		/// </summary>
		/// <seealso cref="Errors"/>
		/// <seealso cref="SqlParseError"/>
		public bool HasErrors {
			get { return Errors.Count > 0; }
		}
	}
}

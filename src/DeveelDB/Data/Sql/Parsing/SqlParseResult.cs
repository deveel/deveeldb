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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parsing {
	/// <summary>
	/// Contains the informations and results of the parsing
	/// operation from <see cref="ISqlParser"/>.
	/// </summary>
	public sealed class SqlParseResult {
		public SqlParseResult() {
			Messages = new List<SqlParseMessage>();
			Statements = new List<SqlStatement>();
		}

		/// <summary>
		/// Gets a mutable collection of messages to output during the
		/// parsing operation.
		/// </summary>
		public ICollection<SqlParseMessage> Messages { get; }

		/// <summary>
		/// Gets a mutable collection of statements resulted from the parse
		/// of a valid input SQL text.
		/// </summary>
		public ICollection<SqlStatement> Statements { get; }

		/// <summary>
		/// Gets a boolean value indicating if the parse failed.
		/// </summary>
		/// <remarks>
		/// A parse failed if all objects in <see cref="Messages"/> are
		/// of <see cref="SqlParseMessageLevel.Error"/>.
		/// </remarks>
		public bool Failed => Messages.Any(x => x.Level == SqlParseMessageLevel.Error);

		public bool Succeeded => Messages.All(x => x.Level != SqlParseMessageLevel.Error);
	}
}
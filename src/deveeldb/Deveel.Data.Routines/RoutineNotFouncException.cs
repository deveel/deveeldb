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
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public class RoutineNotFouncException : ObjectNotFoundException {
		public RoutineNotFouncException(ObjectName routineName)
			: this(routineName, new SqlExpression[0]) {
		}

		public RoutineNotFouncException(string message)
			: this(null, message) {
		}

		public RoutineNotFouncException(ObjectName routineName, string message)
			: this(routineName, new SqlExpression[0], message) {
		}

		public RoutineNotFouncException(ObjectName routineName, SqlExpression[] args)
			: this(routineName, args, FormMessage(routineName, args)) {
		}

		public RoutineNotFouncException(ObjectName routineName, SqlExpression[] args, string message)
			: base(routineName, message) {
			Arguments = args;
		}

		public SqlExpression[] Arguments { get; private set; }

		private static string FormMessage(ObjectName name, SqlExpression[] args) {
			var sb = new StringBuilder(name.FullName);
			sb.Append("(");
			if (args != null) {
				for (int i = 0; i < args.Length; i++) {
					sb.Append(args[i]);

					if (i < args.Length - 1)
						sb.Append(", ");
				}
			}
			sb.Append(")");

			return String.Format("Unable to resolve {0} to any routine within the system.", sb);
		}
	}
}

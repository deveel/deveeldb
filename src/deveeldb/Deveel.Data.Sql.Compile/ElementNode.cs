using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	class ElementNode  {
		public ObjectName Id { get; set; }

		public FunctionArgumentNode[] Argument { get; set; }

		public static ElementNode Form(PlSqlParser.General_elementContext context) {
			var id = Name.Object(context.objectName());
			var arg = context.function_argument();
			IEnumerable<FunctionArgumentNode> argNodes = null;
			if (arg != null) {
				argNodes = arg.argument().Select(FunctionArgumentNode.Form);
			}

			return new ElementNode {
				Id = id,
				Argument = argNodes != null ? argNodes.ToArray() : null
			};
		}
	}
}

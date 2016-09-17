using System;
using System.Linq.Expressions;
using System.Reflection;

using IQToolkit;
using IQToolkit.Data;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbQueryLanguage : QueryLanguage {
		private readonly QueryTypeSystem typeSystem;

		public DeveelDbQueryLanguage() {
			// TODO: make the DeveelDB type system
			typeSystem = new DbTypeSystem();
		}

		public override Expression GetGeneratedIdExpression(MemberInfo member) {
			// TODO: Support the query by table name
			return new FunctionExpression(TypeHelper.GetMemberType(member), "LAST_IDENTITY()", null);
		}

		public override QueryTypeSystem TypeSystem {
			get { return typeSystem; }
		}

		public override string Quote(string name) {
			return String.Format("\"{0}\"", name.Replace("\"", "\"\""));
		}

		public override QueryLinguist CreateLinguist(QueryTranslator translator) {
			return new DeveelDbLinguist(this, translator);
		}
	}
}

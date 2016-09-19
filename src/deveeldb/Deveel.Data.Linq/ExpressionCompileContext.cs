using System;
using System.Collections.Generic;

using Deveel.Data.Design;

namespace Deveel.Data.Linq {
	class ExpressionCompileContext {
		private Dictionary<string, Type> typeAliases;

		public ExpressionCompileContext(CompiledModel model) {
			Model = model;
			typeAliases = new Dictionary<string, Type>();
		}

		public CompiledModel Model { get; private set; }

		public void AddAlias(Type type, string aliasName) {
			typeAliases[aliasName] = type;
		}

		public string FindTableName(string alias) {
			Type type;
			if (!typeAliases.TryGetValue(alias, out type))
				throw new InvalidOperationException(String.Format("Alias '{0}' maps to none of the types known.", alias));

			return Model.FindTableName(type);
		}

		public string FindTableName(Type type) {
			return Model.FindTableName(type);
		}

		public TypeMemberMapInfo GetMemberMap(Type type, string memberName) {
			var typeInfo = Model.GetTypeInfo(type);
			if (typeInfo == null)
				throw new InvalidOperationException(String.Format("Type '{0}' is not mapped.", type));

			var memberMapInfo = typeInfo.FindMemberMap(memberName);
			if (memberMapInfo == null)
				throw new InvalidOperationException(String.Format("Member '{0}' not found in type '{1}' or not mapped in the model", memberName, type));

			return memberMapInfo;
		}
	}
}
